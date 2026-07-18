"""Protocol-agnostic collector core.

Owns everything that is identical regardless of the wire protocol: loading
gateway settings and asset/datapoint configuration from IronFlock,
subscribing to live config changes, managing one collection task per asset
(paced polling or protocol-pushed streams), assembling the ``measurements``
payload, tracking asset online/offline/paused state in ``assetstatus``, and
writing to IronFlock. All protocol specifics are delegated to an injected
``ProtocolAdapter`` (see ``collector_core.adapter``).

Data writes (measurements, datapoints, assetstatus) can be disabled per
gateway via the ``store_data`` column for forward-only deployments;
configuration is still read live from the platform.
"""

import asyncio
import os
import sys
import traceback
from collections import deque
from contextlib import aclosing
from datetime import datetime, timezone

from collector_core.adapter import (
    PLATFORM_COLUMNS,
    DatapointStore,
    DatapointsChanged,
    emit_error,
)
from collector_core.errors import CollectorError

# Max data rows buffered locally while the cloud link is down, when the
# gateway row sets no buffer_size. Oldest rows are dropped on overflow.
DEFAULT_BUFFER_SIZE = 10000

# How often the background flusher retries draining the buffer (seconds).
FLUSH_INTERVAL = 15

# Max rows per bulk insert, so catching up a large backlog is split into
# several capped requests instead of one oversized one.
BULK_CHUNK_MAX = 5000

# Sentinel for "no value seen yet" in the change-detection cache, so a genuine
# None reading is distinguished from a datapoint that has never been published.
_UNSET = object()


def _now():
    return datetime.now(timezone.utc).isoformat()


def _read_injected(name):
    """Platform-injected value by name: the live env FILE first, env var second.

    The device agent mirrors every injected env var to ``/data/env/{name}.txt``
    (a bind mount it can update in RUNNING containers — e.g. the cloud port of
    an instance tunnel, allocated only after the tunnel first connects). The
    process env is the start-time snapshot and serves as the fallback. Same
    contract as the ironflock SDK's ``_readInjectedValue``; directory
    overridable via ``IRONFLOCK_ENV_DIR``.
    """
    env_dir = os.environ.get("IRONFLOCK_ENV_DIR", "/data/env")
    try:
        with open(os.path.join(env_dir, f"{name}.txt")) as f:
            value = f.read().strip()
        if value:
            return value
    except OSError:
        pass
    return os.environ.get(name)


def _tunnel_access_urls(port, protocol, remote_port_environment):
    """(appliance_url, cloud_url) for a declared port, mirroring the ironflock
    SDK's ``getRemoteAccessUrlForPort`` label/domain rules — but returning BOTH
    routes instead of the single preferred one.

    ``cloud`` is the internet-facing URL: on instance (appliance-managed)
    devices the ``i{INSTANCE_KEY}-``-prefixed route on ``CLOUD_TUNNEL_DOMAIN``;
    on cloud-managed devices the plain route on ``TUNNEL_DOMAIN`` (which IS the
    cloud edge there). ``appliance`` is the appliance-network URL — the plain
    route on the appliance's ``TUNNEL_DOMAIN`` — and exists only on instance
    devices (``INSTANCE_KEY`` set). Either value is None when the identity /
    env it needs is unavailable or the tunnel label would be invalid.
    """
    instance_key = os.environ.get("INSTANCE_KEY")

    if protocol in ("tcp", "udp"):
        if not remote_port_environment:
            return None, None
        appliance = cloud = None
        remote_port = _read_injected(remote_port_environment)
        if remote_port:
            tunnel_domain = os.environ.get("TUNNEL_DOMAIN", "app.ironflock.com")
            url = f"{protocol}://{tunnel_domain}:{remote_port}"
            if instance_key:
                appliance = url
            else:
                cloud = url
        if instance_key:
            cloud_port = _read_injected(f"{remote_port_environment}_CLOUD")
            if cloud_port:
                cloud_domain = os.environ.get("CLOUD_TUNNEL_DOMAIN", "app.ironflock.com")
                cloud = f"{protocol}://{cloud_domain}:{cloud_port}"
        return appliance, cloud

    if protocol not in ("http", "https"):
        return None, None

    device_key = os.environ.get("DEVICE_KEY")
    app_name = os.environ.get("APP_NAME")
    if not (device_key and app_name):
        return None, None

    label = f"{device_key}-{app_name.lower()}-{port}"
    if protocol == "https":
        # https-protocol tunnels get secure- prefixed subdomains platform-wide.
        label = f"secure-{label}"

    def _url(full_label, domain):
        # A tunnel label must be a single valid DNS label (CloudURLFor).
        if len(full_label) > 63 or "." in full_label:
            return None
        return f"https://{full_label}.{domain}"

    tunnel_domain = os.environ.get("TUNNEL_DOMAIN", "app.ironflock.com")
    if instance_key:
        cloud_domain = os.environ.get("CLOUD_TUNNEL_DOMAIN", "app.ironflock.com")
        return _url(label, tunnel_domain), _url(f"i{instance_key}-{label}", cloud_domain)
    return None, _url(label, tunnel_domain)


class Collector:
    def __init__(
        self, device_name, device_key, adapter, store_data=True, on_error=None, ports=None
    ):
        self.device_name = device_name
        self.device_key = device_key
        self.adapter = adapter
        # Optional reporting hook so an implementation can own the report_error
        # call. Signature: on_error(message, *, level, user_message, asset_name);
        # may be sync or async. When None (default) the core reports directly to
        # the platform error-logs table via the injected SDK handle.
        self.on_error = on_error
        # The app's declared remote-access ports — the `ports:` list of its
        # .ironflock/port-template.yml (or any subset), each a dict with
        # ``port`` plus optional ``name``/``protocol``/``main``/
        # ``remote_port_environment``. Resolved to per-scope access URLs
        # (local / appliance / cloud) and written to the gateway row's ``url``
        # column at registration so boards can list/embed a gateway's web UIs
        # (iframe src) — consumers pick a scope explicitly. Empty (the default)
        # leaves the ``url`` column untouched. Use
        # ``Collector.load_ports_from_template()`` to read them from the template.
        self.ports = list(ports) if ports else []
        # App-provided extra gateway columns, merged into every gateway
        # registration (register_gateway). Lets an app publish custom columns
        # it declared in its data-template (e.g. a cluster role) without
        # fighting the core's echo of existing row values. Core-owned columns
        # (gateway_name, deleted, info, url, tsp) always win over these.
        self.gateway_extra: dict = {}
        # Default when the gateway row has no store_data column. With
        # store_data False, configuration is still read live from the
        # platform but collected data is not written to it.
        self._default_store_data = store_data
        self.store_data = store_data
        self.ironflock = None
        self.store = None
        self.gateway = {}
        # The live datapoints catalog (kept in sync by the table subscription
        # and the store). Read per publish to gate writes by the user-set
        # ``enabled`` / ``change_detection`` columns.
        self.datapoints: list[dict] = []
        self.asset_tasks: dict[str, asyncio.Task] = {}
        self._asset_status: dict[str, str] = {}
        # Last value published per (asset_name, datapoint_id) for change
        # detection; only populated for change-detection datapoints.
        self._last_values: dict[tuple, object] = {}
        # Bounded FIFO of (table, payload) writes that could not be sent while
        # the cloud link was down; drained in order on reconnect.
        self._buffer = deque(maxlen=DEFAULT_BUFFER_SIZE)
        self._flush_lock = asyncio.Lock()
        # Cloud-link outage bookkeeping, used to inform the operator once on
        # recovery (never during the outage, when reporting can't get through):
        # whether the last flush attempt failed, and how many buffered rows the
        # bounded buffer dropped (oldest-first) while the link was down.
        self._link_down = False
        self._dropped_rows = 0

    def set_ironflock(self, ironflock):
        """Provide the IronFlock handle used for all table I/O. Must be called
        before ``run`` (which IronFlock invokes as its ``mainFunc``)."""
        self.ironflock = ironflock

    async def report_error(self, message, level="error", asset_name=None, user_message=None):
        """Surface an operational error to the operator. Best-effort (never raises).

        ``message`` is the technical text written to the error-logs ``msg`` column
        (the asset name is prefixed in, since the table has no asset column).
        ``user_message`` is the operator-facing line the board's toast shows — a
        friendly, self-contained sentence (already naming the asset); when None the
        toast falls back to the technical message. ``level`` follows the SDK
        convention (``error``/``warn``/``info``/``debug``); failures that stop data
        flowing use ``error`` (shown red by the toast).

        If an ``on_error`` hook was provided the implementation owns reporting: the
        hook is called with the resolved ``(message, level, user_message,
        asset_name)`` (awaited if it returns a coroutine). Otherwise the core writes
        to the platform error-logs table directly via the injected SDK handle."""
        text = f"{asset_name}: {message}" if asset_name else message
        if self.on_error is not None:
            try:
                result = self.on_error(
                    text, level=level, user_message=user_message, asset_name=asset_name
                )
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                print(f"on_error hook failed: {e}")
            return
        await emit_error(self.ironflock, text, level, user_message=user_message)

    async def report_exception(
        self, exc, *, level="error", asset_name=None, fallback_user_message=None
    ):
        """Report a caught exception through :meth:`report_error`.

        The technical text is ``str(exc)``. The toast's display line is the
        exception's ``user_message`` when it is a ``CollectorError`` (so adapters
        control the wording via the error they raise), otherwise
        ``fallback_user_message``. This is the single funnel the core's boundary
        handlers use, so typed adapter errors get meaningful toasts and any
        unexpected error still gets reported."""
        user_message = exc.user_message if isinstance(exc, CollectorError) else None
        await self.report_error(
            str(exc),
            level=level,
            asset_name=asset_name,
            user_message=user_message or fallback_user_message,
        )

    # ----------------------------------------------------------------- gateway

    def _apply_gateway_row(self, row):
        value = row.get("store_data")
        self.store_data = self._default_store_data if value is None else bool(value)
        if self.store is not None:
            self.store.store_data = self.store_data
        self._apply_buffer_size(row.get("buffer_size"))

    def _apply_buffer_size(self, value):
        try:
            size = int(value)
        except (TypeError, ValueError):
            size = DEFAULT_BUFFER_SIZE
        if size < 1:
            size = DEFAULT_BUFFER_SIZE
        if size != self._buffer.maxlen:
            # Resize, keeping the newest rows (deque drops the oldest overflow).
            self._buffer = deque(self._buffer, maxlen=size)

    async def load_gateway(self):
        """Load this gateway's own row (registry + per-gateway settings)."""
        rows = await self.ironflock.getHistory(
            "gateways",
            {
                "limit": 10,
                "filterAnd": [
                    {"column": "latest_flag", "operator": "=", "value": True},
                    {
                        "column": "gateway_name",
                        "operator": "=",
                        "value": self.device_name,
                    },
                ],
            },
        )
        self.gateway = rows[0] if rows else {}
        self._apply_gateway_row(self.gateway)
        print(f"Gateway settings: {self.gateway}")

    @staticmethod
    def load_ports_from_template(path=".ironflock/port-template.yml"):
        """Read the app's port template and return its ``ports:`` list, ready to
        pass as the ``ports`` argument.

        Optional convenience so an app need not re-implement the parse; the core
        keeps zero hard dependencies by importing PyYAML lazily here (only apps
        that call this helper need it). Returns ``[]`` when the file is absent
        (dev/demo) so callers can pass the result unconditionally, e.g.::

            Collector(..., ports=Collector.load_ports_from_template())
        """
        if not os.path.exists(path):
            print(f"port template not found at {path}; no remote-access URLs will be published")
            return []
        try:
            import yaml
        except ImportError:
            print(
                "load_ports_from_template needs PyYAML; install it or pass ports= "
                "explicitly. No remote-access URLs will be published."
            )
            return []
        with open(path) as f:
            doc = yaml.safe_load(f) or {}
        ports = doc.get("ports")
        return ports if isinstance(ports, list) else []

    def _resolve_remote_access_urls(self):
        """Resolve each declared port's access URLs for the gateway ``url`` column.

        Returns a dict **keyed by the port number** (as a string) mapping to
        ``{name, protocol, main, local?, appliance?, cloud?}`` — one entry per
        configured port — so the frontend can look a port up directly
        (``url["55000"]``) instead of scanning a list.

        There is deliberately NO ambient/default URL field: a consumer must
        choose the access scope explicitly. The three scopes are separate
        keys, each ``{... , url}`` with a directly usable URL (iframe ``src``
        for http/https ports):

        - ``local: {ip, port, url}`` — the LAN endpoint other devices on the
          same network reach directly (no tunnel). Present when the platform
          provides ``DEVICE_LAN_IP`` and ``DEVICE_PORT_FOR_<port>`` (the host
          port from the agent's managed pool).
        - ``appliance: {url}`` — the appliance-network route on the operator's
          appliance tunnel domain. Present only on instance devices
          (``INSTANCE_KEY`` set).
        - ``cloud: {url}`` — the internet-facing route: the cloud-forwarded
          ``i{INSTANCE_KEY}-…`` URL on instance devices, the plain tunnel URL
          on cloud-managed devices. Reachable once the port's tunnel is active.

        Best-effort: a port with no numeric ``port`` is skipped, and absent
        identity/env leaves the corresponding scope key out entirely (an entry
        can carry no scope at all, e.g. in local dev). If two specs declare
        the same port the last one wins.

        Breaking change in 2.7.0: the legacy top-level ``url`` field (the
        SDK-preferred route) is gone — bindings like ``url.55000.url`` must
        become ``url.55000.cloud.url`` (or ``.appliance.url`` / ``.local.url``).
        """
        urls = {}
        for spec in self.ports:
            try:
                port = int(spec["port"])
            except (KeyError, TypeError, ValueError):
                print(f"skipping remote-access port without a valid port: {spec!r}")
                continue
            protocol = str(spec.get("protocol") or "http").lower()
            remote_port_environment = spec.get("remote_port_environment")
            entry = {
                "name": spec.get("name") or str(port),
                "protocol": protocol,
                "main": bool(spec.get("main")),
            }
            lan_ip = os.environ.get("DEVICE_LAN_IP")
            mapped = os.environ.get(f"DEVICE_PORT_FOR_{port}")
            if lan_ip and mapped:
                try:
                    scheme = protocol if protocol in ("https", "tcp", "udp") else "http"
                    entry["local"] = {
                        "ip": lan_ip,
                        "port": int(mapped),
                        "url": f"{scheme}://{lan_ip}:{int(mapped)}",
                    }
                except ValueError:
                    print(f"ignoring non-numeric DEVICE_PORT_FOR_{port}={mapped!r}")
            appliance_url, cloud_url = _tunnel_access_urls(
                port, protocol, remote_port_environment
            )
            if appliance_url:
                entry["appliance"] = {"url": appliance_url}
            if cloud_url:
                entry["cloud"] = {"url": cloud_url}
            urls[str(port)] = entry
        return urls

    async def register_gateway(self):
        """Write the registry row, echoing existing columns so user-edited
        gateway settings survive the startup append (the appended row becomes
        the latest one). When the app declared remote-access ports, their
        resolved access scopes (local/appliance/cloud) are (re)written to the
        ``url`` column. App-provided ``gateway_extra`` columns are merged in
        before the core-owned fields, so the core always wins on those."""
        payload = {
            k: v for k, v in self.gateway.items() if k not in PLATFORM_COLUMNS
        }
        payload.update(self.gateway_extra)
        payload["gateway_name"] = self.device_name
        payload["deleted"] = False
        payload["info"] = {
            "app_started": _now(),
            "python": sys.version.split()[0],
        }
        # Derived, core-owned: recompute from the live tunnel/identity rather than
        # echoing a stale value forward.
        if self.ports:
            payload["url"] = self._resolve_remote_access_urls()
        payload["tsp"] = _now()
        await self.ironflock.append_to_table("gateways", payload)

    def _handle_gateway_update(self):
        async def handler(*args, **kwargs):
            row = args[0] if args else {}
            if row.get("gateway_name") != self.device_name:
                return
            print("Gateway settings updated:", row)
            self.gateway = row
            self._apply_gateway_row(row)
            try:
                await self.adapter.apply_settings(row)
            except Exception as e:
                print(f"apply_settings failed: {e}")
                await self.report_exception(
                    e,
                    level="warn",
                    fallback_user_message="Gateway settings could not be applied; using defaults.",
                )

        return handler

    # ------------------------------------------------------------------ status

    async def set_asset_status(self, asset, status, detail=""):
        """Record an asset state transition (online | offline | paused).

        Appends to ``assetstatus`` only when the status actually changes, so
        the table is a transition log (latest row = current state). Returns
        ``True`` when the status changed (a row was written), ``False`` when it
        was already in this state — callers gate transition-only side effects
        (e.g. reporting an error toast on the first offline) on this."""
        asset_name = asset["asset_name"]
        if self._asset_status.get(asset_name) == status:
            return False
        self._asset_status[asset_name] = status
        row = {
            "tsp": _now(),
            "asset_name": asset_name,
            "gateway_id": self.device_key,
            "status": status,
            "detail": str(detail)[:500],
            "deleted": False,
        }
        print("asset status:", row)
        await self._send("assetstatus", row)
        return True

    async def clear_asset_status(self, asset):
        """Soft-delete the status stream of a removed asset."""
        asset_name = asset["asset_name"]
        self._asset_status.pop(asset_name, None)
        row = {
            "tsp": _now(),
            "asset_name": asset_name,
            "gateway_id": self.device_key,
            "status": "offline",
            "detail": "asset deleted",
            "deleted": True,
        }
        await self._send("assetstatus", row)

    # ------------------------------------------------------------------ config

    async def soft_delete_asset(self, asset):
        """Append a deleted record to soft-delete an invalid asset config.

        Echoes the original row's columns back with deleted=True and a fresh
        tsp so the platform's latest_flag mechanism matches the existing row
        by its key.
        """
        payload = {
            k: v
            for k, v in asset.items()
            if k not in PLATFORM_COLUMNS and k != "datapoint_list"
        }
        payload["deleted"] = True
        payload["tsp"] = _now()
        print(f"Soft-deleting invalid asset config: {payload}")
        await self.ironflock.append_to_table("assets", payload)

    @staticmethod
    def _is_enabled(asset):
        # Null-safe pause switch: only an explicit false pauses the asset, so
        # rows without the column (and board forms that omit it) keep running.
        return asset.get("enabled") is not False

    async def configure_asset(self, asset, assets):
        """(Re)configure one asset row: resolve datapoints, start or stop its
        collection task according to ``enabled``."""
        assets[:] = [a for a in assets if a["asset_name"] != asset["asset_name"]]
        assets.append(asset)

        if not self._is_enabled(asset):
            print(f"Asset {asset['asset_name']} is paused")
            self.stop_asset_task(asset["asset_name"])
            await self.set_asset_status(asset, "paused")
            return

        if not self.adapter.resolve_each_cycle:
            asset["datapoint_list"] = await self.adapter.prepare_datapoints(
                asset, self.store
            )
            print("datapoint_list", asset["datapoint_list"])
        self.start_asset_task(asset)

    async def load_asset_configs(self):
        """Load asset configurations from the assets table."""
        # No gateway_id filter here: we need to see invalid/orphaned rows
        # (empty asset_name or null gateway_id) so we can clean them up.
        # Configuration is scoped to this device's gateway in the loop below.
        result = await self.ironflock.getHistory(
            "assets",
            {
                "limit": 1000,
                "filterAnd": [
                    {"column": "latest_flag", "operator": "=", "value": True},
                    {"column": "deleted", "operator": "=", "value": False},
                ],
            },
        )
        if not result:
            print(
                "No assets found in the assets table. Please add asset configurations first."
            )
            await self.report_error(
                "no assets found in the assets table",
                level="info",
                user_message="No assets configured yet — add an asset configuration to start collecting.",
            )
        print(f"Loaded {len(result)} asset(s) from assets table")

        configured_assets = []
        for asset in result:
            asset_name = (asset.get("asset_name") or "").strip()
            gateway_id = asset.get("gateway_id")

            # Clean up invalid configs: empty asset name or no gateway assignment.
            if not asset_name or gateway_id is None:
                try:
                    await self.soft_delete_asset(asset)
                    await self.report_error(
                        f"removed invalid asset config (asset_name={asset_name!r}, "
                        f"gateway_id={gateway_id!r})",
                        level="warn",
                        user_message=(
                            f"An incomplete asset configuration"
                            f"{f' ({asset_name!r})' if asset_name else ''} was removed — "
                            "it was missing a name or gateway assignment."
                        ),
                    )
                except Exception as e:
                    print(f"Failed to soft-delete invalid asset: {e}")
                continue

            # Only configure assets assigned to this device's gateway.
            if int(gateway_id) != self.device_key:
                continue

            print(f"Configuring asset: {asset_name}")
            try:
                await self.configure_asset(asset, configured_assets)
            except Exception as e:
                print(f"Error configuring asset {asset_name}, skipping: {e}")
                await self.report_exception(
                    e,
                    asset_name=asset_name,
                    fallback_user_message=f"'{asset_name}' has an invalid configuration.",
                )

        await self.ironflock.subscribe_to_table(
            "assets", self._handle_asset_update(configured_assets)
        )
        return configured_assets

    def _handle_asset_update(self, assets):
        async def handler(*args, **kwargs):
            asset = args[0] if args else {}
            gateway_id = asset.get("gateway_id")
            if gateway_id is None or int(gateway_id) != self.device_key:
                print(f"Received asset update for gateway_id {gateway_id}, ignoring")
                return
            if asset.get("deleted"):
                assets[:] = [
                    a for a in assets if a["asset_name"] != asset["asset_name"]
                ]
                self.stop_asset_task(asset["asset_name"])
                self._clear_datapoint_values(asset["asset_name"])
                try:
                    await self.clear_asset_status(asset)
                except Exception as e:
                    print(f"Failed to clear status of {asset['asset_name']}: {e}")
            else:
                try:
                    await self.configure_asset(asset, assets)
                except Exception as e:
                    asset_name = asset.get("asset_name", "unknown")
                    print(f"Error configuring asset {asset_name}: {e}")
                    traceback.print_exc()
                    await self.report_exception(
                        e,
                        asset_name=asset_name,
                        fallback_user_message=f"'{asset_name}' has an invalid configuration.",
                    )

        return handler

    async def load_datapoints(self):
        """Load the datapoint catalog from the datapoints table."""
        datapoints = await self.ironflock.getHistory(
            "datapoints",
            {
                "limit": 3000,
                "filterAnd": [
                    {"column": "latest_flag", "operator": "=", "value": True},
                    {"column": "deleted", "operator": "=", "value": False},
                    {"column": "gateway_id", "operator": "=", "value": self.device_key},
                ],
            },
        )
        # Subscribe to, return and store the SAME list object so the live
        # subscription, the store and the collector's gating all stay in sync
        # (a fresh [] here would diverge from the subscribed list on empty
        # startup, hiding later catalog/edit updates until a restart).
        if not datapoints:
            datapoints = []
        await self.ironflock.subscribe_to_table(
            "datapoints", self._handle_datapoint_update(datapoints)
        )

        if datapoints:
            print(f"Loaded {len(datapoints)} datapoint(s) from datapoints table")
        else:
            print("No datapoints found in the datapoints table.")

        return datapoints

    def _handle_datapoint_update(self, datapoints):
        def handler(*args, **kwargs):
            row = args[0] if args else {}
            gateway_id = row.get("gateway_id")
            if gateway_id is None or int(gateway_id) != self.device_key:
                print(f"Received datapoint update for gateway_id {gateway_id}, ignoring")
                return

            datapoints[:] = [
                dp
                for dp in datapoints
                if dp["datapoint_id"] != row["datapoint_id"]
                or dp["asset_name"] != row["asset_name"]
            ]
            if not row["deleted"]:
                datapoints.append(row)

            print("Updated datapoints with row:", row)

        return handler

    # -------------------------------------------------------------- task mgmt

    def stop_asset_task(self, asset_name):
        task = self.asset_tasks.pop(asset_name, None)
        if task is not None:
            task.cancel()

    def start_asset_task(self, asset):
        name = asset["asset_name"]
        self.stop_asset_task(name)
        self.asset_tasks[name] = asyncio.create_task(self.collect_asset(asset))

    # ---------------------------------------------------------- outbound writes

    async def _send(self, table, payload):
        """Write a data row to the platform, buffering it if the link is down.

        The single write path for the live data streams (measurements,
        assetstatus). Forward-only gateways (``store_data`` False) drop the
        row. Otherwise the row is appended to the bounded buffer and an
        opportunistic flush is attempted; a network failure leaves the row
        (and any backlog) buffered for the next flush instead of raising, so
        collection keeps running during a cloud outage.
        """
        if not self.store_data:
            return
        if self._buffer.maxlen is not None and len(self._buffer) >= self._buffer.maxlen:
            self._dropped_rows += 1  # deque drops the oldest row silently
        self._buffer.append((table, payload))
        await self._flush()

    async def _flush(self):
        """Drain the buffer oldest-first while the link is up.

        Rows are sent with the SDK's bulk ``append_rows_to_table``: the backlog
        is snapshotted (bounded to the current length so concurrently appended
        rows can't livelock the drain) and consecutive rows for the same table
        are written in one call, capped at ``BULK_CHUNK_MAX`` rows so a long
        backlog is split into several bounded inserts rather than one oversized
        one. A reconnect after a long outage then catches up in a handful of
        bulk requests instead of thousands of single-row ones. Stops at the
        first failure, re-queuing the un-sent rows oldest-first so the backlog
        (and ordering) is preserved.
        """
        async with self._flush_lock:
            pending = []
            for _ in range(len(self._buffer)):
                if not self._buffer:
                    break
                pending.append(self._buffer.popleft())

            i, n = 0, len(pending)
            while i < n:
                table = pending[i][0]
                j = i
                rows = []
                while j < n and pending[j][0] == table and len(rows) < BULK_CHUNK_MAX:
                    rows.append(pending[j][1])
                    j += 1
                try:
                    await self.ironflock.append_rows_to_table(table, rows)
                except Exception as e:
                    # Still down: re-queue everything not yet sent, in order.
                    self._buffer.extendleft(reversed(pending[i:]))
                    self._link_down = True
                    print(f"buffered {len(self._buffer)} row(s); link down: {e}")
                    return
                i = j

            # Backlog fully drained after an outage: tell the operator once,
            # escalating to a warning when the bounded buffer overflowed and
            # readings were lost.
            if n and self._link_down:
                self._link_down = False
                dropped, self._dropped_rows = self._dropped_rows, 0
                if dropped:
                    await self.report_error(
                        f"link restored; delivered {n} buffered row(s), "
                        f"{dropped} oldest row(s) dropped on buffer overflow",
                        level="warn",
                        user_message=(
                            f"Connection restored — {n} buffered readings were "
                            f"delivered, but {dropped} were lost while offline."
                        ),
                    )
                else:
                    await self.report_error(
                        f"link restored; delivered {n} buffered row(s)",
                        level="info",
                        user_message="Connection restored — all buffered readings were delivered.",
                    )

    async def _flush_loop(self):
        """Periodically retry draining the buffer, so a backlog is sent on
        reconnect even when no new data is flowing (device also down, assets
        paused)."""
        while True:
            await asyncio.sleep(FLUSH_INTERVAL)
            if self._buffer:
                try:
                    await self._flush()
                except Exception as e:
                    print(f"flush loop error: {e}")

    # -------------------------------------------------------------- collection

    def _datapoints_provider(self, asset):
        """Async callable handed to the session's stream: returns the current
        datapoint list, re-resolving per cycle for resolve_each_cycle
        adapters and lazily re-running prepare_datapoints for cached adapters
        whenever the cache was invalidated (DatapointsChanged)."""

        async def provider():
            if self.adapter.resolve_each_cycle:
                return await self.adapter.prepare_datapoints(asset, self.store)
            datapoints = asset.get("datapoint_list")
            if datapoints is None:
                datapoints = await self.adapter.prepare_datapoints(asset, self.store)
                asset["datapoint_list"] = datapoints
                print("datapoint_list (re-resolved)", datapoints)
            return datapoints

        return provider

    def _datapoint_catalog(self, asset_name):
        """Map ``datapoint_id`` -> live catalog row for one asset, used to read
        the per-datapoint ``enabled`` / ``change_detection`` settings."""
        return {
            row.get("datapoint_id"): row
            for row in self.datapoints
            if row.get("asset_name") == asset_name
        }

    def _clear_datapoint_values(self, asset_name):
        """Drop the change-detection cache for a removed asset."""
        for key in [k for k in self._last_values if k[0] == asset_name]:
            del self._last_values[key]

    async def _publish_batch(self, asset, datapoints, raw):
        """Assemble and write one measurements row from a yielded batch.

        Iterates the raw map's keys (not the datapoint list) so push
        protocols can deliver partial batches without writing None entries
        for absent datapoints. An empty raw map writes nothing. Returns
        whether anything was published.

        Each datapoint is gated by its catalog row: an explicit
        ``enabled == False`` drops it (per-datapoint pause), and with
        ``change_detection`` set it is written only when its value differs from
        the last published value. With both off the full snapshot is written as
        before; when gating empties the row, nothing is sent.
        """
        asset_name = asset["asset_name"]
        by_id = {dp["id"]: dp for dp in datapoints if dp.get("id")}
        catalog = self._datapoint_catalog(asset_name)
        payload = {
            "tsp": _now(),
            "asset_name": asset_name,
            "gateway_id": self.device_key,
            "data": {},
        }
        for datapoint_id, raw_value in (raw or {}).items():
            datapoint = by_id.get(datapoint_id)
            if datapoint is None:
                continue
            meta = catalog.get(datapoint_id)
            if meta is not None and meta.get("enabled") is False:
                continue  # datapoint paused
            entry = self.adapter.make_entry(asset, datapoint, raw_value)
            if meta is not None and bool(meta.get("change_detection")):
                key = (asset_name, datapoint_id)
                value = entry.get("value")
                if self._last_values.get(key, _UNSET) == value:
                    continue  # unchanged since last publish
                self._last_values[key] = value
            payload["data"][datapoint_id] = entry

        if not payload["data"]:
            return False
        print(payload)
        await self._send("measurements", payload)
        return True

    async def _mark_online(self, asset):
        """Record the online status; on a recovery (offline -> online) also
        inform the operator, mirroring the one-toast-per-transition rule the
        offline path follows. First-ever online stays quiet — a toast per
        asset at every startup would be noise, not signal."""
        asset_name = asset["asset_name"]
        was_offline = self._asset_status.get(asset_name) == "offline"
        if await self.set_asset_status(asset, "online") and was_offline:
            await self.report_error(
                "recovered; data is flowing again",
                level="info",
                asset_name=asset_name,
                user_message=f"'{asset_name}' is back online.",
            )

    async def collect_asset(self, asset):
        """Session-lifecycle loop for one asset: create a session, consume its
        stream (or pace demo values), and on any failure close it, mark the
        asset offline, back off one interval and start over.
        DatapointsChanged invalidates the cached datapoint list and restarts
        without backoff."""
        asset_name = asset["asset_name"]
        interval = max(int(asset.get("collect_interval") or 1), 1)
        provider = self._datapoints_provider(asset)
        while True:
            backoff = interval
            session = self.adapter.create_session(asset)
            try:
                if asset.get("demo_mode"):
                    # Core-paced demo loop: works for push protocols too,
                    # since no server-side subscription exists in demo mode.
                    while True:
                        datapoints = await provider()
                        if await self._publish_batch(
                            asset, datapoints, session.demo_values(datapoints)
                        ):
                            await self._mark_online(asset)
                        await asyncio.sleep(interval)
                else:
                    stream = session.stream(provider, interval)
                    async with aclosing(stream):
                        async for datapoints, raw in stream:
                            if await self._publish_batch(asset, datapoints, raw):
                                await self._mark_online(asset)
                    print(f"collect_asset {asset_name}: stream ended, restarting")
            except asyncio.CancelledError:
                raise
            except DatapointsChanged:
                print(f"collect_asset {asset_name}: datapoints changed, re-discovering")
                asset.pop("datapoint_list", None)
                backoff = 0
            except Exception as e:
                print(f"collect_asset {asset_name} error: {e}")
                try:
                    # Report to the board's toast only on the actual transition
                    # to offline, not on every retry cycle, so a persistently
                    # failing asset pops one toast (and one again after a
                    # recovery + new failure), never a stream of them.
                    if await self.set_asset_status(asset, "offline", str(e)):
                        await self.report_exception(
                            e,
                            asset_name=asset_name,
                            fallback_user_message=f"{asset_name} is not responding",
                        )
                except Exception as status_error:
                    print(f"Failed to record offline status: {status_error}")
            finally:
                await session.close()
            await asyncio.sleep(backoff)

    # ------------------------------------------------------------------- run

    async def run(self):
        try:
            await self.load_gateway()
            try:
                await self.adapter.apply_settings(self.gateway)
            except Exception as e:
                print(f"apply_settings failed, using adapter defaults: {e}")
                await self.report_exception(
                    e,
                    level="warn",
                    fallback_user_message="Gateway settings could not be applied; using defaults.",
                )

            self.datapoints = await self.load_datapoints()
            self.store = DatapointStore(
                self.ironflock, self.device_key, self.datapoints, store_data=self.store_data
            )
            await self.load_asset_configs()

            # Drains the offline buffer on reconnect even when no new data flows.
            asyncio.create_task(self._flush_loop())

            # Table subscriptions are active now, so asset rows appended by
            # protocol background work (e.g. network discovery) are picked up.
            # A discovery failure must not abort startup — report and continue.
            try:
                await self.adapter.start_background(self)
            except Exception as e:
                print(f"start_background failed: {e}")
                await self.report_exception(
                    e,
                    fallback_user_message="Background device discovery failed to start.",
                )

            await self.ironflock.subscribe_to_table(
                "gateways", self._handle_gateway_update()
            )
            await self.register_gateway()
        except Exception as e:
            # Fatal startup failure (config load, subscription, registration):
            # surface it to the operator, then re-raise so the runtime sees the
            # collector could not start.
            print(f"collector startup failed: {e}")
            traceback.print_exc()
            await self.report_exception(
                e, fallback_user_message="The collector failed to start."
            )
            raise

        while True:
            await asyncio.sleep(3600)
