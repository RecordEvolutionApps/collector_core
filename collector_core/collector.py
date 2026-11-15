"""Protocol-agnostic collector core.

Owns everything that is identical regardless of the wire protocol: registering
the gateway, following the board-written configuration tables
(``gateway_settings``, ``assets``, ``datapoint_settings``) subscribe-first,
managing one collection task per asset (paced polling or protocol-pushed
streams), keeping the ``datapoints`` catalog, assembling the ``measurements``
payload, tracking asset online/offline/paused state in ``assetstatus``,
writing to IronFlock, and refreshing the app's transforms after its writes.
All protocol specifics are delegated to an injected ``ProtocolAdapter`` (see
``collector_core.adapter``).

Data writes (measurements, datapoints, assetstatus) can be disabled per
gateway via the ``store_data`` setting for forward-only deployments;
configuration is still read live from the platform.
"""

import asyncio
import os
import sys
import time
import traceback
from collections import deque
from contextlib import aclosing
from datetime import datetime, timezone

from collector_core.adapter import (
    DEMO_DATAPOINT_COLUMNS,
    PLATFORM_COLUMNS,
    DatapointStore,
    DatapointsChanged,
    emit_error,
)
from collector_core.errors import CollectorError
from collector_core.storable import storable

# Max data rows buffered locally while the cloud link is down, when the
# gateway_settings row sets no buffer_size. Oldest rows are dropped on overflow.
DEFAULT_BUFFER_SIZE = 10000

# How often the background flusher retries draining the buffer (seconds).
FLUSH_INTERVAL = 15

# Max rows per bulk insert, so catching up a large backlog is split into
# several capped requests instead of one oversized one.
BULK_CHUNK_MAX = 5000

# Sentinel for "no value seen yet" in the change-detection cache, so a genuine
# None reading is distinguished from a datapoint that has never been published.
_UNSET = object()

# Keeping entity rows alive. Table retention (dropAfter) drops whole chunks,
# the latest row of an entity included, so a catalog row or an asset status
# that never changes ages out of its table and the board loses the datapoint.
# The core rewrites the latest datapoints and assetstatus rows it owns once they are
# older than REASSERT_AFTER (seconds), checking every REASSERT_INTERVAL. Both
# must stay far below the shortest dropAfter an app gives those tables (the
# core template uses 3 months).
REASSERT_AFTER = 7 * 24 * 3600
REASSERT_INTERVAL = 6 * 3600

# A repaired (unstorable) row is reported at most once per asset, table and
# kind of repair in this window, so a device sending bad values every second
# produces one toast an hour, not one per row.
REPAIR_REPORT_INTERVAL = 3600

# Following the config tables. The core subscribes first and reads the current
# rows afterwards, so a row saved in between is not lost, and applies a row
# only when its tsp is newer than the last one applied for that entity. A row
# can arrive twice, from the read and from the subscription, with its tsp in
# two forms (epoch milliseconds from the read, ISO text from the event); rows
# within this many seconds of each other count as the same row.
SAME_ROW_WINDOW = 0.001

# The read filter for the current row of every entity (the data backend
# derives it from the table's maintainLatestFlagFor columns). The former
# physical latest_flag column no longer exists.
LATEST = {"latest": True}

# Refreshing the app's transforms after the core writes a table they read
# (see Collector.request_refresh). Two refreshes of one transform are at least
# REFRESH_MIN_INTERVAL seconds apart, so a mapping driven by measurements
# cannot hammer the data backend. A refresh refused with
# URI_REFRESH_IN_PROGRESS (a scheduled refresh holds the view) is retried once
# after REFRESH_RETRY_DELAY seconds.
REFRESH_MIN_INTERVAL = 2.0
REFRESH_RETRY_DELAY = 2.0
REFRESH_IN_PROGRESS = "URI_REFRESH_IN_PROGRESS"


def _now():
    return datetime.now(timezone.utc).isoformat()


def _epoch_seconds(value):
    """A row's ``tsp`` as epoch seconds, or None when it cannot be read.

    History reads return timestamps as epoch milliseconds, the core's own
    writes and the table events carry ISO strings, and a test may pass a
    datetime; all three must agree for the keep-alive to judge a row's age.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return moment.timestamp()
    if isinstance(value, (int, float)):
        return value / 1000.0 if value > 1e11 else float(value)
    text = str(value).strip()
    try:
        return _epoch_seconds(float(text))
    except ValueError:
        pass
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _epoch_seconds(moment)


def _tolerate_unencodable_output(streams=None):
    """Let the process log any text, lone surrogates included.

    The core and its adapters print device-derived text (an exception quoting
    a payload, a datapoint name). In the app image stdout is strict UTF-8, so
    printing a lone surrogate raises UnicodeEncodeError — inside the very
    handler that reports a failed asset, which then ends the asset's task.
    ``backslashreplace`` prints such a character as ``\\ud800`` instead.
    ``Collector.run`` is the app's main function, so the process is the
    core's to configure.
    """
    for stream in streams if streams is not None else (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="backslashreplace")
        except Exception:
            pass  # not a TextIOWrapper (pytest capture, a pipe wrapper): leave it


def outage_cause(exc):
    """Name the layer behind a failed platform write, for the outage log line.

    The collector only ever sees the exception the SDK raises, but its text
    often carries the platform's own error. The one worth telling apart is
    the platform's table service failing to reach *its* Postgres (pg-pool
    reporting ECONNREFUSED on port 5432): the gateway's link to the router
    is fine, and nothing on the gateway can fix it. Everything else stays
    the generic "link down" (router unreachable, auth, timeouts).
    """
    text = str(exc)
    if "ECONNREFUSED" in text and ("5432" in text or "pg-pool" in text):
        return "platform database unreachable (platform-side, not the gateway link)"
    return "link down"


class _TransformRefresh:
    """Refresh bookkeeping of one transform (see ``Collector.request_refresh``).

    ``requested`` counts the requests, ``completed`` is the request count the
    last finished refresh covered: a refresh that starts after a request
    covers it. One worker task at a time runs the refreshes, so at most one is
    in flight and the requests made meanwhile wait as one more refresh.
    """

    __slots__ = ("changed", "completed", "last", "requested", "worker")

    def __init__(self):
        self.requested = 0
        self.completed = 0
        self.last = None  # monotonic end of the last refresh
        self.worker = None
        self.changed = asyncio.Condition()


def _transform_lists(mapping):
    """``refresh_transforms`` as {table: [transform, ...]}; a single name may
    be given as a string."""
    lists = {}
    for table, names in (mapping or {}).items():
        if isinstance(names, str):
            names = [names]
        lists[table] = [name for name in names if name]
    return lists


class Collector:
    def __init__(
        self,
        device_name,
        device_key,
        adapter,
        store_data=True,
        on_error=None,
        ports=None,
        refresh_transforms=None,
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
        # (local / cloud) and written to the gateway row's ``url``
        # column at registration so boards can list/embed a gateway's web UIs
        # (iframe src) — consumers pick a scope explicitly. Empty (the default)
        # leaves the ``url`` column out. Use
        # ``Collector.load_ports_from_template()`` to read them from the template.
        self.ports = list(ports) if ports else []
        # App-provided extra gateway columns, merged into the registration row
        # (register_gateway), e.g. a cluster role the app declared in its
        # data-template. Core-owned columns (gateway_id, gateway_name, info,
        # url, deleted, tsp) always win over these.
        self.gateway_extra: dict = {}
        # The app's transforms (views in its data-template) to refresh after
        # the core writes, or applies a row of, a table they read:
        # {table: [transform, ...]}, each list in bottom-up order (a transform
        # before the ones that read it). Needs ironflock >= 1.11.0 for
        # refresh_transform; see request_refresh.
        self.refresh_transforms = _transform_lists(refresh_transforms)
        # Default when the gateway_settings row has no store_data value. With
        # store_data False, configuration is still read live from the
        # platform but collected data is not written to it.
        self._default_store_data = store_data
        self.store_data = store_data
        self.ironflock = None
        self.store = None
        # This gateway's row of gateway_settings in force ({} until the board
        # writes one).
        self.settings: dict = {}
        # The datapoints catalog the core writes (through the store), read at
        # startup. Shared with the store, which keeps it current.
        self.datapoints: list[dict] = []
        # The user's per-datapoint switches from datapoint_settings, keyed
        # (asset_name, datapoint_id): enabled, change_detection and the demo
        # tuning. Read per publish to gate writes and shape demo values.
        self.datapoint_settings: dict[tuple, dict] = {}
        self.asset_tasks: dict[str, asyncio.Task] = {}
        self._asset_status: dict[str, str] = {}
        # Last value published per (asset_name, datapoint_id) for change
        # detection; only populated for change-detection datapoints.
        self._last_values: dict[tuple, object] = {}
        # Bounded FIFO of (table, payload) writes that could not be sent while
        # the cloud link was down; drained in order on reconnect.
        self._buffer = deque(maxlen=DEFAULT_BUFFER_SIZE)
        self._flush_lock = asyncio.Lock()
        self._draining = set()
        # Whether a background flush is scheduled but has not taken the
        # backlog yet (see _schedule_flush).
        self._flush_queued = False
        # Cloud-link outage bookkeeping, used to inform the operator once on
        # recovery (never during the outage, when reporting can't get through):
        # whether the last flush attempt failed, and how many buffered rows the
        # bounded buffer dropped (oldest-first) while the link was down.
        self._link_down = False
        self._dropped_rows = 0
        # Last write per asset of its assetstatus row: (monotonic time, the
        # row's status and detail), so the keep-alive can rewrite it before
        # retention drops it.
        self._status_written: dict[str, tuple] = {}
        # (asset, table, repairs) -> monotonic time of its last report.
        self._repair_reported: dict[tuple, float] = {}
        # Incomplete asset rows (no name or no gateway) already reported.
        self._incomplete_reported: set = set()
        # Overridable per instance (tests, apps with an unusually short
        # dropAfter); see REASSERT_AFTER.
        self.reassert_after = REASSERT_AFTER
        # (table, entity key) -> tsp (epoch seconds) of the last config row
        # applied, so an older or repeated row is not applied again.
        self._applied_tsp: dict[tuple, float] = {}
        # One lock per asset name: configuring an asset awaits discovery, and
        # two rows of the same asset must not be applied out of order.
        self._asset_locks: dict[str, asyncio.Lock] = {}
        # Transform refreshes (see request_refresh): per transform its
        # bookkeeping, per table whether a refresh of its list runs and
        # whether another was requested meanwhile, and the background tasks,
        # held until they end (the loop keeps tasks only weakly).
        self._transform_refresh: dict[str, _TransformRefresh] = {}
        self._table_refresh_running: set = set()
        self._table_refresh_pending: set = set()
        self._refreshing: set = set()
        self._refresh_unsupported_reported = False

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

    # ------------------------------------------------------------ config rows

    def _is_newer(self, table, key, row):
        """Whether ``row`` is newer, by its tsp, than the last row of this
        entity applied from ``table``; records it when it is.

        A row without a readable tsp is always applied (nothing to compare).
        The same row delivered twice, by the startup read and by the
        subscription, is applied once."""
        when = _epoch_seconds(row.get("tsp"))
        if when is None:
            return True
        last = self._applied_tsp.get((table, key))
        if last is not None and when <= last + SAME_ROW_WINDOW:
            return False
        self._applied_tsp[(table, key)] = when
        return True

    def _is_own(self, row):
        """Whether a row of a per-gateway table (gateway_settings, assets,
        datapoints, datapoint_settings) belongs to this gateway."""
        gateway_id = row.get("gateway_id")
        try:
            return gateway_id is not None and int(gateway_id) == self.device_key
        except (TypeError, ValueError):
            return False

    # ------------------------------------------------------- gateway settings

    def _apply_settings_row(self, row):
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

    def _take_settings_row(self, row):
        """Make ``row`` this gateway's settings if it is this gateway's row of
        ``gateway_settings`` and newer than the row in force. Returns whether
        it was taken."""
        if not self._is_own(row):
            return False
        if not self._is_newer("gateway_settings", self.device_key, row):
            return False
        self.settings = row
        self._apply_settings_row(row)
        self.request_refresh("gateway_settings")
        return True

    async def load_gateway_settings(self):
        """Follow ``gateway_settings``: subscribe, then read this gateway's
        current row.

        The board writes the row; a row the subscription already applied is
        newer than or the same as the one read here, and the read does not
        replace it. Without a row the defaults apply (``store_data`` from the
        constructor, ``DEFAULT_BUFFER_SIZE``). The adapter's
        ``apply_settings`` is called by ``run`` after this, and by the
        subscription on every later row."""
        await self.ironflock.subscribe_to_table(
            "gateway_settings", self._handle_settings_update()
        )
        rows = await self.ironflock.getHistory(
            "gateway_settings",
            {
                "limit": 1,
                "filterAnd": [
                    LATEST,
                    {"column": "gateway_id", "operator": "=", "value": self.device_key},
                ],
            },
        )
        if rows:
            self._take_settings_row(rows[-1])
        if not self.settings:
            self._apply_settings_row({})
        print(f"Gateway settings: {self.settings}")

    def _handle_settings_update(self):
        async def handler(*args, **kwargs):
            row = args[0] if args else {}
            if not self._take_settings_row(row):
                return
            print("Gateway settings updated:", row)
            await self._apply_adapter_settings(self.settings)

        return handler

    async def _apply_adapter_settings(self, row):
        try:
            await self.adapter.apply_settings(row)
        except Exception as e:
            print(f"apply_settings failed, using adapter defaults: {e}")
            await self.report_exception(
                e,
                level="warn",
                fallback_user_message="Gateway settings could not be applied; using defaults.",
            )

    # ---------------------------------------------------------------- gateway

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
        ``{name, protocol, main, local?, cloud?}`` — one entry per configured
        port — so the frontend can look a port up directly (``url["55000"]``)
        instead of scanning a list.

        There is deliberately NO ambient/default URL field: a consumer must
        choose the access scope explicitly. The scopes are separate keys, each
        ``{... , url}`` with a directly usable URL (iframe ``src`` for
        http/https ports):

        - ``local: {ip, port, url}`` — the LAN endpoint other devices on the
          same network reach directly (no tunnel). Present when the platform
          provides ``DEVICE_LAN_IP`` and ``DEVICE_PORT_FOR_<port>`` (the host
          port from the agent's managed pool).
        - ``cloud: {url}`` — the tunnel URL, exactly as the ironflock SDK's
          ``getRemoteAccessUrlForPort`` returns it (see ``_tunnel_url``).
          Reachable once the port's tunnel is active.

        Best-effort: a port with no numeric ``port`` is skipped, and absent
        identity/env leaves the corresponding scope key out entirely (an entry
        can carry no scope at all, e.g. in local dev). If two specs declare
        the same port the last one wins.

        Breaking change in 2.7.0: the legacy top-level ``url`` field is gone —
        bindings like ``url.55000.url`` must become ``url.55000.cloud.url``
        (or ``.local.url``). The ``appliance`` scope that 2.7.0 added is gone
        again: it came from a copy of the SDK's tunnel rules, and the SDK
        exposes no appliance route.
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
            cloud_url = self._tunnel_url(port, protocol, remote_port_environment)
            if cloud_url:
                entry["cloud"] = {"url": cloud_url}
            urls[str(port)] = entry
        return urls

    def _tunnel_url(self, port, protocol, remote_port_environment):
        """The tunnel URL of a declared port, from the SDK handle's
        ``getRemoteAccessUrlForPort``, or None.

        The SDK owns the platform's tunnel rules: the label, the ``secure-``
        prefix of https ports, the ``-dev`` suffix of a development run
        (``ENV`` other than PROD, SDK >= 1.11.1), the ``i{INSTANCE_KEY}-``
        route of an instance device, the tunnel domains and the remote ports
        the agent announces. The core used to copy those rules, and the copy
        fell behind: dev runs got the PROD install's URL. So it asks the SDK
        instead, the way every other IronFlock app does. A handle without the
        method (SDK < 1.5.3) or a failing call gives None and leaves the scope
        out.
        """
        get_url = getattr(self.ironflock, "getRemoteAccessUrlForPort", None)
        if get_url is None:
            return None
        try:
            return get_url(port, protocol, remote_port_environment) or None
        except Exception as e:
            print(f"remote-access URL for port {port} ({protocol}) unavailable: {e}")
            return None

    async def register_gateway(self):
        """Write this gateway's row of ``gateways``: the gateway registers
        itself, keyed by ``gateway_id`` (its DEVICE_KEY).

        The row is ``{tsp, gateway_id, gateway_name, info, url, deleted:
        False}``, ``url`` only when the app declared remote-access ports
        (recomputed from the live tunnel/identity on every start). App-provided
        ``gateway_extra`` columns are merged in first, so the core-owned
        columns win. ``deleted: False`` brings back a gateway a user deleted
        on the board, since it runs again. Nothing of an earlier row is
        echoed: the board writes only delete rows to this table, and the
        user's settings live in ``gateway_settings``."""
        payload = dict(self.gateway_extra)
        payload["gateway_id"] = self.device_key
        payload["gateway_name"] = self.device_name
        payload["info"] = {
            "app_started": _now(),
            "python": sys.version.split()[0],
        }
        if self.ports:
            payload["url"] = self._resolve_remote_access_urls()
        else:
            payload.pop("url", None)
        payload["deleted"] = False
        payload["tsp"] = _now()
        await self.ironflock.append_to_table("gateways", payload)
        self.request_refresh("gateways")

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
        self._status_written[asset_name] = (time.monotonic(), status, row["detail"])
        await self._send("assetstatus", row)
        return True

    async def clear_asset_status(self, asset):
        """Soft-delete the status stream of a removed asset."""
        asset_name = asset["asset_name"]
        self._asset_status.pop(asset_name, None)
        self._status_written.pop(asset_name, None)
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
        """Follow the assets table: subscribe, then read the current rows and
        configure this gateway's assets.

        Subscribing first means no row saved during the read is lost; each row
        goes through :meth:`_apply_asset_row`, which applies only rows newer
        than the one in force. A row whose ``deleted`` is NULL is a live asset
        (board forms and older rows leave the flag out).

        The core never writes ``assets`` (the board and the AI assistant do).
        An incomplete row, without a name or a gateway, is skipped and
        reported once (:meth:`_skip_incomplete_asset`); the user deletes it on
        the board. For an asset deleted while the gateway was not running, the
        catalog rows still in force are soft-deleted here."""
        configured_assets = []
        await self.ironflock.subscribe_to_table(
            "assets", self._handle_asset_update(configured_assets)
        )
        # No gateway_id filter: rows without a gateway are reported too.
        # Configuration is scoped to this gateway in the loop below.
        rows = await self.ironflock.getHistory(
            "assets", {"limit": 1000, "filterAnd": [LATEST]}
        )
        live = [row for row in rows or [] if not row.get("deleted")]
        own = [row for row in live if self._is_own(row)]
        if not own:
            print(
                "No assets found in the assets table. Please add asset configurations first."
            )
            await self.report_error(
                "no assets found in the assets table",
                level="info",
                user_message="No assets configured yet — add an asset configuration to start collecting.",
            )
        print(f"Loaded {len(own)} asset(s) from assets table")

        for asset in live:
            if await self._skip_incomplete_asset(asset):
                continue
            if self._is_own(asset):
                await self._apply_asset_row(asset, configured_assets)

        for asset in rows or []:
            name = (asset.get("asset_name") or "").strip()
            if asset.get("deleted") and name and self._is_own(asset):
                await self._delete_catalog(name)

        return configured_assets

    async def _skip_incomplete_asset(self, asset):
        """Whether ``asset`` is an incomplete row to skip: no ``gateway_id``,
        or this gateway's row without an ``asset_name``. Each one is reported
        once (warn); rows of other gateways are theirs to report. Deleted rows
        are never reported."""
        name = (asset.get("asset_name") or "").strip()
        gateway_id = asset.get("gateway_id")
        if gateway_id is not None and name:
            return False
        if gateway_id is not None and not self._is_own(asset):
            return True
        if asset.get("deleted"):
            return True
        key = (str(gateway_id), name)
        if key in self._incomplete_reported:
            return True
        self._incomplete_reported.add(key)
        await self.report_error(
            f"skipped incomplete asset row (asset_name={name!r}, gateway_id={gateway_id!r})",
            level="warn",
            user_message=(
                f"An asset configuration{f' ({name!r})' if name else ''} is incomplete: "
                f"it has no {'gateway' if gateway_id is None else 'name'}, so it is not "
                "collected. Complete it or delete it on the board."
            ),
        )
        return True

    def _asset_lock(self, asset_name):
        lock = self._asset_locks.get(asset_name)
        if lock is None:
            lock = self._asset_locks[asset_name] = asyncio.Lock()
        return lock

    async def _apply_asset_row(self, asset, assets):
        """Apply one of this gateway's asset rows if it is newer than the row
        in force: configure the asset, or stop it when the row deletes it.

        Rows of one asset are applied one at a time, in the order they win the
        lock, and an older row that waited behind a newer one is dropped, so a
        slow discovery for a stale row can never override a later edit.
        A deleting row also soft-deletes the asset's catalog rows and its
        status. An applied row requests a refresh of the transforms over
        ``assets``. Returns whether the row was applied; never raises."""
        asset_name = asset.get("asset_name")
        if not asset_name:
            return False
        async with self._asset_lock(asset_name):
            if not self._is_newer("assets", asset_name, asset):
                return False
            if asset.get("deleted"):
                assets[:] = [a for a in assets if a["asset_name"] != asset_name]
                self.stop_asset_task(asset_name)
                self._clear_datapoint_values(asset_name)
                try:
                    await self.clear_asset_status(asset)
                except Exception as e:
                    print(f"Failed to clear status of {asset_name}: {e}")
                await self._delete_catalog(asset_name)
                self.request_refresh("assets")
                return True
            print(f"Configuring asset: {asset_name}")
            try:
                await self.configure_asset(asset, assets)
            except Exception as e:
                print(f"Error configuring asset {asset_name}: {e}")
                traceback.print_exc()
                await self.report_exception(
                    e,
                    asset_name=asset_name,
                    fallback_user_message=f"'{asset_name}' has an invalid configuration.",
                )
            self.request_refresh("assets")
            return True

    async def _delete_catalog(self, asset_name):
        """Soft-delete a deleted asset's ``datapoints`` rows. The core is the
        catalog's only writer; with ``store_data`` False nothing is written,
        as for every catalog write. Never raises."""
        if self.store is None:
            return
        try:
            await self.store.prune(asset_name, [])
        except Exception as e:
            print(f"Failed to delete the datapoints of {asset_name}: {e}")

    def _handle_asset_update(self, assets):
        async def handler(*args, **kwargs):
            asset = args[0] if args else {}
            if await self._skip_incomplete_asset(asset):
                return
            if not self._is_own(asset):
                return
            await self._apply_asset_row(asset, assets)

        return handler

    async def load_datapoints(self):
        """Read this gateway's current ``datapoints`` catalog rows.

        The core is the catalog's only writer, so the table is read once at
        startup and not followed: the store keeps the returned list current
        as it writes. The returned list object is shared with the store."""
        datapoints: list[dict] = []
        rows = await self.ironflock.getHistory(
            "datapoints",
            {
                "limit": 3000,
                "filterAnd": [
                    LATEST,
                    {"column": "gateway_id", "operator": "=", "value": self.device_key},
                ],
            },
        )
        for row in rows or []:
            self._apply_datapoint_row(datapoints, row)

        if datapoints:
            print(f"Loaded {len(datapoints)} datapoint(s) from datapoints table")
        else:
            print("No datapoints found in the datapoints table.")

        return datapoints

    def _apply_datapoint_row(self, datapoints, row):
        """Put one of this gateway's catalog rows into ``datapoints`` if it is
        newer than the row in force; a deleted row (``deleted`` true) removes
        the datapoint, a NULL flag keeps it. Returns whether it was applied."""
        if not self._is_own(row):
            return False
        asset_name, datapoint_id = row.get("asset_name"), row.get("datapoint_id")
        if not self._is_newer("datapoints", (asset_name, datapoint_id), row):
            return False
        datapoints[:] = [
            dp
            for dp in datapoints
            if dp.get("datapoint_id") != datapoint_id or dp.get("asset_name") != asset_name
        ]
        if not row.get("deleted"):
            datapoints.append(row)
        return True

    # ----------------------------------------------------- datapoint settings

    async def load_datapoint_settings(self):
        """Follow ``datapoint_settings``: subscribe, then read this gateway's
        current rows into ``self.datapoint_settings``.

        The board and the AI assistant write these rows (the user's
        per-datapoint switches); each goes through
        :meth:`_apply_datapoint_settings_row`, which applies only rows newer
        than the one in force."""
        await self.ironflock.subscribe_to_table(
            "datapoint_settings", self._handle_datapoint_settings_update()
        )
        rows = await self.ironflock.getHistory(
            "datapoint_settings",
            {
                "limit": 3000,
                "filterAnd": [
                    LATEST,
                    {"column": "gateway_id", "operator": "=", "value": self.device_key},
                ],
            },
        )
        for row in rows or []:
            self._apply_datapoint_settings_row(row)
        print(f"Loaded {len(self.datapoint_settings)} datapoint setting(s)")
        return self.datapoint_settings

    def _apply_datapoint_settings_row(self, row):
        """Take one of this gateway's ``datapoint_settings`` rows if it is
        newer than the row in force for its datapoint. The next reading of the
        datapoint is a new change-detection baseline. Requests a refresh of
        the transforms over the table. Returns whether it was applied."""
        if not self._is_own(row):
            return False
        key = (row.get("asset_name"), row.get("datapoint_id"))
        if not key[0] or not key[1]:
            return False
        if not self._is_newer("datapoint_settings", key, row):
            return False
        self.datapoint_settings[key] = row
        self._last_values.pop(key, None)
        self.request_refresh("datapoint_settings")
        return True

    def _handle_datapoint_settings_update(self):
        def handler(*args, **kwargs):
            row = args[0] if args else {}
            if self._apply_datapoint_settings_row(row):
                print("Datapoint settings updated:", row)

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
        row. Otherwise the row is appended to the bounded buffer and a flush
        is started in the background (``_schedule_flush``); a network failure
        leaves the row (and any backlog) buffered for the next flush instead
        of raising, so collection keeps running during a cloud outage.

        ``_send`` never waits for the write: the SDK (ironflock >= 1.9) holds
        a table write for up to its ``reconnect_window`` (60 s) while the link
        is down, and waiting for that here would stall the asset's collection
        loop for the whole window on every reading.

        The row is made storable first (see ``collector_core.storable``). This
        is the one door into the shared buffer, and a row the platform cannot
        encode would otherwise sit at the buffer's head and block every asset's
        writes; a repaired row is written, and the repair is reported.
        """
        if not self.store_data:
            return
        payload, problems = storable(payload)
        if problems:
            await self._report_repair(table, payload, problems)
        if self._buffer.maxlen is not None and len(self._buffer) >= self._buffer.maxlen:
            self._dropped_rows += 1  # deque drops the oldest row silently
        self._buffer.append((table, payload))
        self._schedule_flush()

    def _schedule_flush(self):
        """Start a background drain, unless one is already waiting its turn.

        A drain takes the whole backlog when it gets the lock, so at most one
        drain runs and one waits behind it for the rows appended meanwhile.
        The task is held in ``_draining`` until it ends: nothing else
        references it, and the loop keeps tasks only weakly. Being a task of
        its own, it is not cancelled with the asset task that sent the row.
        """
        if self._flush_queued:
            return
        self._flush_queued = True
        task = asyncio.ensure_future(self._background_drain())
        self._draining.add(task)
        task.add_done_callback(self._draining.discard)

    async def _background_drain(self):
        try:
            await self._drain()
        except Exception as e:
            print(f"flush error: {e}")

    async def _flush(self):
        """Drain the buffer oldest-first while the link is up (``_drain``).

        The drain runs shielded from whoever awaits it (``_flush_loop``; the
        writes ``_send`` triggers run as tasks of their own, see
        ``_schedule_flush``). Cancelled mid-write, an unshielded drain would
        drop every row it had taken out of the shared buffer, and putting
        them back instead could write a chunk twice, since the write may
        already have landed. Shielded, the drain finishes: each row is
        written once, or re-queued by the failure path as usual. The caller
        still sees its cancellation.
        """
        drain = asyncio.ensure_future(self._drain())
        # Held until it ends: once its caller is gone, nothing else references
        # it, and the loop keeps tasks only weakly.
        self._draining.add(drain)
        drain.add_done_callback(self._draining.discard)
        await asyncio.shield(drain)

    async def _drain(self):
        """The flush itself.

        Rows are sent with the SDK's bulk ``append_rows_to_table``: the backlog
        is snapshotted (bounded to the current length so concurrently appended
        rows can't livelock the drain) and consecutive rows for the same table
        are written in one call, capped at ``BULK_CHUNK_MAX`` rows so a long
        backlog is split into several bounded inserts rather than one oversized
        one. A reconnect after a long outage then catches up in a handful of
        bulk requests instead of thousands of single-row ones. Stops at the
        first failure, re-queuing the un-sent rows oldest-first so the backlog
        (and ordering) is preserved. Every chunk written requests a refresh of
        the transforms over its table (``request_refresh``, which never waits).
        """
        async with self._flush_lock:
            # Rows appended from here on need a drain of their own.
            self._flush_queued = False
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
                    print(f"buffered {len(self._buffer)} row(s); {outage_cause(e)}: {e}")
                    return
                self.request_refresh(table)
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

    # ------------------------------------------------------- transform refresh

    def request_refresh(self, table):
        """Refresh the app's transforms that read ``table``, in the background.

        ``refresh_transforms`` maps a table to its transforms in bottom-up
        order; they are refreshed one after the other with the SDK's
        ``refresh_transform`` (ironflock >= 1.11.0), so a board bound to them
        shows the change at once instead of at the transform's schedule tick.
        The core calls this after it writes a table (a drained chunk, a
        catalog write, the registration, the keep-alive) and after it applies
        a row of its own gateway from a followed table (assets,
        gateway_settings, datapoint_settings). An adapter calls it after
        writing a table of its own.

        Returns at once; call it from the event loop. Requests coalesce: per
        table one refresh of its list runs and one more waits for the requests
        made meanwhile; per transform one refresh is in flight and one waits,
        and two refreshes of a transform are at least REFRESH_MIN_INTERVAL
        seconds apart. A refresh refused with URI_REFRESH_IN_PROGRESS is
        retried once after REFRESH_RETRY_DELAY seconds; every other failure is
        printed, never raised. An SDK without refresh_transform refreshes
        nothing (printed once).
        """
        names = self.refresh_transforms.get(table)
        if not names or self.ironflock is None:
            return
        if getattr(self.ironflock, "refresh_transform", None) is None:
            if not self._refresh_unsupported_reported:
                self._refresh_unsupported_reported = True
                print(
                    "refresh_transforms is set, but this ironflock SDK has no "
                    "refresh_transform (needs >= 1.11.0); transforms follow their schedule"
                )
            return
        if table in self._table_refresh_running:
            self._table_refresh_pending.add(table)
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            print(f"refresh of the transforms over {table} skipped: no running event loop")
            return
        self._table_refresh_running.add(table)
        self._hold(asyncio.ensure_future(self._refresh_table(table)))

    def _hold(self, task):
        self._refreshing.add(task)
        task.add_done_callback(self._refreshing.discard)

    async def _refresh_table(self, table):
        """Refresh a table's transforms in order, again while requests for
        the table arrived during the pass."""
        try:
            while True:
                self._table_refresh_pending.discard(table)
                for name in self.refresh_transforms.get(table, ()):
                    await self._refresh_transform(name)
                if table not in self._table_refresh_pending:
                    break
        finally:
            self._table_refresh_running.discard(table)
            self._table_refresh_pending.discard(table)

    async def _refresh_transform(self, name):
        """Return once a refresh of ``name`` that started after this call has
        ended (successful or not)."""
        state = self._transform_refresh.get(name)
        if state is None:
            state = self._transform_refresh[name] = _TransformRefresh()
        state.requested += 1
        target = state.requested
        if state.worker is None:
            state.worker = asyncio.ensure_future(self._transform_worker(name, state))
            self._hold(state.worker)
        async with state.changed:
            await state.changed.wait_for(lambda: state.completed >= target)

    async def _transform_worker(self, name, state):
        try:
            while state.completed < state.requested:
                if state.last is not None:
                    gap = state.last + REFRESH_MIN_INTERVAL - time.monotonic()
                    if gap > 0:
                        await asyncio.sleep(gap)
                covered = state.requested
                await self._call_refresh(name)
                state.last = time.monotonic()
                async with state.changed:
                    state.completed = covered
                    state.changed.notify_all()
        finally:
            state.worker = None
            if state.completed < state.requested:  # ended early: release the waiters
                async with state.changed:
                    state.completed = state.requested
                    state.changed.notify_all()

    async def _call_refresh(self, name):
        """One ``refresh_transform(name)``; never raises."""
        refresh = getattr(self.ironflock, "refresh_transform", None)
        if refresh is None:
            return
        for attempt in (1, 2):
            try:
                await refresh(name)
                return
            except Exception as e:
                if attempt == 1 and REFRESH_IN_PROGRESS in str(e):
                    await asyncio.sleep(REFRESH_RETRY_DELAY)
                    continue
                print(f"refresh of transform {name} failed (its schedule still runs): {e}")
                return

    async def _report_repair(self, table, payload, problems):
        """Tell the operator once an hour per asset, table and kind of repair."""
        asset_name = payload.get("asset_name") if isinstance(payload, dict) else None
        key = (asset_name, table, tuple(problems))
        now = time.monotonic()
        last = self._repair_reported.get(key)
        if last is not None and now - last < REPAIR_REPORT_INTERVAL:
            return
        self._repair_reported[key] = now
        what = ", ".join(problems)
        await self.report_error(
            f"repaired a {table} row the platform could not store ({what})",
            level="warn",
            asset_name=asset_name,
            user_message=(
                f"'{asset_name}' sent a value the platform cannot store ({what}). "
                "It was stored in a repaired form."
                if asset_name
                else f"A value the platform cannot store ({what}) was stored in a repaired form."
            ),
        )

    async def reassert_stale_rows(self):
        """Rewrite the latest datapoints and assetstatus rows older than
        ``reassert_after``, so table retention never drops a live entity.

        Writes full rows, not identity-only ones: the content comes from the
        in-memory catalog (read at startup and kept current by the store) and
        the last status write, which keeps the rewrite correct on a platform
        without carry-over merges too. Both tables are the gateway's alone,
        so a rewrite reverts nobody's edit. Requests a refresh of the
        transforms over the tables written. Returns the number of rows
        written; never raises — a failure is retried on the next pass.
        """
        if not self.store_data or self.ironflock is None:
            return 0
        written = 0
        cutoff = time.time() - self.reassert_after
        stamp = _now()

        stale = []
        for row in list(self.datapoints):
            when = _epoch_seconds(row.get("tsp"))
            if when is None or when >= cutoff:
                continue
            payload = {k: v for k, v in row.items() if k not in PLATFORM_COLUMNS}
            payload["tsp"] = stamp
            stale.append((row, payload))
        for i in range(0, len(stale), BULK_CHUNK_MAX):
            chunk = stale[i : i + BULK_CHUNK_MAX]
            rows = [storable(payload)[0] for _, payload in chunk]
            try:
                await self.ironflock.append_rows_to_table("datapoints", rows)
            except Exception as e:
                print(f"keep-alive of {len(rows)} datapoint row(s) failed: {e}")
                break
            for row, _ in chunk:
                row["tsp"] = stamp
            written += len(rows)
        if written:
            self.request_refresh("datapoints")

        monotonic_cutoff = time.monotonic() - self.reassert_after
        for asset_name, (when, status, detail) in list(self._status_written.items()):
            if when >= monotonic_cutoff or self._asset_status.get(asset_name) != status:
                continue
            self._status_written[asset_name] = (time.monotonic(), status, detail)
            await self._send(
                "assetstatus",
                {
                    "tsp": stamp,
                    "asset_name": asset_name,
                    "gateway_id": self.device_key,
                    "status": status,
                    "detail": detail,
                    "deleted": False,
                },
            )
            written += 1

        if written:
            print(f"keep-alive: rewrote {written} row(s) older than {self.reassert_after}s")
        return written

    async def _reassert_loop(self):
        """Run the keep-alive every REASSERT_INTERVAL; see REASSERT_AFTER."""
        while True:
            await asyncio.sleep(REASSERT_INTERVAL)
            try:
                await self.reassert_stale_rows()
            except Exception as e:
                print(f"keep-alive loop error: {e}")

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
        """Map ``datapoint_id`` -> catalog row for one asset."""
        return {
            row.get("datapoint_id"): row
            for row in self.datapoints
            if row.get("asset_name") == asset_name
        }

    def _with_demo_config(self, asset_name, datapoints):
        """Datapoint dicts with the effective demo tuning merged in.

        Per column of ``DEMO_DATAPOINT_COLUMNS`` the user's
        ``datapoint_settings`` value wins when it is not null, then the
        catalog row's value (a default the adapter derived from its spec),
        then whatever the adapter's resolved dict carries.

        Returns the originals untouched when there is nothing to merge, so the
        common (unconfigured) case allocates nothing per cycle.
        """
        catalog = self._datapoint_catalog(asset_name)
        merged = []
        for datapoint in datapoints:
            datapoint_id = datapoint.get("id")
            settings = self.datapoint_settings.get((asset_name, datapoint_id)) or {}
            row = catalog.get(datapoint_id) or {}
            demo = {}
            for column in DEMO_DATAPOINT_COLUMNS:
                if settings.get(column) is not None:
                    demo[column] = settings[column]
                elif row.get(column) is not None:
                    demo[column] = row[column]
            merged.append({**datapoint, **demo} if demo else datapoint)
        return merged

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

        Each datapoint is gated by its ``datapoint_settings`` row: an explicit
        ``enabled == False`` drops it (per-datapoint pause), and with
        ``change_detection`` set it is written only when its value differs from
        the last published value. Without a row, or with both off, the full
        snapshot is written; when gating empties the row, nothing is sent.
        """
        asset_name = asset["asset_name"]
        by_id = {dp["id"]: dp for dp in datapoints if dp.get("id")}
        settings = self.datapoint_settings
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
            meta = settings.get((asset_name, datapoint_id))
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
                        demo = self._with_demo_config(asset_name, datapoints)
                        if await self._publish_batch(
                            asset, datapoints, session.demo_values(demo)
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
        _tolerate_unencodable_output()
        try:
            # Each followed table is subscribed before it is read, so a row
            # saved during startup is not lost; the read then applies only
            # what is newer.
            await self.load_gateway_settings()
            await self._apply_adapter_settings(self.settings)

            self.datapoints = await self.load_datapoints()
            self.store = DatapointStore(
                self.ironflock,
                self.device_key,
                self.datapoints,
                store_data=self.store_data,
                on_write=self.request_refresh,
            )
            await self.load_datapoint_settings()
            await self.load_asset_configs()

            # Drains the offline buffer on reconnect even when no new data flows.
            asyncio.create_task(self._flush_loop())
            # Rewrites long-unchanged catalog and status rows before table
            # retention can drop them (see REASSERT_AFTER).
            asyncio.create_task(self._reassert_loop())

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
