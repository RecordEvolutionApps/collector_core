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
import sys
import traceback
from collections import deque
from contextlib import aclosing
from datetime import datetime, timezone

from collector_core.adapter import PLATFORM_COLUMNS, DatapointStore, DatapointsChanged

# Max data rows buffered locally while the cloud link is down, when the
# gateway row sets no buffer_size. Oldest rows are dropped on overflow.
DEFAULT_BUFFER_SIZE = 10000

# How often the background flusher retries draining the buffer (seconds).
FLUSH_INTERVAL = 15


def _now():
    return datetime.now(timezone.utc).isoformat()


class Collector:
    def __init__(self, device_name, device_key, adapter, store_data=True):
        self.device_name = device_name
        self.device_key = device_key
        self.adapter = adapter
        # Default when the gateway row has no store_data column. With
        # store_data False, configuration is still read live from the
        # platform but collected data is not written to it.
        self._default_store_data = store_data
        self.store_data = store_data
        self.ironflock = None
        self.store = None
        self.gateway = {}
        self.asset_tasks: dict[str, asyncio.Task] = {}
        self._asset_status: dict[str, str] = {}
        # Bounded FIFO of (table, payload) writes that could not be sent while
        # the cloud link was down; drained in order on reconnect.
        self._buffer = deque(maxlen=DEFAULT_BUFFER_SIZE)
        self._flush_lock = asyncio.Lock()

    def set_ironflock(self, ironflock):
        """Provide the IronFlock handle used for all table I/O. Must be called
        before ``run`` (which IronFlock invokes as its ``mainFunc``)."""
        self.ironflock = ironflock

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

    async def register_gateway(self):
        """Write the registry row, echoing existing columns so user-edited
        gateway settings survive the startup append (the appended row becomes
        the latest one)."""
        payload = {
            k: v for k, v in self.gateway.items() if k not in PLATFORM_COLUMNS
        }
        payload["gateway_name"] = self.device_name
        payload["deleted"] = False
        payload["info"] = {
            "app_started": _now(),
            "python": sys.version.split()[0],
        }
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

        return handler

    # ------------------------------------------------------------------ status

    async def set_asset_status(self, asset, status, detail=""):
        """Record an asset state transition (online | offline | paused).

        Appends to ``assetstatus`` only when the status actually changes, so
        the table is a transition log (latest row = current state)."""
        asset_name = asset["asset_name"]
        if self._asset_status.get(asset_name) == status:
            return
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
        print(f"Loaded {len(result)} asset(s) from assets table")

        configured_assets = []
        for asset in result:
            asset_name = (asset.get("asset_name") or "").strip()
            gateway_id = asset.get("gateway_id")

            # Clean up invalid configs: empty asset name or no gateway assignment.
            if not asset_name or gateway_id is None:
                try:
                    await self.soft_delete_asset(asset)
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
                try:
                    await self.clear_asset_status(asset)
                except Exception as e:
                    print(f"Failed to clear status of {asset['asset_name']}: {e}")
            else:
                try:
                    await self.configure_asset(asset, assets)
                except Exception as e:
                    print(
                        f"Error configuring asset {asset.get('asset_name', 'unknown')}: {e}"
                    )
                    traceback.print_exc()

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
        await self.ironflock.subscribe_to_table(
            "datapoints", self._handle_datapoint_update(datapoints)
        )

        if not datapoints:
            print("No datapoints found in the datapoints table.")
            return []
        print(f"Loaded {len(datapoints)} datapoint(s) from datapoints table")

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
        self._buffer.append((table, payload))
        await self._flush()

    async def _flush(self):
        """Drain the buffer oldest-first while the link is up.

        Bounded to the current length so concurrently appended rows can't
        livelock the drain. Stops at the first failure, keeping the backlog.
        """
        async with self._flush_lock:
            for _ in range(len(self._buffer)):
                if not self._buffer:
                    break
                item = self._buffer.popleft()
                try:
                    await self.ironflock.append_to_table(*item)
                except Exception as e:
                    # Still down: put the row back and keep the backlog.
                    self._buffer.appendleft(item)
                    print(f"buffered {len(self._buffer)} row(s); link down: {e}")
                    return

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

    async def _publish_batch(self, asset, datapoints, raw):
        """Assemble and write one measurements row from a yielded batch.

        Iterates the raw map's keys (not the datapoint list) so push
        protocols can deliver partial batches without writing None entries
        for absent datapoints. An empty raw map writes nothing. Returns
        whether anything was published.
        """
        by_id = {dp["id"]: dp for dp in datapoints if dp.get("id")}
        payload = {
            "tsp": _now(),
            "asset_name": asset["asset_name"],
            "gateway_id": self.device_key,
            "data": {},
        }
        for datapoint_id, raw_value in (raw or {}).items():
            datapoint = by_id.get(datapoint_id)
            if datapoint is None:
                continue
            payload["data"][datapoint_id] = self.adapter.make_entry(
                asset, datapoint, raw_value
            )

        if not payload["data"]:
            return False
        print(payload)
        await self._send("measurements", payload)
        return True

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
                            await self.set_asset_status(asset, "online")
                        await asyncio.sleep(interval)
                else:
                    stream = session.stream(provider, interval)
                    async with aclosing(stream):
                        async for datapoints, raw in stream:
                            if await self._publish_batch(asset, datapoints, raw):
                                await self.set_asset_status(asset, "online")
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
                    await self.set_asset_status(asset, "offline", str(e))
                except Exception as status_error:
                    print(f"Failed to record offline status: {status_error}")
            finally:
                await session.close()
            await asyncio.sleep(backoff)

    # ------------------------------------------------------------------- run

    async def run(self):
        await self.load_gateway()
        try:
            await self.adapter.apply_settings(self.gateway)
        except Exception as e:
            print(f"apply_settings failed, using adapter defaults: {e}")

        datapoints = await self.load_datapoints()
        self.store = DatapointStore(
            self.ironflock, self.device_key, datapoints, store_data=self.store_data
        )
        await self.load_asset_configs()

        # Drains the offline buffer on reconnect even when no new data flows.
        asyncio.create_task(self._flush_loop())

        # Table subscriptions are active now, so asset rows appended by
        # protocol background work (e.g. network discovery) are picked up.
        await self.adapter.start_background(self)

        await self.ironflock.subscribe_to_table(
            "gateways", self._handle_gateway_update()
        )
        await self.register_gateway()

        while True:
            await asyncio.sleep(3600)
