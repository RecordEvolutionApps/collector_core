"""Protocol-agnostic seam between the generic collector core and a concrete
protocol implementation.

A protocol provides:
  - a ``ProtocolAdapter`` that knows how to resolve the list of datapoints to
    collect for an asset (optionally discovering + persisting metadata), how
    to normalise raw values into measurement entries, and optionally how to
    run protocol-level background work (network discovery) and consume
    gateway-level settings, and
  - a ``ProtocolSession`` (one per asset task) that owns any connection and
    produces raw value batches — either by being polled (``read_values``) or
    by pushing at its own cadence (a ``stream`` override).

The core (``collector_core.collector``) never imports protocol libraries
(requests, pymodbus, bacpypes3, asyncua, ...). Everything protocol-specific
lives behind these abstractions.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

# Columns the platform stamps onto rows it returns; never echo them back.
PLATFORM_COLUMNS = ("tsp", "latest_flag", "authid", "device_key")

# Datapoints columns owned by the user via the board (per-datapoint pause and
# change detection), not by protocol discovery. The store preserves them across
# metadata re-discovery so a device-driven catalog rewrite never clobbers a
# user's choice; the collector reads them to gate measurement writes.
#  - enabled:          pause switch, null-safe (only an explicit false pauses;
#                      missing/null means the datapoint is collected).
#  - change_detection: when true, the datapoint is written only when its value
#                      differs from the last written value (missing/null/false
#                      stores every reading).
USER_DATAPOINT_COLUMNS = ("enabled", "change_detection")


def _now():
    return datetime.now(timezone.utc).isoformat()


async def emit_error(ironflock, message, level="error", user_message=None):
    """Best-effort write to the platform ``error-logs`` table, surfaced live on
    the board by the toast widget.

    Never raises: error reporting is a UX nicety and a failure here (missing
    handle, SDK without ``report_error``, link down) must never break
    collection. ``message`` is the technical text (stringified — pass a concise
    string rather than an exception object, since ``report_error`` records an
    exception's full traceback). ``user_message`` is the operator-facing display
    line the toast shows; when None the SDK defaults it to ``message``. ``level``
    is one of ``error``/``warn``/``info``/``debug``.
    """
    if ironflock is None:
        return
    try:
        await ironflock.report_error(str(message), level=level, user_message=user_message)
    except Exception as e:
        print(f"report_error failed: {e}")


class DatapointsChanged(Exception):
    """Raised by a session (from ``read_values`` or a ``stream`` override)
    when the set of available datapoints has changed at the source (IO-Link
    sensor hot-plug, OPC UA address-space change).

    The core reacts by discarding any cached ``asset["datapoint_list"]``,
    closing the session and restarting the stream immediately (no backoff);
    the next datapoint resolution re-runs ``prepare_datapoints`` so the
    datapoints catalog is re-discovered and re-synced. Sessions must raise
    this only on an actually observed change, never speculatively, or the
    task will spin on re-discovery.
    """


class DatapointStore:
    """Persist datapoint metadata to the ``datapoints`` table.

    Wraps the in-memory datapoints list (kept in sync by the core's table
    subscription) and the IronFlock handle so an adapter can store/prune
    metadata without knowing about IronFlock or the device key.

    The store is column-agnostic: each protocol's ``datapoints`` schema
    differs beyond the core contract columns (MTConnect has
    category/native_units, Modbus has register offset/count/type, BACnet has
    object type/instance), so adapters pass full row payloads and the store
    only owns the generic mechanics — identity on ``datapoint_id`` + asset,
    change detection, appending, and soft-deleting.

    With ``store_data`` False the in-memory state is still maintained but
    nothing is written to the platform (forward-only mode). The attribute is
    public so the core can toggle it live with the gateway settings.
    """

    def __init__(self, ironflock, device_key, datapoints, store_data=True):
        self._ironflock = ironflock
        self._device_key = device_key
        self._datapoints = datapoints
        self.store_data = store_data

    @property
    def device_key(self):
        return self._device_key

    async def report_error(self, message, level="error", user_message=None):
        """Surface an error to the board's toast widget (best-effort).

        Lets an adapter report a swallowed, user-facing problem (e.g. a bad
        uploaded device description) from ``prepare_datapoints``, where it holds
        the store but not the collector. ``message`` is the technical text;
        ``user_message`` is the friendly line shown on the toast. Never raises."""
        await emit_error(self._ironflock, message, level, user_message)

    def rows_for_asset(self, asset_name):
        """Return the in-memory datapoint rows belonging to an asset."""
        return [row for row in self._datapoints if row.get("asset_name") == asset_name]

    @staticmethod
    def _norm(value):
        # Numeric columns may come back from the table as float or string;
        # compare value-wise so an unchanged row is not re-appended forever.
        if isinstance(value, bool) or value is None:
            return value
        try:
            return float(value)
        except (TypeError, ValueError):
            return str(value)

    def _drop_row(self, asset_name, datapoint_id):
        self._datapoints[:] = [
            row
            for row in self._datapoints
            if row.get("datapoint_id") != datapoint_id
            or row.get("asset_name") != asset_name
        ]

    async def upsert(self, asset_name, payloads):
        """Append every payload row that is new or changed for this asset.

        Each payload is a full ``datapoints`` row in the protocol's own schema
        (all columns except ``tsp``) and must contain ``datapoint_id``. Rows
        already present with identical values are skipped, so calling this
        repeatedly is cheap. User-owned columns (``USER_DATAPOINT_COLUMNS``)
        are carried over from the existing row, so re-discovering metadata never
        clobbers a user's pause / change-detection choice.
        """
        stored = 0
        for payload in payloads:
            current = next(
                (
                    row
                    for row in self._datapoints
                    if row.get("asset_name") == asset_name
                    and row.get("datapoint_id") == payload["datapoint_id"]
                ),
                None,
            )
            if current is not None and all(
                self._norm(current.get(k)) == self._norm(v) for k, v in payload.items()
            ):
                continue

            record = {**payload, "tsp": _now()}
            # Preserve user-set columns the protocol does not manage across a
            # metadata-driven rewrite (the payload never carries them).
            if current is not None:
                for column in USER_DATAPOINT_COLUMNS:
                    if column not in record and current.get(column) is not None:
                        record[column] = current[column]
            print("storing datapoint", record)
            if self.store_data:
                await self._ironflock.append_to_table("datapoints", record)
            self._drop_row(asset_name, payload["datapoint_id"])
            self._datapoints.append(record)
            stored += 1
        if stored:
            print(f"Stored {stored} datapoint(s) for {asset_name}")

    async def prune(self, asset_name, keep_ids):
        """Soft-delete datapoints for this asset no longer in ``keep_ids``."""
        keep = set(keep_ids)
        stale = [
            row
            for row in self._datapoints
            if row.get("asset_name") == asset_name
            and row.get("datapoint_id") not in keep
        ]

        for row in stale:
            payload = {k: v for k, v in row.items() if k not in PLATFORM_COLUMNS}
            payload["deleted"] = True
            payload["tsp"] = _now()
            print("deleting stale datapoint", payload)
            if self.store_data:
                await self._ironflock.append_to_table("datapoints", payload)
            self._drop_row(asset_name, row.get("datapoint_id"))

        if stale:
            print(f"Pruned {len(stale)} stale datapoint(s) for {asset_name}")


class ProtocolSession(ABC):
    """Per-asset reader, created fresh for each collection task. Owns any
    persistent connection (stateless protocols just no-op ``close``).

    Constructors must not perform I/O: sessions are also created in demo mode
    where no device exists. Connect lazily inside ``read_values``/``stream``.
    """

    async def stream(self, datapoints_provider, interval):
        """Yield ``(datapoints, {datapoint_id: raw_value})`` batches forever.

        ``datapoints_provider`` is an async callable returning the current
        datapoint list (the core wires it to ``prepare_datapoints``/the cached
        list, honouring ``resolve_each_cycle``). ``interval`` is the asset's
        collect_interval in seconds: the poll period for pull protocols, the
        publish/batch window for push protocols.

        The default implementation is the classic poll loop (resolve, read,
        yield, sleep), so pull protocols only implement ``read_values``. Push
        protocols (OPC UA subscriptions, cyclic feeds) override this: connect,
        subscribe, accumulate server-pushed changes, and yield one batch per
        ``interval`` window (merge by datapoint id — accumulation must stay
        bounded).

        Contract:
        - Each yielded ``datapoints`` list is the one used to interpret that
          batch; raw keys must be a subset of its datapoint ids. Absent keys
          mean "no reading this window" and produce no measurement entry.
        - Raise ``DatapointsChanged`` when the available datapoints change;
          the core re-discovers and restarts the stream.
        - Any other exception ends the stream: the core closes the session,
          marks the asset offline, waits ``interval`` seconds, creates a fresh
          session and restarts. Returning normally is treated the same way
          (restart after backoff).
        - The core closes the generator with ``aclose()`` on its way out;
          overrides should keep ``finally`` cleanup at the yield point brief
          and put heavy teardown in ``close()``, which the core always awaits.
        - Not called in demo mode (the core paces ``demo_values`` itself).
        """
        while True:
            datapoints = await datapoints_provider()
            yield datapoints, await self.read_values(datapoints)
            await asyncio.sleep(interval)

    async def read_values(self, datapoints) -> dict:
        """Read current raw values for all datapoints in one poll cycle.

        Returns ``{datapoint_id: raw_value}`` with a key for every readable
        datapoint. Encapsulates connect/reconnect; raises on failure (the
        core handles backoff/reconnect and the offline status). May raise
        ``DatapointsChanged``. Required only when the default ``stream`` is
        used; push protocols that override ``stream`` need not implement it.
        Not called in demo mode.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement read_values() or override stream()"
        )

    @abstractmethod
    def demo_values(self, datapoints) -> dict:
        """Return synthetic ``{datapoint_id: raw_value}`` for demo mode, in
        the same raw shape ``read_values``/``stream`` produce so
        ``format_value`` and ``make_entry`` work identically."""

    async def close(self) -> None:
        """Release any persistent resources. No-op for stateless protocols.
        Always awaited by the core after the stream ends, errors, or the task
        is cancelled. Must be idempotent."""
        return None


class ProtocolAdapter(ABC):
    """Protocol behaviour the core composes with. One instance per collector."""

    # False -> resolve the datapoint list once at (re)configure time and cache
    #          it on ``asset["datapoint_list"]`` (MTConnect probe discovery).
    # True  -> re-resolve every poll cycle so live edits to the datapoint spec
    #          take effect without restarting the task (Modbus register map).
    resolve_each_cycle: bool = False

    @abstractmethod
    async def prepare_datapoints(self, asset, store: DatapointStore) -> list:
        """Return the datapoints to collect for ``asset``.

        Each datapoint is a dict with at least ``id``, ``name`` and ``units``
        (plus any protocol-private keys the session needs). May discover and
        persist metadata via ``store`` (MTConnect probe, BACnet object-list)
        or just parse the user-authored ``datapoint_spec`` (Modbus).
        """

    @abstractmethod
    def create_session(self, asset) -> ProtocolSession:
        """Create a per-asset session for the collection task (no I/O)."""

    @abstractmethod
    def format_value(self, asset, datapoint, raw_value) -> Any:
        """Normalise a raw protocol value to the scalar stored under
        ``payload['data'][datapoint_id]['value']``."""

    def make_entry(self, asset, datapoint, raw_value) -> dict:
        """Build the dict stored under ``payload['data'][datapoint_id]`` for
        one reading. The default carries name, units and the formatted value;
        protocols may override it to add per-reading fields (quality flags,
        source timestamps — e.g. BACnet ``status_flags``, OPC UA
        ``source_tsp``/``status``) or to refresh metadata from the raw value.
        """
        return {
            "name": datapoint["name"],
            "units": datapoint["units"],
            "value": self.format_value(asset, datapoint, raw_value),
        }

    async def apply_settings(self, gateway_row: dict) -> None:
        """Consume gateway-level settings (columns of this gateway's row in
        the ``gateways`` table). Called once before assets are configured and
        again whenever the row changes. Adapters read their own settings
        columns here and fall back to built-in defaults for missing ones; a
        setting that cannot be applied live should be logged as requiring a
        restart. The default does nothing.
        """
        return None

    async def start_background(self, collector) -> None:
        """Called once by the core after the initial configuration is loaded
        and the table subscriptions are active. Protocols may spawn
        long-running background work here (e.g. periodic network discovery
        that appends new asset configurations). The default does nothing.
        """
        return None
