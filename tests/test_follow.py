"""The board-written tables are followed the way the platform expects.

``gateway_settings``, ``assets`` and ``datapoint_settings`` are subscribed
first, then read with the latest marker; a row applies only when it is this
gateway's and its tsp is newer than the one in force, and a NULL ``deleted``
is a live row. The ``datapoints`` catalog is the gateway's own: read once at
startup, never followed. The core never writes ``assets``.
"""

import asyncio
from datetime import datetime, timezone

from collector_core.adapter import DatapointStore, ProtocolAdapter
from collector_core.collector import DEFAULT_BUFFER_SIZE, Collector


class _Adapter(ProtocolAdapter):
    def __init__(self):
        self.settings = []

    async def prepare_datapoints(self, asset, store):
        return []

    def create_session(self, asset):
        return None

    def format_value(self, asset, dp, raw):
        return raw

    async def apply_settings(self, settings_row):
        self.settings.append(settings_row)


class FakeIF:
    """Records subscriptions, reads and writes; ``during_read`` runs inside a
    read, like a row the platform stores between the subscription and the
    read."""

    def __init__(self, rows=None, during_read=None):
        self.rows = rows or {}
        self.during_read = during_read or {}
        self.log = []
        self.handlers = {}
        self.queries = {}
        self.written = []
        self.reported = []

    async def subscribe_to_table(self, table, handler):
        self.log.append(("subscribe", table))
        self.handlers[table] = handler

    async def getHistory(self, table, params):
        self.log.append(("read", table))
        self.queries[table] = params
        hook = self.during_read.get(table)
        if hook:
            await hook(self)
        return list(self.rows.get(table, []))

    async def deliver(self, table, row):
        result = self.handlers[table](row)
        if asyncio.iscoroutine(result):
            await result

    async def append_to_table(self, table, row):
        self.written.append((table, row))

    async def append_rows_to_table(self, table, rows):
        self.written.extend((table, row) for row in rows)

    async def report_error(self, message, level="error", user_message=None, **kwargs):
        self.reported.append((message, level, user_message))


def _collector(fake, **kw):
    c = Collector(device_name="gw", device_key=1, adapter=_Adapter(), **kw)
    c.set_ironflock(fake)
    return c


def _ms(second):
    """A tsp as a history read returns it: epoch milliseconds."""
    return datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc).timestamp() * 1000


def _iso(second):
    """The same moment as a table event carries it: ISO text."""
    return datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc).isoformat()


def _dp(dp_id, tsp, **extra):
    return {"tsp": tsp, "gateway_id": 1, "asset_name": "A", "datapoint_id": dp_id, **extra}


def _setting(dp_id, tsp, **extra):
    return {"tsp": tsp, "gateway_id": 1, "asset_name": "A", "datapoint_id": dp_id, **extra}


def _asset(tsp, name="A", **extra):
    return {"tsp": tsp, "gateway_id": 1, "asset_name": name, **extra}


def _gw(tsp, gateway_id=1, **extra):
    return {"tsp": tsp, "gateway_id": gateway_id, **extra}


def _record_configures(c, delay=0):
    configured = []

    async def configure_asset(asset, assets):
        await asyncio.sleep(delay)
        configured.append(asset)

    c.configure_asset = configure_asset
    return configured


# ------------------------------------------------------------- reading rules


async def test_reads_use_the_latest_marker_and_no_deleted_predicate():
    fake = FakeIF()
    c = _collector(fake)
    _record_configures(c)
    await c.load_gateway_settings()
    await c.load_datapoints()
    await c.load_datapoint_settings()
    await c.load_asset_configs()
    assert set(fake.queries) == {"gateway_settings", "datapoints", "datapoint_settings", "assets"}
    for table, params in fake.queries.items():
        filters = params["filterAnd"]
        assert {"latest": True} in filters, table
        assert not any(f.get("column") in ("latest_flag", "deleted") for f in filters), table


async def test_per_gateway_reads_filter_on_this_gateways_id():
    fake = FakeIF()
    c = _collector(fake)
    await c.load_gateway_settings()
    await c.load_datapoints()
    await c.load_datapoint_settings()
    own = {"column": "gateway_id", "operator": "=", "value": 1}
    for table in ("gateway_settings", "datapoints", "datapoint_settings"):
        assert own in fake.queries[table]["filterAnd"], table


async def test_followed_tables_are_subscribed_before_they_are_read():
    fake = FakeIF()
    c = _collector(fake)
    _record_configures(c)
    await c.load_gateway_settings()
    await c.load_datapoints()
    await c.load_datapoint_settings()
    await c.load_asset_configs()
    assert fake.log == [
        ("subscribe", "gateway_settings"), ("read", "gateway_settings"),
        ("read", "datapoints"),  # the gateway's own catalog: read, not followed
        ("subscribe", "datapoint_settings"), ("read", "datapoint_settings"),
        ("subscribe", "assets"), ("read", "assets"),
        ("read", "assets"),  # the rows without a gateway, to report them
    ]


async def test_the_core_follows_neither_gateways_nor_datapoints():
    fake = FakeIF()
    c = _collector(fake)
    _record_configures(c)
    await c.load_gateway_settings()
    await c.load_datapoints()
    await c.load_datapoint_settings()
    await c.load_asset_configs()
    touched = {table for _, table in fake.log}
    assert "gateways" not in touched
    assert ("subscribe", "datapoints") not in fake.log


async def test_rows_without_a_deleted_flag_are_live():
    fake = FakeIF(rows={
        "datapoints": [_dp("ai:1", _ms(1), deleted=None), _dp("ai:2", _ms(1), deleted=True),
                       _dp("ai:3", _ms(1))],
        "assets": [_asset(_ms(1), "A", deleted=None), _asset(_ms(1), "B", deleted=True),
                   _asset(_ms(1), "C", deleted=False)],
    })
    c = _collector(fake)
    configured = _record_configures(c)
    datapoints = await c.load_datapoints()
    await c.load_asset_configs()
    assert sorted(dp["datapoint_id"] for dp in datapoints) == ["ai:1", "ai:3"]
    assert sorted(a["asset_name"] for a in configured) == ["A", "C"]


async def test_other_gateways_catalog_rows_are_ignored():
    fake = FakeIF(rows={"datapoints": [{**_dp("ai:1", _ms(1)), "gateway_id": 2}]})
    c = _collector(fake)
    assert await c.load_datapoints() == []


# --------------------------------------------------------- gateway_settings


async def test_no_settings_row_means_defaults():
    fake = FakeIF()
    c = _collector(fake, store_data=True)
    c._apply_buffer_size(5)
    await c.load_gateway_settings()
    assert c.settings == {}
    assert c.store_data is True
    assert c._buffer.maxlen == DEFAULT_BUFFER_SIZE


async def test_the_settings_row_read_at_startup_applies():
    fake = FakeIF(rows={"gateway_settings": [_gw(_ms(1), store_data=False, buffer_size=20)]})
    c = _collector(fake)
    await c.load_gateway_settings()
    assert c.settings["buffer_size"] == 20
    assert c.store_data is False and c._buffer.maxlen == 20


async def test_the_settings_read_does_not_undo_a_newer_row():
    async def saved_meanwhile(fake):
        await fake.deliver("gateway_settings", _gw(_iso(9), buffer_size=50))

    fake = FakeIF(rows={"gateway_settings": [_gw(_ms(1), buffer_size=20)]},
                  during_read={"gateway_settings": saved_meanwhile})
    c = _collector(fake)
    await c.load_gateway_settings()
    assert c.settings["buffer_size"] == 50
    assert c._buffer.maxlen == 50
    assert [row["buffer_size"] for row in c.adapter.settings] == [50]  # only the newer row


async def test_a_live_settings_row_reaches_the_adapter_and_store_data():
    fake = FakeIF()
    c = _collector(fake)
    await c.load_gateway_settings()
    c.store = DatapointStore(fake, 1, c.datapoints)
    await fake.deliver("gateway_settings", _gw(_iso(3), store_data=False, iodd_autofetch=False))
    assert c.store_data is False and c.store.store_data is False
    assert c.adapter.settings == [c.settings]
    assert c.adapter.settings[0]["iodd_autofetch"] is False
    # an older row (a late duplicate) changes nothing
    await fake.deliver("gateway_settings", _gw(_iso(2), store_data=True))
    assert c.store_data is False and len(c.adapter.settings) == 1


async def test_another_gateways_settings_are_ignored():
    fake = FakeIF(rows={"gateway_settings": [_gw(_ms(1), gateway_id=2, buffer_size=5)]})
    c = _collector(fake)
    await c.load_gateway_settings()
    assert c.settings == {}
    await fake.deliver("gateway_settings", _gw(_iso(2), gateway_id=2, buffer_size=5))
    assert c.settings == {} and c.adapter.settings == []
    assert c._buffer.maxlen == DEFAULT_BUFFER_SIZE


async def _started(fake):
    """Run the collector's startup (background loops stubbed) and stop it."""
    c = _collector(fake)

    async def idle(*args):
        pass

    c._flush_loop = c._reassert_loop = c.register_gateway = idle
    c.adapter.start_background = idle
    task = asyncio.ensure_future(c.run())
    await asyncio.sleep(0.01)
    task.cancel()
    return c


async def test_run_applies_empty_settings_without_a_row():
    c = await _started(FakeIF())
    assert c.adapter.settings == [{}]


async def test_run_applies_the_loaded_settings_row_once():
    c = await _started(FakeIF(rows={"gateway_settings": [_gw(_ms(1), iodd_autofetch=False)]}))
    assert len(c.adapter.settings) == 1
    assert c.adapter.settings[0]["iodd_autofetch"] is False


# ------------------------------------------------------- datapoint_settings


async def test_a_settings_row_saved_during_the_read_is_not_lost():
    async def saved_meanwhile(fake):
        await fake.deliver("datapoint_settings", _setting("ai:9", _iso(5), enabled=False))

    fake = FakeIF(rows={"datapoint_settings": [_setting("ai:1", _ms(1), enabled=True)]},
                  during_read={"datapoint_settings": saved_meanwhile})
    c = _collector(fake)
    settings = await c.load_datapoint_settings()
    assert sorted(settings) == [("A", "ai:1"), ("A", "ai:9")]


async def test_an_older_read_never_replaces_a_newer_settings_event():
    async def edited_meanwhile(fake):
        await fake.deliver("datapoint_settings", _setting("ai:1", _iso(9), enabled=False))

    fake = FakeIF(rows={"datapoint_settings": [_setting("ai:1", _ms(1), enabled=True)]},
                  during_read={"datapoint_settings": edited_meanwhile})
    c = _collector(fake)
    await c.load_datapoint_settings()
    assert c.datapoint_settings[("A", "ai:1")]["enabled"] is False


async def test_the_same_settings_row_from_the_read_and_the_event_is_applied_once():
    c = _collector(FakeIF())
    assert c._apply_datapoint_settings_row(_setting("ai:1", _ms(3)))
    assert not c._apply_datapoint_settings_row(_setting("ai:1", _iso(3)))
    assert c._apply_datapoint_settings_row(_setting("ai:1", _iso(4)))


async def test_other_gateways_datapoint_settings_are_ignored():
    fake = FakeIF(rows={"datapoint_settings": [{**_setting("ai:1", _ms(1)), "gateway_id": 2}]})
    c = _collector(fake)
    await c.load_datapoint_settings()
    await fake.deliver("datapoint_settings", {**_setting("ai:2", _iso(2)), "gateway_id": 2})
    assert c.datapoint_settings == {}


# ------------------------------------------------------------------ assets


async def test_a_slow_older_asset_row_never_overrides_a_newer_one():
    c = _collector(FakeIF())
    configured = _record_configures(c, delay=0.01)
    assets = []
    older = asyncio.create_task(c._apply_asset_row(_asset(_ms(1), address="old"), assets))
    await asyncio.sleep(0)  # the older row holds the lock and is discovering
    await c._apply_asset_row(_asset(_iso(5), address="new"), assets)
    await older
    assert [a["address"] for a in configured] == ["old", "new"]


async def test_an_older_asset_row_after_a_newer_one_is_dropped():
    c = _collector(FakeIF())
    configured = _record_configures(c)
    assets = []
    assert await c._apply_asset_row(_asset(_iso(5), address="new"), assets)
    assert not await c._apply_asset_row(_asset(_ms(1), address="old"), assets)
    assert [a["address"] for a in configured] == ["new"]


async def test_a_deleting_row_stops_the_asset():
    c = _collector(FakeIF())
    _record_configures(c)
    stopped = []
    c.stop_asset_task = stopped.append
    assets = [_asset(_ms(1))]
    assert await c._apply_asset_row(_asset(_iso(5), deleted=True), assets)
    assert assets == [] and stopped == ["A"]


def _with_catalog(c, fake, store_data=True):
    c.datapoints[:] = [
        {"tsp": _ms(1), "gateway_id": 1, "asset_name": "A", "datapoint_id": "t", "name": "T"},
        {"tsp": _ms(1), "gateway_id": 1, "asset_name": "A", "datapoint_id": "p", "name": "P"},
        {"tsp": _ms(1), "gateway_id": 1, "asset_name": "B", "datapoint_id": "t", "name": "T"},
    ]
    c.store_data = store_data
    c.store = DatapointStore(fake, 1, c.datapoints, store_data=store_data)


async def test_a_deleted_asset_soft_deletes_its_catalog_rows():
    fake = FakeIF()
    c = _collector(fake)
    _with_catalog(c, fake)
    await c._apply_asset_row(_asset(_iso(5), deleted=True), [])
    catalog = [row for table, row in fake.written if table == "datapoints"]
    assert sorted(row["datapoint_id"] for row in catalog) == ["p", "t"]
    assert all(row["deleted"] is True and row["asset_name"] == "A" for row in catalog)
    assert [(row["asset_name"], row["datapoint_id"]) for row in c.datapoints] == [("B", "t")]


async def test_a_deleted_asset_writes_no_catalog_rows_when_forward_only():
    fake = FakeIF()
    c = _collector(fake)
    _with_catalog(c, fake, store_data=False)
    await c._apply_asset_row(_asset(_iso(5), deleted=True), [])
    assert [t for t, _ in fake.written if t == "datapoints"] == []
    assert [row["asset_name"] for row in c.datapoints] == ["B"]


async def test_an_asset_deleted_while_the_gateway_was_down_loses_its_catalog_at_startup():
    fake = FakeIF(rows={"assets": [_asset(_ms(1), "A", deleted=True), _asset(_ms(1), "B")]})
    c = _collector(fake)
    _record_configures(c)
    _with_catalog(c, fake)
    await c.load_asset_configs()
    catalog = [row for table, row in fake.written if table == "datapoints"]
    assert sorted(row["datapoint_id"] for row in catalog) == ["p", "t"]
    assert {row["asset_name"] for row in catalog} == {"A"}


# ------------------------------------------------------- incomplete assets


async def test_incomplete_asset_rows_are_skipped_and_never_written():
    fake = FakeIF(rows={"assets": [
        _asset(_ms(1), ""),                                 # own, no name
        {"tsp": _ms(1), "gateway_id": None, "asset_name": "X"},  # no gateway
        _asset(_ms(1), "OK"),
    ]})
    c = _collector(fake)
    configured = _record_configures(c)
    await c.load_asset_configs()
    assert [a["asset_name"] for a in configured] == ["OK"]
    assert [t for t, _ in fake.written if t == "assets"] == []
    warnings = [r for r in fake.reported if r[1] == "warn"]
    assert len(warnings) == 2
    assert all("delete it on the board" in user for _, _, user in warnings)


async def test_an_incomplete_row_is_reported_once():
    fake = FakeIF(rows={"assets": [{"tsp": _ms(1), "gateway_id": None, "asset_name": "X"}]})
    c = _collector(fake)
    _record_configures(c)
    await c.load_asset_configs()
    await fake.deliver("assets", {"tsp": _iso(2), "gateway_id": None, "asset_name": "X"})
    await fake.deliver("assets", {"tsp": _iso(3), "gateway_id": 1, "asset_name": " "})
    await fake.deliver("assets", {"tsp": _iso(4), "gateway_id": 1, "asset_name": ""})
    warnings = [r for r in fake.reported if r[1] == "warn"]
    assert len(warnings) == 2  # the gateway-less row and this gateway's nameless row


async def test_another_gateways_nameless_row_is_theirs_to_report():
    fake = FakeIF(rows={"assets": [_asset(_ms(1), "", gateway_id=2)]})
    c = _collector(fake)
    _record_configures(c)
    await c.load_asset_configs()
    assert [r for r in fake.reported if r[1] == "warn"] == []


async def test_a_deleted_incomplete_row_is_not_reported():
    fake = FakeIF()
    c = _collector(fake)
    _record_configures(c)
    await c.load_asset_configs()
    await fake.deliver("assets", {"tsp": _iso(2), "gateway_id": None, "asset_name": "X",
                                  "deleted": True})
    assert [r for r in fake.reported if r[1] == "warn"] == []


async def test_the_core_has_no_asset_writer():
    assert not hasattr(Collector, "soft_delete_asset")


async def test_assets_are_read_per_gateway_and_rows_without_one_separately():
    fake = FakeIF()
    c = _collector(fake)
    _record_configures(c)
    reads = []
    real = fake.getHistory

    async def get_history(table, params):
        reads.append((table, params))
        return await real(table, params)

    fake.getHistory = get_history
    await c.load_asset_configs()
    own, orphans = [params for table, params in reads if table == "assets"]
    assert {"column": "gateway_id", "operator": "=", "value": 1} in own["filterAnd"]
    assert own["limit"] == 10000
    assert {"column": "gateway_id", "operator": "IS NULL"} in orphans["filterAnd"]


async def test_a_refused_read_of_rows_without_a_gateway_does_not_stop_startup():
    fake = FakeIF(rows={"assets": [_asset(_ms(1))]})
    c = _collector(fake)
    configured = _record_configures(c)
    real = fake.getHistory

    async def get_history(table, params):
        if any(f.get("operator") == "IS NULL" for f in params["filterAnd"]):
            raise RuntimeError("operator not supported")
        return await real(table, params)

    fake.getHistory = get_history
    await c.load_asset_configs()
    assert [a["asset_name"] for a in configured] == ["A"]


async def test_a_stale_deleted_row_at_startup_keeps_the_catalog_of_a_newer_live_asset():
    # The subscription applies a live row while the read still returns the
    # older deleting row: the catalog must stay.
    async def store_live_row(fake):
        await fake.deliver("assets", _asset(_iso(2)))

    fake = FakeIF(rows={"assets": [_asset(_ms(1), deleted=True)]},
                  during_read={"assets": store_live_row})
    c = _collector(fake)
    _record_configures(c)
    c.store = DatapointStore(fake, 1, [_dp("t", _ms(0))])
    await c.load_asset_configs()
    assert not [row for table, row in fake.written if table == "datapoints"]
