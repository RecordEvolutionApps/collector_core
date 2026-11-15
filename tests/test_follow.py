"""The config tables are followed the way the platform expects.

Subscribe first, then read the current rows with the latest marker, apply a
row only when its tsp is newer than the one in force, and treat a NULL
``deleted`` as a live row. Before, the core read first (a row saved meanwhile
was lost until the next change), filtered on the removed ``latest_flag``
column, and skipped every row whose ``deleted`` was NULL.
"""

import asyncio
from datetime import datetime, timezone

from collector_core.adapter import ProtocolAdapter
from collector_core.collector import Collector


class _Adapter(ProtocolAdapter):
    def __init__(self):
        self.settings = []

    async def prepare_datapoints(self, asset, store):
        return []

    def create_session(self, asset):
        return None

    def format_value(self, asset, dp, raw):
        return raw

    async def apply_settings(self, gateway_row):
        self.settings.append(gateway_row)


class FakeIF:
    """Records subscriptions and reads; ``during_read`` runs inside a read,
    like a row the platform stores between the subscription and the read."""

    def __init__(self, rows=None, during_read=None):
        self.rows = rows or {}
        self.during_read = during_read or {}
        self.log = []
        self.handlers = {}
        self.queries = {}

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
        pass

    async def append_rows_to_table(self, table, rows):
        pass

    async def report_error(self, *args, **kwargs):
        pass


def _collector(fake):
    c = Collector(device_name="gw", device_key=1, adapter=_Adapter())
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


def _asset(tsp, name="A", **extra):
    return {"tsp": tsp, "gateway_id": 1, "asset_name": name, **extra}


def _record_configures(c, delay=0):
    configured = []

    async def configure_asset(asset, assets):
        await asyncio.sleep(delay)
        configured.append(asset)

    c.configure_asset = configure_asset
    return configured


async def test_reads_use_the_latest_marker_and_no_deleted_predicate():
    fake = FakeIF()
    c = _collector(fake)
    _record_configures(c)
    await c.load_gateway()
    await c.load_datapoints()
    await c.load_asset_configs()
    for table, params in fake.queries.items():
        filters = params["filterAnd"]
        assert {"latest": True} in filters, table
        assert not any(f.get("column") in ("latest_flag", "deleted") for f in filters), table


async def test_each_table_is_subscribed_before_it_is_read():
    fake = FakeIF()
    c = _collector(fake)
    _record_configures(c)
    await c.load_datapoints()
    await c.load_asset_configs()
    assert fake.log == [
        ("subscribe", "datapoints"), ("read", "datapoints"),
        ("subscribe", "assets"), ("read", "assets"),
    ]


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


async def test_a_row_saved_during_the_read_is_not_lost():
    async def saved_meanwhile(fake):
        await fake.deliver("datapoints", _dp("ai:9", _iso(5)))

    fake = FakeIF(rows={"datapoints": [_dp("ai:1", _ms(1))]},
                  during_read={"datapoints": saved_meanwhile})
    c = _collector(fake)
    datapoints = await c.load_datapoints()
    assert sorted(dp["datapoint_id"] for dp in datapoints) == ["ai:1", "ai:9"]


async def test_an_older_read_never_replaces_a_newer_event():
    async def edited_meanwhile(fake):
        await fake.deliver("datapoints", _dp("ai:1", _iso(9), enabled=False))

    fake = FakeIF(rows={"datapoints": [_dp("ai:1", _ms(1), enabled=True)]},
                  during_read={"datapoints": edited_meanwhile})
    c = _collector(fake)
    datapoints = await c.load_datapoints()
    assert [dp["enabled"] for dp in datapoints] == [False]


async def test_a_deleted_datapoint_is_not_revived_by_an_older_read():
    async def deleted_meanwhile(fake):
        await fake.deliver("datapoints", _dp("ai:1", _iso(9), deleted=True))

    fake = FakeIF(rows={"datapoints": [_dp("ai:1", _ms(1))]},
                  during_read={"datapoints": deleted_meanwhile})
    c = _collector(fake)
    assert await c.load_datapoints() == []


async def test_the_same_row_from_the_read_and_the_event_is_applied_once():
    fake = FakeIF()
    c = _collector(fake)
    datapoints = []
    assert c._apply_datapoint_row(datapoints, _dp("ai:1", _ms(3)))
    assert not c._apply_datapoint_row(datapoints, _dp("ai:1", _iso(3)))
    assert c._apply_datapoint_row(datapoints, _dp("ai:1", _iso(4)))


async def test_other_gateways_rows_are_ignored():
    fake = FakeIF(rows={"datapoints": [{**_dp("ai:1", _ms(1)), "gateway_id": 2}]})
    c = _collector(fake)
    assert await c.load_datapoints() == []


async def test_the_gateway_read_does_not_undo_a_newer_settings_row():
    async def saved_meanwhile(fake):
        await fake.deliver("gateways", {"tsp": _iso(9), "gateway_name": "gw", "buffer_size": 50})

    fake = FakeIF(rows={"gateways": [{"tsp": _ms(1), "gateway_name": "gw", "buffer_size": 20}]},
                  during_read={"gateways": saved_meanwhile})
    c = _collector(fake)
    await fake.subscribe_to_table("gateways", c._handle_gateway_update())
    await c.load_gateway()
    assert c.gateway["buffer_size"] == 50
    assert c._buffer.maxlen == 50
    assert len(c.adapter.settings) == 1  # only the newer row reached the adapter


async def test_another_gateways_settings_are_ignored():
    fake = FakeIF()
    c = _collector(fake)
    await c._handle_gateway_update()({"tsp": _iso(1), "gateway_name": "other", "buffer_size": 5})
    assert c.gateway == {}


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
