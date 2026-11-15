"""The asset's collection task: discovery runs (and is retried) inside it, a
failing session constructor or close does not end it, and a restarted task
waits for the one it replaces to close its session."""

import asyncio

import pytest

from collector_core.adapter import DatapointStore, ProtocolAdapter, ProtocolSession
from collector_core.collector import Collector, _collect_interval
from collector_core.errors import DeviceConnectionError

_real_sleep = asyncio.sleep


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    """The task's interval and backoff are whole seconds; run them at once."""

    async def sleep(delay, result=None):
        return await _real_sleep(0, result)

    monkeypatch.setattr(asyncio, "sleep", sleep)


class FakeIF:
    def __init__(self):
        self.rows = []
        self.errors = []

    async def append_rows_to_table(self, table, rows):
        self.rows.extend((table, row) for row in rows)

    async def append_to_table(self, table, row):
        self.rows.append((table, row))

    async def report_error(self, error, level="error", user_message=None, **kw):
        self.errors.append((error, level, user_message))

    def statuses(self):
        return [row["status"] for table, row in self.rows if table == "assetstatus"]


class _Session(ProtocolSession):
    def __init__(self, log, close_delay=0):
        self.log = log
        self.close_delay = close_delay

    async def read_values(self, datapoints):
        self.log.append("read")
        return {dp["id"]: 1 for dp in datapoints}

    async def close(self):
        self.log.append("close start")
        for _ in range(self.close_delay):
            await _real_sleep(0)
        self.log.append("close end")


class _Adapter(ProtocolAdapter):
    def __init__(self, discovery_failures=0, session_failures=0, close_delay=0):
        self.discovery_failures = discovery_failures
        self.session_failures = session_failures
        self.close_delay = close_delay
        self.discoveries = 0
        self.log = []

    async def prepare_datapoints(self, asset, store):
        self.discoveries += 1
        if self.discoveries <= self.discovery_failures:
            raise DeviceConnectionError("probe failed", user_message="unreachable")
        return [{"id": "t", "name": "T", "units": "C"}]

    def create_session(self, asset):
        if self.session_failures:
            self.session_failures -= 1
            raise RuntimeError("bad session config")
        self.log.append("open")
        return _Session(self.log, self.close_delay)

    def format_value(self, asset, dp, raw):
        return raw


def _collector(adapter):
    c = Collector(device_name="d", device_key=1, adapter=adapter)
    fake = FakeIF()
    c.set_ironflock(fake)
    c.store = DatapointStore(fake, 1, [])
    return c, fake


async def _until(predicate, steps=200):
    for _ in range(steps):
        if predicate():
            return
        await _real_sleep(0)
    raise AssertionError("condition not reached")


async def test_a_failed_discovery_is_retried_and_marks_the_asset_offline():
    adapter = _Adapter(discovery_failures=2)
    c, fake = _collector(adapter)
    await c.configure_asset({"asset_name": "m", "collect_interval": 1}, [])
    # configure only starts the task; the discovery runs inside it
    assert adapter.discoveries == 0
    await _until(lambda: "online" in fake.statuses())
    c.stop_asset_task("m")
    assert adapter.discoveries == 3
    assert fake.statuses()[:2] == ["offline", "online"]
    assert [e[2] for e in fake.errors] == ["unreachable", "'m' is back online."]


async def test_a_failing_create_session_is_retried():
    adapter = _Adapter(session_failures=1)
    c, fake = _collector(adapter)
    await c.configure_asset({"asset_name": "m", "collect_interval": 1}, [])
    await _until(lambda: "online" in fake.statuses())
    c.stop_asset_task("m")
    assert fake.statuses()[:2] == ["offline", "online"]


async def test_a_restarted_task_waits_for_the_old_session_to_close():
    adapter = _Adapter(close_delay=5)
    c, fake = _collector(adapter)
    await c.configure_asset({"asset_name": "m", "collect_interval": 1}, [])
    await _until(lambda: "read" in adapter.log)
    await c.configure_asset({"asset_name": "m", "collect_interval": 2}, [])
    await _until(lambda: adapter.log.count("open") == 2)
    c.stop_asset_task("m")
    second_open = len(adapter.log) - 1 - adapter.log[::-1].index("open")
    assert adapter.log.index("close end") < second_open


def test_collect_interval_tolerates_bad_values():
    assert _collect_interval({"collect_interval": 5}) == 5
    assert _collect_interval({"collect_interval": 2.5}) == 2
    assert _collect_interval({"collect_interval": "3"}) == 3
    assert _collect_interval({"collect_interval": 0.2}) == 1
    assert _collect_interval({"collect_interval": "abc"}) == 1
    assert _collect_interval({"collect_interval": float("inf")}) == 1
    assert _collect_interval({}) == 1


async def test_upsert_writes_deleted_false_so_a_returning_datapoint_is_live():
    fake = FakeIF()
    store = DatapointStore(fake, 1, [])
    await store.upsert("m", [{"datapoint_id": "t", "asset_name": "m", "gateway_id": 1}])
    await store.prune("m", [])
    await store.upsert("m", [{"datapoint_id": "t", "asset_name": "m", "gateway_id": 1}])
    assert [row["deleted"] for _, row in fake.rows] == [False, True, False]
