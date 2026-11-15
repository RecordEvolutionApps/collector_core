"""refresh_transforms: after the core writes a table, or applies a followed row
of its own gateway, it refreshes the transforms the app mapped to that table.

Refreshes run in the background, coalesce, keep a minimum gap per transform,
retry once while a scheduled refresh holds the view, and never raise.
"""

import asyncio
import time

import pytest

import collector_core.collector as core
from collector_core.adapter import DatapointStore, ProtocolAdapter
from collector_core.collector import Collector


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(core, "REFRESH_MIN_INTERVAL", 0.05)
    monkeypatch.setattr(core, "REFRESH_RETRY_DELAY", 0.03)


class _Adapter(ProtocolAdapter):
    async def prepare_datapoints(self, asset, store):
        return []

    def create_session(self, asset):
        return None

    def format_value(self, asset, dp, raw):
        return raw


class FakeIF:
    """Records writes and refreshes. ``errors`` is a list of exceptions the
    next refresh calls raise, in order; ``gate`` holds every refresh."""

    def __init__(self, fail_on=None, errors=None):
        self.written = []
        self.refreshed = []  # (name, start, end) per successful call
        self.calls = []      # (name, monotonic) per call, failed or not
        self.fail_on = fail_on
        self.errors = list(errors or [])
        self.gate = None
        self.handlers = {}
        self.rows = {}

    async def append_rows_to_table(self, table, rows):
        if table == self.fail_on:
            raise RuntimeError("link down")
        self.written.append((table, list(rows)))

    async def append_to_table(self, table, row):
        self.written.append((table, [row]))

    async def subscribe_to_table(self, table, handler):
        self.handlers[table] = handler

    async def getHistory(self, table, params):
        return list(self.rows.get(table, []))

    async def report_error(self, *args, **kwargs):
        pass

    async def refresh_transform(self, name):
        start = time.monotonic()
        self.calls.append((name, start))
        if self.gate is not None:
            await self.gate.wait()
        if self.errors:
            raise self.errors.pop(0)
        self.refreshed.append((name, start, time.monotonic()))
        return {"refreshed": True, "published": True}

    def names(self):
        return [name for name, _, _ in self.refreshed]


class OldIF(FakeIF):
    """An SDK before 1.11.0, without refresh_transform."""

    refresh_transform = None


MAPPING = {
    "assetstatus": ["asset_status_counts", "asset_overview"],
    "assets": ["asset_overview"],
    "gateways": ["gateway_overview"],
    "gateway_settings": ["gateway_overview"],
    "datapoints": ["datapoint_overview"],
    "datapoint_settings": ["datapoint_overview"],
}


def _collector(fake=None, mapping=MAPPING, **kw):
    c = Collector(device_name="d", device_key=1, adapter=_Adapter(),
                  refresh_transforms=mapping, **kw)
    c.set_ironflock(fake if fake is not None else FakeIF())
    return c


async def _settle(c):
    """Wait for the background drains and refreshes."""
    while c._draining or c._refreshing:
        await asyncio.gather(*c._draining, *c._refreshing)


def _row(n=0):
    return {"tsp": f"t{n}", "asset_name": "a", "gateway_id": 1}


# ------------------------------------------------------------ configuration


def test_a_single_name_is_a_list_and_the_default_is_empty():
    assert _collector(mapping={"assets": "x"}).refresh_transforms == {"assets": ["x"]}
    assert Collector(device_name="d", device_key=1, adapter=_Adapter()).refresh_transforms == {}


# ------------------------------------------------------------- after writes


async def test_a_drain_refreshes_the_tables_transforms_in_order():
    c = _collector()
    c._buffer.extend([("assetstatus", _row(1)), ("assetstatus", _row(2))])
    await c._drain()
    await _settle(c)
    assert c.ironflock.names() == ["asset_status_counts", "asset_overview"]


async def test_a_failed_chunk_refreshes_only_what_was_written():
    c = _collector(FakeIF(fail_on="assetstatus"), mapping={
        "assetstatus": ["by_status"], "measurements": ["by_reading"]})
    c._buffer.extend([("measurements", _row(1)), ("assetstatus", _row(2))])
    await c._drain()
    await _settle(c)
    assert c.ironflock.names() == ["by_reading"]
    assert [t for t, _ in c._buffer] == ["assetstatus"]


async def test_unmapped_tables_refresh_nothing():
    c = _collector()
    c._buffer.append(("measurements", _row()))
    await c._drain()
    await _settle(c)
    assert c.ironflock.calls == []


async def test_forward_only_writes_and_refreshes_nothing():
    c = _collector()
    c.store_data = False
    await c.set_asset_status({"asset_name": "a"}, "online")
    await _settle(c)
    assert c.ironflock.written == [] and c.ironflock.calls == []


async def test_a_drain_does_not_wait_for_the_refresh():
    fake = FakeIF()
    fake.gate = asyncio.Event()
    c = _collector(fake)
    c._buffer.append(("assetstatus", _row()))
    await asyncio.wait_for(c._drain(), 0.1)
    await asyncio.sleep(0.01)
    assert [name for name, _ in fake.calls] == ["asset_status_counts"]  # held, in flight
    fake.gate.set()
    await _settle(c)
    assert fake.names() == ["asset_status_counts", "asset_overview"]


async def test_the_registration_refreshes_the_gateways_transforms():
    c = _collector()
    await c.register_gateway()
    await _settle(c)
    assert c.ironflock.names() == ["gateway_overview"]


async def test_catalog_writes_refresh_and_forward_only_ones_do_not():
    c = _collector()
    store = DatapointStore(c.ironflock, 1, c.datapoints, on_write=c.request_refresh)
    payload = {"datapoint_id": "t", "asset_name": "A", "gateway_id": 1, "name": "T"}
    await store.upsert("A", [payload])
    await _settle(c)
    assert c.ironflock.names() == ["datapoint_overview"]
    await store.upsert("A", [payload])  # unchanged: no write, no refresh
    await asyncio.sleep(0.1)
    await store.prune("A", [])
    await _settle(c)
    assert c.ironflock.names() == ["datapoint_overview"] * 2

    c2 = _collector()
    store2 = DatapointStore(c2.ironflock, 1, c2.datapoints, store_data=False,
                            on_write=c2.request_refresh)
    await store2.upsert("A", [payload])
    await store2.prune("A", [])
    await _settle(c2)
    assert c2.ironflock.calls == []


async def test_the_keep_alive_refreshes_the_catalogs_transforms():
    c = _collector()
    c.reassert_after = 0
    c.datapoints[:] = [{"datapoint_id": "t", "asset_name": "A", "gateway_id": 1,
                        "tsp": "2020-01-01T00:00:00+00:00"}]
    assert await c.reassert_stale_rows() == 1
    await _settle(c)
    assert c.ironflock.names() == ["datapoint_overview"]


# ------------------------------------------------------ after applied rows


async def test_applying_an_own_asset_row_refreshes_and_another_gateways_does_not():
    c = _collector()

    async def configure_asset(asset, assets):
        pass

    c.configure_asset = configure_asset
    await c.load_asset_configs()
    await c.ironflock.handlers["assets"]({"tsp": "2026-01-01T00:00:01+00:00",
                                          "gateway_id": 2, "asset_name": "X"})
    await _settle(c)
    assert c.ironflock.calls == []
    await c.ironflock.handlers["assets"]({"tsp": "2026-01-01T00:00:02+00:00",
                                          "gateway_id": 1, "asset_name": "A"})
    await _settle(c)
    assert c.ironflock.names() == ["asset_overview"]


async def test_applying_settings_rows_refreshes_their_transforms():
    c = _collector()
    await c.load_gateway_settings()
    await c.load_datapoint_settings()
    await c.ironflock.handlers["gateway_settings"]({"tsp": "2026-01-01T00:00:01+00:00",
                                                    "gateway_id": 1, "buffer_size": 9})
    c.ironflock.handlers["datapoint_settings"]({"tsp": "2026-01-01T00:00:01+00:00",
                                                "gateway_id": 1, "asset_name": "A",
                                                "datapoint_id": "t", "enabled": False})
    await _settle(c)
    assert sorted(c.ironflock.names()) == ["datapoint_overview", "gateway_overview"]
    # another gateway's rows are not applied, so they refresh nothing
    c.ironflock.handlers["datapoint_settings"]({"tsp": "2026-01-01T00:00:09+00:00",
                                                "gateway_id": 2, "asset_name": "A",
                                                "datapoint_id": "t"})
    await _settle(c)
    assert len(c.ironflock.refreshed) == 2


# --------------------------------------------------------------- coalescing


async def test_requests_during_a_refresh_coalesce_into_one_more():
    fake = FakeIF()
    fake.gate = asyncio.Event()
    c = _collector(fake, mapping={"assets": ["overview"]})
    for _ in range(50):
        c.request_refresh("assets")
        await asyncio.sleep(0)
    await asyncio.sleep(0.01)
    assert len(fake.calls) == 1  # one in flight, the rest wait as one
    fake.gate.set()
    await _settle(c)
    assert fake.names() == ["overview", "overview"]


async def test_two_refreshes_of_a_transform_keep_the_minimum_gap():
    c = _collector(mapping={"assets": ["overview"]})
    c.request_refresh("assets")
    await _settle(c)
    c.request_refresh("assets")
    await _settle(c)
    (_, _, first_end), (_, second_start, _) = c.ironflock.refreshed
    assert second_start - first_end >= core.REFRESH_MIN_INTERVAL * 0.9


async def test_a_transform_shared_by_two_tables_is_refreshed_once_per_burst():
    c = _collector(mapping={"assetstatus": ["counts", "overview"], "assets": ["overview"]})
    c.request_refresh("assetstatus")
    c.request_refresh("assets")
    await _settle(c)
    names = c.ironflock.names()
    assert names.index("counts") < len(names) - 1 - names[::-1].index("overview")
    assert names.count("overview") <= 2 and names.count("counts") == 1


async def test_the_list_is_refreshed_bottom_up_after_each_request():
    c = _collector(mapping={"assetstatus": ["counts", "overview"]})
    for _ in range(3):
        c.request_refresh("assetstatus")
        await asyncio.sleep(0.01)
    await _settle(c)
    names = c.ironflock.names()
    assert names[0] == "counts" and names[-1] == "overview"
    for i, name in enumerate(names):
        if name == "overview":
            assert "counts" in names[:i]


# ------------------------------------------------------------------ failures


async def test_a_refresh_in_progress_is_retried_once_after_the_delay():
    fake = FakeIF(errors=[RuntimeError("refresh refused: URI_REFRESH_IN_PROGRESS")])
    c = _collector(fake, mapping={"assets": ["overview"]})
    c.request_refresh("assets")
    await _settle(c)
    (_, first), (_, second) = fake.calls
    assert second - first >= core.REFRESH_RETRY_DELAY * 0.9
    assert fake.names() == ["overview"]


async def test_a_second_refusal_is_printed_not_raised(capsys):
    refusal = RuntimeError("URI_REFRESH_IN_PROGRESS")
    fake = FakeIF(errors=[refusal, refusal])
    c = _collector(fake, mapping={"assets": ["overview"]})
    c.request_refresh("assets")
    await _settle(c)
    assert len(fake.calls) == 2 and fake.refreshed == []
    assert "refresh of transform overview failed" in capsys.readouterr().out


async def test_any_other_failure_is_printed_once_and_the_list_goes_on(capsys):
    fake = FakeIF(errors=[RuntimeError("unknown transform")])
    c = _collector(fake, mapping={"assetstatus": ["counts", "overview"]})
    c._buffer.append(("assetstatus", _row()))
    await c._drain()
    await _settle(c)
    assert [name for name, _ in fake.calls] == ["counts", "overview"]  # no retry
    assert fake.names() == ["overview"]
    assert not c._buffer
    assert "unknown transform" in capsys.readouterr().out


async def test_an_sdk_without_refresh_transform_skips_and_says_so_once(capsys):
    c = _collector(OldIF())
    c._buffer.append(("assetstatus", _row()))
    await c._drain()
    c.request_refresh("assets")
    await _settle(c)
    assert not c._buffer and c._refreshing == set()
    assert capsys.readouterr().out.count("needs >= 1.11.0") == 1


async def test_nothing_configured_refreshes_nothing():
    c = _collector(mapping=None)
    c._buffer.append(("assetstatus", _row()))
    await c._drain()
    c.request_refresh("assets")
    await _settle(c)
    assert c.ironflock.calls == []


async def test_an_adapter_can_request_a_refresh_of_its_own_table():
    c = _collector(mapping={"iodds": ["iodd_overview"]})
    c.request_refresh("iodds")
    await _settle(c)
    assert c.ironflock.names() == ["iodd_overview"]
