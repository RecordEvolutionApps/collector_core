"""Latest entity rows are rewritten before table retention can drop them.

Retention drops whole chunks, the latest row of an entity included, so a
catalog row or an asset status that never changes used to age out after the
table's dropAfter: the board lost the datapoint, and the user's per-datapoint
pause / change-detection setting with it.
"""

import time
from datetime import datetime, timedelta, timezone

import collector_core.collector as core
from collector_core.adapter import ProtocolAdapter
from collector_core.collector import Collector


class _Adapter(ProtocolAdapter):
    async def prepare_datapoints(self, asset, store):
        return []

    def create_session(self, asset):
        return None

    def format_value(self, asset, dp, raw):
        return raw


class FakeIF:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    async def append_rows_to_table(self, table, rows):
        if self.fail:
            raise RuntimeError("link down")
        self.calls.append((table, list(rows)))

    async def report_error(self, *args, **kwargs):
        pass


def _collector(**kw):
    c = Collector(device_name="d", device_key=1, adapter=_Adapter())
    c.set_ironflock(FakeIF(**kw))
    return c


def _old(days):
    return datetime.now(timezone.utc) - timedelta(days=days)


def _row(dp, tsp, **extra):
    return {"datapoint_id": dp, "asset_name": "A", "gateway_id": 1, "name": dp,
            "enabled": False, "deleted": False, "tsp": tsp,
            "latest_flag": True, "authid": "1", "device_key": 1, **extra}


async def test_old_catalog_rows_are_rewritten_whatever_their_tsp_form():
    c = _collector()
    c.datapoints[:] = [
        _row("iso", _old(10).isoformat()),                       # event / own write
        _row("ms", _old(10).timestamp() * 1000),                  # history read (epoch ms)
        _row("dt", _old(10)),                                     # datetime
        _row("zulu", _old(10).strftime("%Y-%m-%dT%H:%M:%S.000Z")),
        _row("fresh", _old(1).isoformat()),
        _row("unknown", None),
    ]
    written = await c.reassert_stale_rows()

    assert written == 4
    (table, rows), = c.ironflock.calls
    assert table == "datapoints"
    assert sorted(r["datapoint_id"] for r in rows) == ["dt", "iso", "ms", "zulu"]
    for r in rows:
        # The full row, user-owned columns included, minus platform columns.
        assert r["enabled"] is False and r["name"] == r["datapoint_id"]
        assert not {"latest_flag", "authid", "device_key"} & set(r)
        assert core._epoch_seconds(r["tsp"]) > time.time() - 60


async def test_a_rewritten_row_is_not_rewritten_again_on_the_next_pass():
    c = _collector()
    c.datapoints[:] = [_row("a", _old(30).isoformat())]
    assert await c.reassert_stale_rows() == 1
    assert await c.reassert_stale_rows() == 0


async def test_a_long_unchanged_status_is_rewritten(monkeypatch):
    c = _collector()
    await c.set_asset_status({"asset_name": "A"}, "online")
    c.ironflock.calls.clear()
    assert await c.reassert_stale_rows() == 0  # fresh

    # Age the last write past the threshold.
    when, status, detail = c._status_written["A"]
    c._status_written["A"] = (when - c.reassert_after - 1, status, detail)
    assert await c.reassert_stale_rows() == 1
    rows = [row for table, rows in c.ironflock.calls for row in rows if table == "assetstatus"]
    assert rows == [{"tsp": rows[0]["tsp"], "asset_name": "A", "gateway_id": 1,
                     "status": "online", "detail": "", "deleted": False}]


async def test_a_deleted_assets_status_is_not_revived():
    c = _collector()
    asset = {"asset_name": "A"}
    await c.set_asset_status(asset, "online")
    await c.clear_asset_status(asset)
    c.reassert_after = 0
    c.ironflock.calls.clear()
    assert await c.reassert_stale_rows() == 0


async def test_forward_only_gateways_write_nothing():
    c = _collector()
    c.store_data = False
    c.datapoints[:] = [_row("a", _old(30).isoformat())]
    assert await c.reassert_stale_rows() == 0
    assert c.ironflock.calls == []


async def test_a_failed_rewrite_is_retried_next_pass_and_never_raises():
    c = _collector(fail=True)
    c.datapoints[:] = [_row("a", _old(30).isoformat())]
    assert await c.reassert_stale_rows() == 0
    c.ironflock.fail = False
    assert await c.reassert_stale_rows() == 1


def test_the_threshold_stays_far_below_the_template_retention():
    # The core template drops datapoints and assetstatus after 3 months; a row
    # must be rewritten long before that even if a pass is missed.
    assert core.REASSERT_AFTER + 2 * core.REASSERT_INTERVAL < 30 * 24 * 3600
