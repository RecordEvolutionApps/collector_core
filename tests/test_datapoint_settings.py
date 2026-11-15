"""Per-datapoint write gating reads datapoint_settings, not the catalog.

``enabled`` is the null-safe pause switch (only an explicit false drops the
datapoint), ``change_detection`` writes a datapoint only when its value
changed. Both come from the user's ``datapoint_settings`` rows, which the
board and the AI assistant write; the gateway's catalog holds no switches.
"""

import itertools
from datetime import datetime, timedelta, timezone

from collector_core.adapter import ProtocolAdapter
from collector_core.collector import Collector


class _Adapter(ProtocolAdapter):
    async def prepare_datapoints(self, asset, store):
        return []

    def create_session(self, asset):
        return None

    def format_value(self, asset, dp, raw):
        return raw


class _Collector(Collector):
    """Captures the measurements rows instead of buffering them."""

    def __init__(self):
        super().__init__(device_name="gw", device_key=1, adapter=_Adapter())
        self.sent = []

    async def _send(self, table, payload):
        self.sent.append(payload["data"])


ASSET = {"asset_name": "A"}
DATAPOINTS = [{"id": i, "name": i, "units": ""} for i in ("t", "p", "s")]

_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
_tick = itertools.count(1)


def _setting(dp_id, asset_name="A", gateway_id=1, **columns):
    """A settings row, each one newer than the last."""
    tsp = (_START + timedelta(seconds=next(_tick))).isoformat()
    return {"tsp": tsp,
            "gateway_id": gateway_id, "asset_name": asset_name, "datapoint_id": dp_id,
            **columns}


async def _publish(c, raw):
    await c._publish_batch(ASSET, DATAPOINTS, raw)
    return c.sent[-1] if c.sent else None


async def test_without_settings_every_datapoint_is_written():
    c = _Collector()
    assert set(await _publish(c, {"t": 1, "p": 2, "s": 3})) == {"t", "p", "s"}


async def test_an_explicit_false_pauses_and_null_keeps_collecting():
    c = _Collector()
    c._apply_datapoint_settings_row(_setting("t", enabled=False))
    c._apply_datapoint_settings_row(_setting("p", enabled=None))
    c._apply_datapoint_settings_row(_setting("s", enabled=True))
    assert set(await _publish(c, {"t": 1, "p": 2, "s": 3})) == {"p", "s"}


async def test_a_paused_catalog_column_no_longer_gates():
    c = _Collector()
    c.datapoints[:] = [{"asset_name": "A", "datapoint_id": "t", "enabled": False,
                        "change_detection": True}]
    assert set(await _publish(c, {"t": 1})) == {"t"}
    assert set(await _publish(c, {"t": 1})) == {"t"}


async def test_another_assets_or_gateways_settings_do_not_gate():
    c = _Collector()
    c._apply_datapoint_settings_row(_setting("t", asset_name="B", enabled=False))
    c._apply_datapoint_settings_row(_setting("p", gateway_id=2, enabled=False))
    assert set(await _publish(c, {"t": 1, "p": 2})) == {"t", "p"}


async def test_change_detection_writes_changes_only():
    c = _Collector()
    c._apply_datapoint_settings_row(_setting("t", change_detection=True))
    assert set(await _publish(c, {"t": 1, "p": 5})) == {"t", "p"}
    assert set(await _publish(c, {"t": 1, "p": 5})) == {"p"}
    assert set(await _publish(c, {"t": 2, "p": 5})) == {"t", "p"}


async def test_a_row_emptied_by_gating_is_not_sent():
    c = _Collector()
    c._apply_datapoint_settings_row(_setting("t", enabled=False))
    assert not await c._publish_batch(ASSET, DATAPOINTS, {"t": 1})
    assert c.sent == []


async def test_a_new_settings_row_takes_effect_on_the_next_publish():
    c = _Collector()
    c._apply_datapoint_settings_row(_setting("t", change_detection=True))
    await _publish(c, {"t": 1})
    assert not await c._publish_batch(ASSET, DATAPOINTS, {"t": 1})  # unchanged
    # pause it, then resume it: the first reading after a change is a baseline
    c._apply_datapoint_settings_row(_setting("t", enabled=False))
    assert not await c._publish_batch(ASSET, DATAPOINTS, {"t": 1})
    c._apply_datapoint_settings_row(_setting("t", enabled=True, change_detection=True))
    assert set(await _publish(c, {"t": 1})) == {"t"}
