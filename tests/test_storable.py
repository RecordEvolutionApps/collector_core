"""Rows the platform cannot carry are repaired before they enter the buffer.

One unencodable value (a lone surrogate from a JSON escape, NaN, a 2**70
integer, a set) used to sit at the head of the shared buffer and block every
asset's writes. These tests drive the real Collector against a fake platform
that refuses what msgpack/JSON cannot carry.
"""

import json
import math
from datetime import datetime, timezone

from collector_core.adapter import DatapointStore, ProtocolAdapter, emit_error
from collector_core.collector import Collector
from collector_core.storable import repair_text, storable


class _Adapter(ProtocolAdapter):
    async def prepare_datapoints(self, asset, store):
        return []

    def create_session(self, asset):
        return None

    def format_value(self, asset, dp, raw):
        return raw


def _encodable(rows):
    """What the SDK's msgpack serializer and the platform's JSON columns refuse."""
    stack = [rows]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            for key, value in item.items():
                if not isinstance(key, str):
                    raise TypeError("non-text key")
                stack.extend([key, value])
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
        elif isinstance(item, int) and not isinstance(item, bool):
            if not -(2**63) <= item <= 2**64 - 1:
                raise OverflowError("integer out of range")
    json.dumps(rows, allow_nan=False, ensure_ascii=False).encode("utf-8")


class StrictPlatform:
    def __init__(self):
        self.calls = []
        self.reported = []

    async def append_rows_to_table(self, table, rows):
        _encodable(rows)
        self.calls.append((table, list(rows)))

    async def append_to_table(self, table, row):
        _encodable(row)
        self.calls.append((table, [row]))

    async def report_error(self, error, level="error", append=False, tsp=None, user_message=None):
        _encodable([error, user_message])
        self.reported.append((error, level, user_message))


def _collector():
    c = Collector(device_name="d", device_key=1, adapter=_Adapter())
    c.set_ironflock(StrictPlatform())
    return c


def _datapoints(*ids):
    return [{"id": i, "name": i, "units": ""} for i in ids]


# ------------------------------------------------------------- the rule itself


def test_clean_values_pass_through_untouched():
    row = {"a": 1, "b": [1.5, "x", None, True], "c": {"d": "é"}}
    out, problems = storable(row)
    assert out is row and problems == []


def test_lone_surrogates_are_escaped_in_values_and_keys():
    out, problems = storable({"v": "a\ud800b", "\udc80k": 1})
    assert out == {"v": "a\\ud800b", "\\udc80k": 1}
    assert problems == ["a key with an unencodable character", "text with an unencodable character"]
    json.dumps(out, ensure_ascii=False).encode("utf-8")


def test_non_finite_numbers_become_null():
    out, problems = storable({"a": float("nan"), "b": float("inf"), "c": 2.5})
    assert out == {"a": None, "b": None, "c": 2.5}
    assert problems == ["a non-finite number"]


def test_out_of_range_integers_and_unknown_types_become_text():
    when = datetime(2026, 1, 2, tzinfo=timezone.utc)
    out, _ = storable({"big": 2**70, "set": {1}, "when": when, "raw": b"\x00\xff", "ok": 2**63})
    assert out == {
        "big": str(2**70),
        "set": "{1}",
        "when": when.isoformat(),
        "raw": "00ff",
        "ok": 2**63,
    }


def test_non_text_keys_become_text_and_tuples_lists():
    out, problems = storable({1: (1, "\ud800")})
    assert out == {"1": [1, "\\ud800"]}
    assert "a non-text key" in problems


def test_deep_nesting_does_not_hit_the_recursion_limit():
    deep = current = {}
    for _ in range(5000):
        current["n"] = {}
        current = current["n"]
    current["v"] = "\ud800"
    out, problems = storable(deep)
    assert problems == ["text with an unencodable character"]
    for _ in range(5000):
        out = out["n"]
    assert out == {"v": "\\ud800"}


def test_repair_text_only_touches_text_that_needs_it():
    assert repair_text("plain é") == "plain é"
    assert repair_text("x\udfffy") == "x\\udfffy"


# ------------------------------------------------- the collector's write path


async def test_a_poisoned_row_no_longer_blocks_other_assets():
    c = _collector()
    bad = {"asset_name": "Bad"}
    good = {"asset_name": "Good"}
    await c._publish_batch(bad, _datapoints("v"), {"v": "\ud800 boom"})
    await c._publish_batch(good, _datapoints("t"), {"t": 21.5})
    await c._publish_batch(bad, _datapoints("v"), {"v": float("nan")})

    assert not c._buffer and not c._link_down
    written = [row for table, rows in c.ironflock.calls for row in rows if table == "measurements"]
    assert [row["asset_name"] for row in written] == ["Bad", "Good", "Bad"]
    assert written[0]["data"]["v"]["value"] == "\\ud800 boom"
    assert written[2]["data"]["v"]["value"] is None


async def test_a_repair_is_reported_once_per_asset_and_kind():
    c = _collector()
    for _ in range(3):
        await c._publish_batch({"asset_name": "Bad"}, _datapoints("v"), {"v": "\ud800"})
    await c._publish_batch({"asset_name": "Bad"}, _datapoints("v"), {"v": float("nan")})
    await c._publish_batch({"asset_name": "Other"}, _datapoints("v"), {"v": "\ud800"})

    warnings = [r for r in c.ironflock.reported if r[1] == "warn"]
    assert len(warnings) == 3  # Bad/text, Bad/number, Other/text
    assert all("repaired a measurements row" in message for message, _, _ in warnings)
    assert "'Bad' sent a value the platform cannot store" in warnings[0][2]


async def test_an_offline_detail_with_a_surrogate_is_stored():
    c = _collector()
    await c.set_asset_status({"asset_name": "A"}, "offline", "decode failed: \ud800")
    rows = [row for table, rows in c.ironflock.calls for row in rows if table == "assetstatus"]
    assert rows and rows[0]["detail"] == "decode failed: \\ud800"


async def test_publish_batch_does_not_print_every_row(capsys):
    c = _collector()
    await c._publish_batch({"asset_name": "A"}, _datapoints("v"), {"v": 1})
    # A healthy write path is silent: the device log is not a row log.
    assert capsys.readouterr().out == ""
    assert c.ironflock.calls


async def test_emit_error_repairs_its_texts():
    platform = StrictPlatform()
    await emit_error(platform, "bad \ud800", level="warn", user_message="shown \udc80")
    assert platform.reported == [("bad \\ud800", "warn", "shown \\udc80")]


# ------------------------------------------------------------ the store's writes


async def test_the_store_writes_a_repaired_catalog_row_once():
    platform = StrictPlatform()
    datapoints = []
    store = DatapointStore(platform, 1, datapoints)
    payload = {"datapoint_id": "k", "asset_name": "A", "gateway_id": 1,
               "name": "kitchen \ud800", "deleted": False}
    await store.upsert("A", [payload])
    await store.upsert("A", [payload])  # unchanged after repair: not rewritten
    assert len(platform.calls) == 1
    assert platform.calls[0][1][0]["name"] == "kitchen \\ud800"
    await store.prune("A", [])
    assert platform.calls[-1][1][0]["deleted"] is True


def test_the_process_output_tolerates_lone_surrogates():
    import io

    from collector_core.collector import _tolerate_unencodable_output

    raw = io.BytesIO()
    strict = io.TextIOWrapper(raw, encoding="utf-8", errors="strict")
    _tolerate_unencodable_output([strict, object()])  # a non-wrapper is skipped
    print("collect_asset A error: bad \ud800", file=strict)
    strict.flush()
    assert raw.getvalue() == b"collect_asset A error: bad \\ud800\n"


def test_nan_is_not_a_finite_float():
    # Guard the premise the repair relies on.
    assert not math.isfinite(float("nan"))
