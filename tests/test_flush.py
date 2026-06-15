"""Buffer catch-up: bulk insert, per-table grouping, chunk cap, failure re-queue."""

from collections import deque

from collector_core.adapter import ProtocolAdapter
from collector_core.collector import BULK_CHUNK_MAX, Collector


class _Adapter(ProtocolAdapter):
    async def prepare_datapoints(self, asset, store):
        return []

    def create_session(self, asset):
        return None

    def format_value(self, asset, dp, raw):
        return raw


class FakeIF:
    """Records bulk calls; can fail by table or after N successful calls."""

    def __init__(self, fail_on=None, fail_after=None):
        self.calls = []
        self.fail_on = fail_on
        self.fail_after = fail_after

    async def append_rows_to_table(self, table, rows):
        if self.fail_on is not None and table == self.fail_on:
            raise RuntimeError("link down")
        if self.fail_after is not None and len(self.calls) >= self.fail_after:
            raise RuntimeError("link down")
        self.calls.append((table, list(rows)))


def _collector(items, **kw):
    c = Collector(device_name="d", device_key=1, adapter=_Adapter())
    c.set_ironflock(FakeIF(**kw))
    c._buffer = deque(items)
    return c


async def test_groups_consecutive_same_table():
    c = _collector([("measurements", "m1"), ("measurements", "m2"),
                    ("assetstatus", "s1"), ("measurements", "m3")])
    await c._flush()
    assert c.ironflock.calls == [("measurements", ["m1", "m2"]),
                                 ("assetstatus", ["s1"]),
                                 ("measurements", ["m3"])]
    assert not c._buffer


async def test_chunk_cap_splits_long_run():
    n = BULK_CHUNK_MAX * 2 + 7
    c = _collector([("measurements", i) for i in range(n)])
    await c._flush()
    assert [len(rows) for _, rows in c.ironflock.calls] == [BULK_CHUNK_MAX, BULK_CHUNK_MAX, 7]
    flat = [r for _, rows in c.ironflock.calls for r in rows]
    assert flat == list(range(n))  # order preserved across chunks
    assert not c._buffer


async def test_failure_requeues_unsent_oldest_first():
    c = _collector([("measurements", "m1"), ("measurements", "m2"),
                    ("assetstatus", "s1"), ("measurements", "m3")],
                   fail_on="assetstatus")
    await c._flush()
    assert c.ironflock.calls == [("measurements", ["m1", "m2"])]
    assert list(c._buffer) == [("assetstatus", "s1"), ("measurements", "m3")]


async def test_chunk_failure_keeps_already_sent_chunks():
    n = BULK_CHUNK_MAX * 3
    c = _collector([("measurements", i) for i in range(n)], fail_after=1)
    await c._flush()
    assert [len(r) for _, r in c.ironflock.calls] == [BULK_CHUNK_MAX]
    assert len(c._buffer) == n - BULK_CHUNK_MAX
    assert c._buffer[0] == ("measurements", BULK_CHUNK_MAX)
    assert c._buffer[-1] == ("measurements", n - 1)


async def test_empty_buffer_is_noop():
    c = _collector([])
    await c._flush()
    assert c.ironflock.calls == [] and not c._buffer
