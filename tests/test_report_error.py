"""Error reporting: best-effort emit, asset-prefixing, and transition de-dup."""

from collector_core.adapter import DatapointStore, ProtocolAdapter
from collector_core.collector import Collector


class _Adapter(ProtocolAdapter):
    async def prepare_datapoints(self, asset, store):
        return []

    def create_session(self, asset):
        return None

    def format_value(self, asset, dp, raw):
        return raw


class FakeIF:
    """Records report_error / assetstatus writes; can fail the error channel."""

    def __init__(self, error_raises=False):
        self.errors = []
        self.user_messages = []
        self.rows = []
        self.error_raises = error_raises

    async def append_rows_to_table(self, table, rows):
        self.rows.append((table, list(rows)))

    async def report_error(self, error, level="error", append=False, tsp=None, user_message=None):
        if self.error_raises:
            raise RuntimeError("report channel down")
        self.errors.append((error, level))
        self.user_messages.append(user_message)


def _collector(**kw):
    c = Collector(device_name="d", device_key=1, adapter=_Adapter())
    c.set_ironflock(FakeIF(**kw))
    return c


async def test_set_asset_status_returns_true_only_on_change():
    c = _collector()
    asset = {"asset_name": "A"}
    assert await c.set_asset_status(asset, "offline", "boom") is True
    assert await c.set_asset_status(asset, "offline", "boom again") is False
    assert await c.set_asset_status(asset, "online") is True
    assert await c.set_asset_status(asset, "offline", "boom") is True


async def test_offline_report_is_deduped_per_outage():
    """Mirrors collect_asset's except-block: report only on the offline
    transition, so a persistently failing asset toasts once per outage."""
    c = _collector()
    asset = {"asset_name": "Pump"}

    async def fail():
        if await c.set_asset_status(asset, "offline", "boom"):
            await c.report_error("boom", level="error", asset_name="Pump")

    await fail()  # online -> offline: reports
    await fail()  # still offline: silent
    await fail()  # still offline: silent
    await c.set_asset_status(asset, "online")  # recovery
    await fail()  # new outage: reports again
    assert c.ironflock.errors == [
        ("Pump: boom", "error"),
        ("Pump: boom", "error"),
    ]


async def test_report_error_prefixes_asset_and_forwards_level():
    c = _collector()
    await c.report_error("bad", level="warn", asset_name="A")
    await c.report_error("global", level="info")
    assert c.ironflock.errors == [("A: bad", "warn"), ("global", "info")]


async def test_report_error_threads_user_message_separately_from_technical_msg():
    c = _collector()
    await c.report_error(
        "ConnectionError: refused",
        level="error",
        asset_name="Pump",
        user_message="Pump is not responding",
    )
    # Technical text gets the asset prefix (-> msg); the friendly display line
    # passes through untouched (-> user_message).
    assert c.ironflock.errors == [("Pump: ConnectionError: refused", "error")]
    assert c.ironflock.user_messages == ["Pump is not responding"]


async def test_report_error_omits_user_message_for_sdk_fallback():
    c = _collector()
    await c.report_error("boom", asset_name="A")
    # None forwarded -> the SDK fills user_message = msg, so the toast still shows.
    assert c.ironflock.user_messages == [None]


async def test_report_error_never_raises_when_channel_down():
    c = _collector(error_raises=True)
    await c.report_error("boom", asset_name="A")  # must not raise


async def test_report_error_noop_without_handle():
    c = Collector(device_name="d", device_key=1, adapter=_Adapter())
    await c.report_error("boom")  # no ironflock set -> must not raise


async def test_store_report_error_reaches_the_channel():
    """Adapters report via the store handle from prepare_datapoints."""
    store = DatapointStore(FakeIF(), device_key=1, datapoints=[])
    await store.report_error("unparsable IODD", level="warn")
    assert store._ironflock.errors == [("unparsable IODD", "warn")]
