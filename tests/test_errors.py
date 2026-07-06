"""Error hierarchy + typed reporting: user_message carriage, report_exception
derivation, and the on_error hook."""

from collector_core.adapter import ProtocolAdapter
from collector_core.collector import Collector
from collector_core.errors import (
    CollectorError,
    ConfigurationError,
    DeviceConnectionError,
    SettingsError,
)


class _Adapter(ProtocolAdapter):
    async def prepare_datapoints(self, asset, store):
        return []

    def create_session(self, asset):
        return None

    def format_value(self, asset, dp, raw):
        return raw


class FakeIF:
    def __init__(self):
        self.errors = []
        self.user_messages = []

    async def append_rows_to_table(self, table, rows):
        pass

    async def report_error(self, error, level="error", append=False, tsp=None, user_message=None):
        self.errors.append((error, level))
        self.user_messages.append(user_message)


def _collector(**kw):
    c = Collector(device_name="d", device_key=1, adapter=_Adapter(), **kw)
    c.set_ironflock(FakeIF())
    return c


# ------------------------------------------------------------------ hierarchy

def test_collector_error_carries_technical_and_user_message():
    e = DeviceConnectionError("connect refused", user_message="Pump is unreachable")
    assert str(e) == "connect refused"
    assert e.user_message == "Pump is unreachable"


def test_subclasses_supply_a_default_user_message():
    assert DeviceConnectionError("x").user_message == DeviceConnectionError.default_user_message
    assert ConfigurationError("x").user_message == ConfigurationError.default_user_message
    assert SettingsError("x").user_message == SettingsError.default_user_message


def test_base_collector_error_has_no_default_user_message():
    assert CollectorError("x").user_message is None


def test_datapoints_changed_is_not_a_collector_error():
    # Control signal must not be swallowed by `except CollectorError`.
    from collector_core.adapter import DatapointsChanged

    assert not issubclass(DatapointsChanged, CollectorError)


# --------------------------------------------------------------- report_exception

async def test_report_exception_prefers_typed_user_message_over_fallback():
    c = _collector()
    await c.report_exception(
        DeviceConnectionError("ConnectionRefused", user_message="Pump 1 is unreachable"),
        asset_name="Pump 1",
        fallback_user_message="generic fallback",
    )
    assert c.ironflock.errors == [("Pump 1: ConnectionRefused", "error")]
    assert c.ironflock.user_messages == ["Pump 1 is unreachable"]


async def test_report_exception_uses_subclass_default_when_none_given():
    c = _collector()
    await c.report_exception(ConfigurationError("bad spec"))
    assert c.ironflock.user_messages == [ConfigurationError.default_user_message]


async def test_report_exception_falls_back_for_untyped_exception():
    c = _collector()
    await c.report_exception(
        ValueError("boom"), asset_name="A", fallback_user_message="A is not responding"
    )
    assert c.ironflock.errors == [("A: boom", "error")]
    assert c.ironflock.user_messages == ["A is not responding"]


# ---------------------------------------------------------------- on_error hook

async def test_on_error_hook_replaces_builtin_report():
    calls = []

    def hook(message, *, level, user_message, asset_name):
        calls.append((message, level, user_message, asset_name))

    c = _collector(on_error=hook)
    await c.report_error("boom", level="warn", asset_name="A", user_message="friendly")

    assert calls == [("A: boom", "warn", "friendly", "A")]
    assert c.ironflock.errors == []  # built-in SDK report NOT used when a hook is set


async def test_on_error_hook_may_be_async():
    seen = []

    async def hook(message, *, level, user_message, asset_name):
        seen.append(message)

    c = _collector(on_error=hook)
    await c.report_exception(
        DeviceConnectionError("refused", user_message="X is unreachable"), asset_name="X"
    )
    assert seen == ["X: refused"]


async def test_on_error_hook_failure_never_propagates():
    def hook(message, *, level, user_message, asset_name):
        raise RuntimeError("hook is broken")

    c = _collector(on_error=hook)
    await c.report_error("boom")  # must not raise
