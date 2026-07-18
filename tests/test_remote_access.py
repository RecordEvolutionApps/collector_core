"""Remote-access URL resolution: port specs -> gateway `url` column.

Covers the `ports` -> `getRemoteAccessUrlForPort` -> `url` JSON pipeline the
core runs at gateway registration so boards can embed a gateway's web UIs.
"""

import pytest

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
    """Records gateway appends and remote-access resolutions.

    ``getRemoteAccessUrlForPort`` mirrors the real (>=1.5.3) signature and
    returns ``url_map[port]`` (default None), recording every call so tests can
    assert the protocol / remote_port_environment passthrough.
    """

    def __init__(self, url_map=None, resolver_raises=False):
        self.appended = []
        self.calls = []
        self.url_map = url_map or {}
        self.resolver_raises = resolver_raises

    async def append_to_table(self, table, payload):
        self.appended.append((table, payload))

    def getRemoteAccessUrlForPort(self, port, protocol="http", remote_port_environment=None):
        self.calls.append((port, protocol, remote_port_environment))
        if self.resolver_raises:
            raise RuntimeError("tunnel not ready")
        return self.url_map.get(port)


class OldIF:
    """A pre-1.5.3 SDK handle without getRemoteAccessUrlForPort."""

    def __init__(self):
        self.appended = []

    async def append_to_table(self, table, payload):
        self.appended.append((table, payload))


def _collector(ports, fake):
    c = Collector(device_name="dev", device_key=42, adapter=_Adapter(), ports=ports)
    c.set_ironflock(fake)
    return c


def test_no_ports_by_default_is_empty():
    c = Collector(device_name="dev", device_key=1, adapter=_Adapter())
    assert c.ports == []


async def test_resolves_each_port_with_metadata():
    ports = [
        {"name": "Web interface", "port": 51821, "main": True},
        {"name": "Config API", "port": 8080, "protocol": "http"},
    ]
    fake = FakeIF(
        url_map={
            51821: "https://42-app-51821.app.ironflock.com",
            8080: "https://42-app-8080.app.ironflock.com",
        }
    )
    c = _collector(ports, fake)
    urls = c._resolve_remote_access_urls()
    assert urls == {
        "51821": {
            "name": "Web interface",
            "protocol": "http",
            "main": True,
            "url": "https://42-app-51821.app.ironflock.com",
        },
        "8080": {
            "name": "Config API",
            "protocol": "http",
            "main": False,
            "url": "https://42-app-8080.app.ironflock.com",
        },
    }


async def test_passes_protocol_and_remote_port_environment_through():
    ports = [
        {"name": "VPN", "port": 51820, "protocol": "UDP", "remote_port_environment": "WG_PORT"},
    ]
    fake = FakeIF(url_map={51820: "udp://app.ironflock.com:34567"})
    c = _collector(ports, fake)
    urls = c._resolve_remote_access_urls()
    # protocol is lowercased before the SDK call
    assert fake.calls == [(51820, "udp", "WG_PORT")]
    assert urls["51820"]["protocol"] == "udp"
    assert urls["51820"]["url"] == "udp://app.ironflock.com:34567"


async def test_defaults_name_protocol_main():
    c = _collector([{"port": 9000}], FakeIF())
    urls = c._resolve_remote_access_urls()
    assert set(urls) == {"9000"}  # keyed by the port number (string)
    entry = urls["9000"]
    assert entry["name"] == "9000"  # falls back to the port number
    assert entry["protocol"] == "http"
    assert entry["main"] is False
    assert entry["url"] is None  # not in url_map


async def test_unresolvable_port_keeps_entry_with_null_url():
    c = _collector([{"name": "raw", "port": 1883}], FakeIF(resolver_raises=True))
    urls = c._resolve_remote_access_urls()
    assert set(urls) == {"1883"}
    assert urls["1883"]["url"] is None
    assert urls["1883"]["name"] == "raw"


async def test_invalid_port_spec_is_skipped():
    ports = [{"name": "bad"}, {"name": "ok", "port": 80}]
    c = _collector(ports, FakeIF(url_map={80: "https://x"}))
    urls = c._resolve_remote_access_urls()
    assert list(urls) == ["80"]  # the bad spec is skipped, only port 80 keyed


async def test_old_sdk_without_resolver_yields_null_urls():
    c = _collector([{"name": "Web", "port": 80}], OldIF())
    urls = c._resolve_remote_access_urls()
    assert urls["80"]["url"] is None


async def test_register_gateway_writes_url_column():
    ports = [{"name": "Web interface", "port": 51821, "main": True}]
    fake = FakeIF(url_map={51821: "https://42-app-51821.app.ironflock.com"})
    c = _collector(ports, fake)
    await c.register_gateway()
    (table, payload) = fake.appended[-1]
    assert table == "gateways"
    assert payload["gateway_name"] == "dev"
    assert payload["url"] == {
        "51821": {
            "name": "Web interface",
            "protocol": "http",
            "main": True,
            "url": "https://42-app-51821.app.ironflock.com",
        }
    }


async def test_register_gateway_omits_url_when_no_ports():
    fake = FakeIF()
    c = Collector(device_name="dev", device_key=42, adapter=_Adapter())
    c.set_ironflock(fake)
    await c.register_gateway()
    (_, payload) = fake.appended[-1]
    assert "url" not in payload  # inert for apps that declare no ports


async def test_register_gateway_recomputes_url_not_echoes_stale():
    fake = FakeIF(url_map={80: "https://fresh"})
    c = _collector([{"name": "Web", "port": 80}], fake)
    # A stale url from a previously loaded gateway row must not survive.
    c.gateway = {"gateway_name": "dev", "url": {"80": {"name": "Web", "url": "https://stale"}}}
    await c.register_gateway()
    (_, payload) = fake.appended[-1]
    assert payload["url"] == {
        "80": {"name": "Web", "protocol": "http", "main": False, "url": "https://fresh"}
    }


# ------------------------------------------------------- load_ports_from_template


def test_load_ports_missing_file_returns_empty(tmp_path):
    missing = tmp_path / "nope.yml"
    assert Collector.load_ports_from_template(str(missing)) == []


def test_load_ports_from_template_parses_ports(tmp_path):
    pytest.importorskip("yaml")
    tpl = tmp_path / "port-template.yml"
    tpl.write_text(
        "ports:\n"
        "  - name: Web interface\n"
        "    port: 51821\n"
        "    main: true\n"
        "  - name: VPN\n"
        "    port: 51820\n"
        "    protocol: udp\n"
        "    remote_port_environment: WG_PORT\n"
    )
    ports = Collector.load_ports_from_template(str(tpl))
    assert ports == [
        {"name": "Web interface", "port": 51821, "main": True},
        {"name": "VPN", "port": 51820, "protocol": "udp", "remote_port_environment": "WG_PORT"},
    ]


def test_load_ports_from_template_no_ports_key(tmp_path):
    pytest.importorskip("yaml")
    tpl = tmp_path / "empty.yml"
    tpl.write_text("something_else: 1\n")
    assert Collector.load_ports_from_template(str(tpl)) == []
