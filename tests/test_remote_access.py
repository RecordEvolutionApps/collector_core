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


async def test_resolves_each_port_with_metadata(monkeypatch):
    for var in ("DEVICE_KEY", "APP_NAME", "INSTANCE_KEY", "DEVICE_LAN_IP"):
        monkeypatch.delenv(var, raising=False)
    ports = [
        {"name": "Web interface", "port": 51821, "main": True},
        {"name": "Config API", "port": 8080, "protocol": "http"},
    ]
    c = _collector(ports, FakeIF())
    urls = c._resolve_remote_access_urls()
    # No SDK URL and no LAN env -> metadata only; there is deliberately no
    # ambient `url` field, consumers pick a scope (local/cloud).
    assert urls == {
        "51821": {"name": "Web interface", "protocol": "http", "main": True},
        "8080": {"name": "Config API", "protocol": "http", "main": False},
    }


async def test_protocol_is_lowercased():
    c = _collector(
        [{"name": "VPN", "port": 51820, "protocol": "UDP", "remote_port_environment": "WG_PORT"}],
        FakeIF(),
    )
    assert c._resolve_remote_access_urls()["51820"]["protocol"] == "udp"


async def test_defaults_name_protocol_main():
    c = _collector([{"port": 9000}], FakeIF())
    urls = c._resolve_remote_access_urls()
    assert set(urls) == {"9000"}  # keyed by the port number (string)
    entry = urls["9000"]
    assert entry["name"] == "9000"  # falls back to the port number
    assert entry["protocol"] == "http"
    assert entry["main"] is False
    assert "url" not in entry  # legacy field removed in 2.7.0


async def test_a_failing_sdk_resolver_leaves_the_cloud_scope_out():
    fake = FakeIF(resolver_raises=True)
    c = _collector([{"name": "raw", "port": 1883}], fake)
    urls = c._resolve_remote_access_urls()
    assert urls["1883"]["name"] == "raw"
    assert "cloud" not in urls["1883"]
    assert fake.calls == [(1883, "http", None)]


async def test_invalid_port_spec_is_skipped():
    ports = [{"name": "bad"}, {"name": "ok", "port": 80}]
    c = _collector(ports, FakeIF())
    urls = c._resolve_remote_access_urls()
    assert list(urls) == ["80"]  # the bad spec is skipped, only port 80 keyed


async def test_works_with_old_sdk_without_resolver():
    c = _collector([{"name": "Web", "port": 80}], OldIF())
    urls = c._resolve_remote_access_urls()
    assert urls["80"]["name"] == "Web"


async def test_register_gateway_writes_url_column(monkeypatch):
    monkeypatch.delenv("DEVICE_LAN_IP", raising=False)
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
            "cloud": {"url": "https://42-app-51821.app.ironflock.com"},
        }
    }


async def test_register_gateway_omits_url_when_no_ports():
    fake = FakeIF()
    c = Collector(device_name="dev", device_key=42, adapter=_Adapter())
    c.set_ironflock(fake)
    await c.register_gateway()
    (_, payload) = fake.appended[-1]
    assert "url" not in payload  # inert for apps that declare no ports


async def test_register_gateway_computes_url_from_the_environment(monkeypatch):
    for var in ("DEVICE_KEY", "APP_NAME", "INSTANCE_KEY", "DEVICE_LAN_IP"):
        monkeypatch.delenv(var, raising=False)
    fake = FakeIF()
    c = _collector([{"name": "Web", "port": 80}], fake)
    c.gateway_extra = {"url": {"80": {"name": "Web", "url": "https://stale"}}}
    await c.register_gateway()
    (_, payload) = fake.appended[-1]
    assert payload["url"] == {"80": {"name": "Web", "protocol": "http", "main": False}}


# ------------------------------------------------------------ registration row


async def test_the_registration_row_is_keyed_by_the_device_key():
    fake = FakeIF()
    c = Collector(device_name="dev", device_key=42, adapter=_Adapter())
    c.set_ironflock(fake)
    await c.register_gateway()
    (table, payload) = fake.appended[-1]
    assert table == "gateways"
    assert set(payload) == {"tsp", "gateway_id", "gateway_name", "info", "deleted"}
    assert payload["gateway_id"] == 42
    assert payload["gateway_name"] == "dev"
    assert payload["deleted"] is False
    assert set(payload["info"]) == {"app_started", "python"}


async def test_the_registration_row_echoes_nothing_and_carries_no_settings():
    fake = FakeIF()
    c = Collector(device_name="dev", device_key=42, adapter=_Adapter())
    c.set_ironflock(fake)
    # settings the board wrote stay in gateway_settings; nothing is copied over
    c.settings = {"tsp": 1, "gateway_id": 42, "store_data": False, "buffer_size": 5}
    await c.register_gateway()
    (_, payload) = fake.appended[-1]
    assert not {"store_data", "buffer_size", "latest_flag", "device_key"} & set(payload)
    assert not hasattr(c, "gateway")


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


# ------------------------------------------------- local (LAN) endpoint info


async def test_local_endpoint_added_when_platform_env_present(monkeypatch):
    monkeypatch.setenv("DEVICE_LAN_IP", "192.168.0.21")
    monkeypatch.setenv("DEVICE_PORT_FOR_50051", "40007")
    c = _collector([{"name": "Control", "port": 50051, "protocol": "tcp"}], FakeIF())
    urls = c._resolve_remote_access_urls()
    assert urls["50051"]["local"] == {
        "ip": "192.168.0.21",
        "port": 40007,
        "url": "tcp://192.168.0.21:40007",
    }


async def test_local_url_uses_http_scheme_for_web_ports(monkeypatch):
    monkeypatch.setenv("DEVICE_LAN_IP", "192.168.0.21")
    monkeypatch.setenv("DEVICE_PORT_FOR_55000", "41002")
    c = _collector([{"name": "Web", "port": 55000}], FakeIF())
    local = c._resolve_remote_access_urls()["55000"]["local"]
    assert local["url"] == "http://192.168.0.21:41002"  # iframe-ready


async def test_local_endpoint_omitted_without_env(monkeypatch):
    monkeypatch.delenv("DEVICE_LAN_IP", raising=False)
    monkeypatch.delenv("DEVICE_PORT_FOR_50051", raising=False)
    c = _collector([{"name": "Control", "port": 50051}], FakeIF())
    assert "local" not in c._resolve_remote_access_urls()["50051"]


async def test_local_endpoint_omitted_without_mapped_port(monkeypatch):
    monkeypatch.setenv("DEVICE_LAN_IP", "192.168.0.21")
    monkeypatch.delenv("DEVICE_PORT_FOR_8080", raising=False)
    c = _collector([{"name": "Web", "port": 8080}], FakeIF())
    assert "local" not in c._resolve_remote_access_urls()["8080"]


async def test_local_endpoint_ignores_non_numeric_mapped_port(monkeypatch):
    monkeypatch.setenv("DEVICE_LAN_IP", "192.168.0.21")
    monkeypatch.setenv("DEVICE_PORT_FOR_80", "not-a-port")
    c = _collector([{"name": "Web", "port": 80}], FakeIF())
    assert "local" not in c._resolve_remote_access_urls()["80"]


# --------------------------------------------------------------- gateway_extra


async def test_register_gateway_merges_gateway_extra():
    fake = FakeIF()
    c = Collector(device_name="dev", device_key=42, adapter=_Adapter())
    c.set_ironflock(fake)
    c.gateway_extra = {"qds_role": "ui"}
    await c.register_gateway()
    (_, payload) = fake.appended[-1]
    assert payload["qds_role"] == "ui"


async def test_gateway_extra_never_overrides_core_columns():
    fake = FakeIF()
    c = Collector(device_name="dev", device_key=42, adapter=_Adapter())
    c.set_ironflock(fake)
    c.gateway_extra = {"gateway_name": "impostor", "gateway_id": 7, "deleted": True,
                       "info": "x", "url": "x", "qds_role": "backend"}
    await c.register_gateway()
    (_, payload) = fake.appended[-1]
    assert payload["gateway_name"] == "dev"
    assert payload["gateway_id"] == 42
    assert payload["deleted"] is False
    assert isinstance(payload["info"], dict) and "url" not in payload
    assert payload["qds_role"] == "backend"


# ----------------------------------------------- cloud scope: the SDK's URL
#
# The tunnel URL is the ironflock SDK's to derive (label, secure- prefix,
# -dev suffix of a development run, instance route, domains, announced remote
# ports). The core passes the port spec through and stores the answer as is.


async def test_cloud_scope_is_the_url_the_sdk_returns():
    fake = FakeIF(url_map={8080: "https://i7-42-myapp-8080-dev.app.ironflock.com"})
    c = _collector([{"name": "Web", "port": 8080}], fake)
    entry = c._resolve_remote_access_urls()["8080"]
    assert entry["cloud"] == {"url": "https://i7-42-myapp-8080-dev.app.ironflock.com"}
    assert fake.calls == [(8080, "http", None)]


async def test_protocol_and_remote_port_environment_go_to_the_sdk():
    fake = FakeIF(url_map={1883: "tcp://app.ironflock.com:34567"})
    c = _collector(
        [{"name": "mqtt", "port": 1883, "protocol": "TCP", "remote_port_environment": "MQTT_PORT"}],
        fake,
    )
    entry = c._resolve_remote_access_urls()["1883"]
    assert entry["cloud"] == {"url": "tcp://app.ironflock.com:34567"}
    assert fake.calls == [(1883, "tcp", "MQTT_PORT")]


async def test_no_cloud_scope_while_the_sdk_knows_no_url():
    c = _collector([{"name": "Web", "port": 8080}], FakeIF())
    assert "cloud" not in c._resolve_remote_access_urls()["8080"]


async def test_the_core_composes_no_tunnel_url_of_its_own(monkeypatch):
    """Platform env alone produces nothing: there is no appliance scope, and
    no cloud URL the SDK did not hand over."""
    monkeypatch.setenv("DEVICE_KEY", "42")
    monkeypatch.setenv("APP_NAME", "MyApp")
    monkeypatch.setenv("INSTANCE_KEY", "7")
    monkeypatch.setenv("TUNNEL_DOMAIN", "tunnel.appliance.lan")
    monkeypatch.setenv("MQTT_PORT", "34567")
    monkeypatch.delenv("DEVICE_LAN_IP", raising=False)
    c = _collector(
        [{"name": "Web", "port": 8080},
         {"name": "mqtt", "port": 1883, "protocol": "tcp", "remote_port_environment": "MQTT_PORT"}],
        OldIF(),
    )
    urls = c._resolve_remote_access_urls()
    for entry in urls.values():
        assert "cloud" not in entry and "appliance" not in entry


@pytest.mark.parametrize("env, expected", [
    ("DEV", "https://42-myapp-5000-dev.app.ironflock.com"),
    ("PROD", "https://42-myapp-5000.app.ironflock.com"),
])
async def test_a_development_run_gets_its_own_dev_tunnel_from_the_real_sdk(monkeypatch, env, expected):
    """End to end with the real SDK, where it is installed (it is not a
    dependency of the core): a DEV run's gateway URL carries the -dev suffix
    of its own tunnel, a PROD install's does not (SDK >= 1.11.1)."""
    ironflock = pytest.importorskip("ironflock")
    from importlib.metadata import version
    if tuple(int(p) for p in version("ironflock").split(".")[:3]) < (1, 11, 1):
        pytest.skip("the -dev suffix needs ironflock >= 1.11.1")
    for var in ("INSTANCE_KEY", "TUNNEL_DOMAIN", "CLOUD_TUNNEL_DOMAIN"):
        monkeypatch.delenv(var, raising=False)
    for var, value in (("DEVICE_SERIAL_NUMBER", "SN1"), ("DEVICE_KEY", "42"),
                       ("APP_NAME", "MyApp"), ("SWARM_KEY", "1"), ("APP_KEY", "1"),
                       ("ENV", env)):
        monkeypatch.setenv(var, value)
    c = _collector([{"name": "QDS", "port": 5000}], ironflock.IronFlock())
    assert c._resolve_remote_access_urls()["5000"]["cloud"] == {"url": expected}
