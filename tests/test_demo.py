"""Per-datapoint demo signals: the walk, the boolean state, and the catalog merge."""

import random

from collector_core.adapter import (
    DEMO_DATAPOINT_COLUMNS,
    USER_DATAPOINT_COLUMNS,
    ProtocolAdapter,
    ProtocolSession,
)
from collector_core.collector import Collector
from collector_core.demo import DemoSignal


def dp(id="t", **extra):
    return {"id": id, "name": id, "units": "", **extra}


def samples(datapoint, n=40, seed=7):
    random.seed(seed)
    signal = DemoSignal()
    return [signal.value(datapoint) for _ in range(n)]


# --- numeric walk ----------------------------------------------------------

def test_unconfigured_datapoint_keeps_the_historical_range():
    values = samples(dp(), n=200)
    assert all(0.0 <= v <= 100.0 for v in values)
    assert len(set(values)) > 1  # it moves


def test_walk_stays_inside_expected_plus_minus_variance():
    values = samples(dp(demo_value=75, demo_variance=2), n=200)
    assert all(73.0 <= v <= 77.0 for v in values)


def test_first_sample_is_the_expected_value():
    assert samples(dp(demo_value=7.5, demo_variance=1), n=1) == [7.5]


def test_consecutive_samples_are_correlated_not_uniform():
    # The point of the walk: a step is a fraction of the band, so successive
    # readings sit close together. Uniform draws would average ~2/3 of it.
    values = samples(dp(demo_value=100, demo_variance=10), n=200)
    steps = [abs(b - a) for a, b in zip(values, values[1:])]
    assert max(steps) <= 10.0
    assert sum(steps) / len(steps) < 5.0


def test_variance_defaults_to_ten_percent_of_the_expected_value():
    assert all(90.0 <= v <= 110.0 for v in samples(dp(demo_value=100), n=200))


def test_zero_expected_value_still_wanders():
    values = samples(dp(demo_value=0), n=100)
    assert len(set(values)) > 1 and all(-1.0 <= v <= 1.0 for v in values)


def test_zero_variance_pins_the_value():
    assert set(samples(dp(demo_value=42, demo_variance=0), n=20)) == {42.0}


def test_signals_are_independent_per_datapoint():
    random.seed(1)
    signal = DemoSignal()
    for _ in range(20):
        signal.value(dp("a", demo_value=10, demo_variance=1))
        signal.value(dp("b", demo_value=500, demo_variance=5))
    assert 9.0 <= signal.value(dp("a", demo_value=10, demo_variance=1)) <= 11.0
    assert 495.0 <= signal.value(dp("b", demo_value=500, demo_variance=5)) <= 505.0


# --- coercion --------------------------------------------------------------

def test_string_settings_from_the_table_are_parsed():
    assert all(73.0 <= v <= 77.0 for v in samples(dp(demo_value="75", demo_variance="2"), n=50))


def test_unusable_settings_fall_back_instead_of_raising():
    # a typo in one datapoint must not take the asset's demo stream down
    assert all(0.0 <= v <= 100.0 for v in samples(dp(demo_value="warm"), n=50))
    assert all(90.0 <= v <= 110.0 for v in samples(dp(demo_value=100, demo_variance="lots"), n=50))
    assert all(90.0 <= v <= 110.0 for v in samples(dp(demo_value=100, demo_variance=-5), n=50))


# --- boolean state ---------------------------------------------------------

def test_boolean_rests_in_the_expected_state():
    values = samples(dp(demo_value=True, demo_variance=0.05), n=400)
    assert all(isinstance(v, bool) for v in values)
    assert values[0] is True
    assert 0.75 < values.count(True) / len(values) < 1.0  # mostly on, but it blips


def test_boolean_false_rests_off_and_accepts_table_strings():
    values = samples(dp(demo_value="false"), n=200)
    assert values[0] is False and values.count(False) > values.count(True)


def test_boolean_never_flips_with_zero_variance():
    assert set(samples(dp(demo_value=True, demo_variance=0), n=50)) == {True}


# --- session default -------------------------------------------------------

class _Session(ProtocolSession):
    pass


def test_session_default_emits_one_scalar_per_datapoint():
    random.seed(3)
    session = _Session()
    raw = session.demo_values([dp("a", demo_value=5, demo_variance=0),
                               dp("b", demo_value=True), {"name": "no id"}])
    assert raw == {"a": 5.0, "b": True}


def test_session_reuses_one_signal_so_the_walk_continues():
    session = _Session()
    assert session.demo_signal is session.demo_signal


# --- catalog merge ---------------------------------------------------------

class _Adapter(ProtocolAdapter):
    async def prepare_datapoints(self, asset, store):
        return []

    def create_session(self, asset):
        return None

    def format_value(self, asset, datapoint, raw):
        return raw


def _collector(rows):
    c = Collector(device_name="d", device_key=1, adapter=_Adapter())
    c.datapoints[:] = rows
    return c


def test_catalog_demo_settings_reach_the_datapoints():
    c = _collector([{"asset_name": "a1", "datapoint_id": "t",
                     "demo_value": "75", "demo_variance": "2"}])
    assert c._with_demo_config("a1", [dp("t")]) == [dp("t", demo_value="75", demo_variance="2")]


def test_catalog_row_wins_over_the_adapters_resolved_value():
    c = _collector([{"asset_name": "a1", "datapoint_id": "t", "demo_value": "80"}])
    merged = c._with_demo_config("a1", [dp("t", demo_value=75, demo_variance=2)])
    # board edit wins; the variance the adapter resolved is untouched
    assert merged[0]["demo_value"] == "80" and merged[0]["demo_variance"] == 2


def test_unconfigured_datapoints_pass_through_untouched():
    c = _collector([{"asset_name": "a1", "datapoint_id": "t", "demo_value": None}])
    original = dp("t")
    merged = c._with_demo_config("a1", [original, dp("other")])
    assert merged[0] is original and merged[1] is not None
    c2 = _collector([])
    assert c2._with_demo_config("a1", [original])[0] is original


# --- contract --------------------------------------------------------------

def test_demo_columns_are_preserved_across_rediscovery():
    # they are user-owned: the store must carry them over when an adapter
    # rewrites the catalog without them
    assert set(DEMO_DATAPOINT_COLUMNS) <= set(USER_DATAPOINT_COLUMNS)
