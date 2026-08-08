"""Synthetic value generation for demo mode.

Demo mode exists so an operator can check names, units, dashboards and alarm
limits before the device is reachable. Uniform noise proves the plumbing works
but says nothing about the dashboard: a temperature that jumps 0..100 every
cycle reads as broken, and no axis, threshold or unit can be judged against it.

Two per-datapoint columns fix that — ``demo_value`` (what the reading should
sit at) and ``demo_variance`` (how far it may wander) — and this module turns
them into a signal shaped like a real process reading. The walk is
mean-reverting: each sample steps from the previous one and is pulled back
towards the expected value, so consecutive readings are correlated (a wandering
line on a chart, not a hairbrush) and never leave the band.

Values are in ENGINEERING units — what the board displays. An adapter must not
apply its raw-to-engineering ``scale`` on top of them.
"""

import random

# Fraction of the band one sample may step: small enough that crossing the band
# takes several cycles (visibly smooth), large enough that a chart moves between
# two polls.
_STEP = 0.25
# Pull back towards the expected value per sample. Without it, a walk that is
# merely clamped spends most of its time pinned to the edges of the band.
_PULL = 0.15
# An unconfigured datapoint keeps demo mode's historical 0..100 range, so
# turning demo mode on without configuring anything behaves as it always did.
_DEFAULT_EXPECTED = 50.0
_DEFAULT_VARIANCE = 50.0
# A configured expected value with no variance wanders +-10% of it...
_RELATIVE_VARIANCE = 0.1
# ...or by this much when the expected value is 0, where a relative band would
# be a flat line.
_ZERO_VARIANCE = 1.0
# Chance per sample that a boolean resting in its expected state blips out of
# it, and the chance per sample that it returns (mean excursion: 2 samples).
_DEFAULT_FLIP = 0.05
_RETURN = 0.5


def _as_number(value):
    """Coerce a spec/table value to float, or None if it is not a number.

    Numeric table columns come back as float, int or string depending on the
    platform, so parse rather than type-check. Booleans are excluded on
    purpose: ``True`` is an int in Python and would otherwise read as 1.0.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value):
    """The boolean a value denotes, or None if it does not denote one."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    return None


class DemoSignal:
    """Synthetic readings for demo mode, one independent signal per datapoint.

    Holds the walk state, so create ONE per session (or per driver) and keep it
    for that session's lifetime — a fresh instance restarts every signal at its
    expected value.

    ``value(datapoint)`` reads the datapoint's ``demo_value`` /
    ``demo_variance`` and returns the next reading:

    - numeric ``demo_value`` — a float wandering within ``demo_value`` +-
      ``demo_variance`` (variance 0 pins it to a constant)
    - boolean ``demo_value`` — a bool resting in that state, blipping out of it
      with probability ``demo_variance`` per sample
    - neither configured — a float wandering in 0..100, demo mode's historical
      behaviour

    Unparsable or out-of-range settings fall back to the defaults rather than
    raising: demo mode is a convenience, and a typo in one datapoint must not
    take an asset's whole demo stream down.
    """

    def __init__(self):
        self._state = {}

    def value(self, datapoint):
        """Next synthetic reading for one datapoint dict."""
        key = datapoint.get("id")
        expected = datapoint.get("demo_value")
        variance = _as_number(datapoint.get("demo_variance"))
        boolean = _as_bool(expected)
        if boolean is not None:
            return self._boolean(key, boolean, variance)
        return self._numeric(key, _as_number(expected), variance)

    def _numeric(self, key, expected, variance):
        if expected is None:
            expected, fallback = _DEFAULT_EXPECTED, _DEFAULT_VARIANCE
        else:
            fallback = abs(expected) * _RELATIVE_VARIANCE or _ZERO_VARIANCE
        if variance is None or variance < 0:
            variance = fallback

        last = self._state.get(key)
        if not isinstance(last, float):
            # First sample (or a datapoint reconfigured from boolean): start
            # clean at the expected value and wander from there.
            self._state[key] = expected
            return round(expected, 4)

        step = random.uniform(-1.0, 1.0) * variance * _STEP
        drift = (expected - last) * _PULL
        value = min(max(last + step + drift, expected - variance), expected + variance)
        self._state[key] = value
        return round(value, 4)

    def _boolean(self, key, expected, flip_chance):
        if flip_chance is None or not 0.0 <= flip_chance <= 1.0:
            flip_chance = _DEFAULT_FLIP

        last = self._state.get(key)
        if not isinstance(last, bool):
            self._state[key] = expected
            return expected

        # Rest in the expected state and blip out of it briefly, rather than
        # flipping symmetrically — a "machine running" tag that toggles half
        # the time is exactly the unreadable demo this module replaces.
        if last == expected:
            value = (not last) if random.random() < flip_chance else last
        else:
            value = expected if random.random() < _RETURN else last
        self._state[key] = value
        return value
