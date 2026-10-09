"""Make a row something the platform can carry, before it enters the buffer.

Every write reaches the platform through the SDK's WAMP session, which
serializes with msgpack, and lands in Postgres. A few Python values survive
neither: a ``str`` holding a lone surrogate (``json.loads`` produces one from
an escape like ``"\\ud800"``, so any device or broker can send it) cannot be
encoded as UTF-8; an ``int`` outside msgpack's 64-bit range raises; a set or
an arbitrary object has no encoding at all; and NaN/Infinity have no JSON
form. Such a row does not fail alone. The core buffers rows in one deque
shared by every asset and re-queues an unsent chunk at its head, so a single
unencodable value used to block every asset on the gateway until restart.

The rule here is to repair, not to drop: one odd datapoint must not cost the
rest of its measurements row. Strings keep their content with the bad code
points escaped, non-finite floats become ``None`` (what JSON would make of
them anyway), out-of-range integers and unknown types become text. The
caller learns what was repaired so it can tell the operator.
"""

import math
from datetime import date, datetime, time

# msgpack's integer range: int64 below zero, uint64 above.
_INT_MIN = -(2**63)
_INT_MAX = 2**64 - 1

_SCALARS = (bool, type(None))


def repair_text(text):
    """``text`` with every lone surrogate escaped (``\\ud800``), so it encodes as UTF-8."""
    try:
        text.encode("utf-8")
        return text
    except UnicodeEncodeError:
        return text.encode("utf-8", "backslashreplace").decode("utf-8")


def _problem(value):
    """Why a scalar cannot be stored as it is, or None when it can."""
    if isinstance(value, _SCALARS):
        return None
    if isinstance(value, str):
        try:
            value.encode("utf-8")
            return None
        except UnicodeEncodeError:
            return "text with an unencodable character"
    if isinstance(value, int):
        return None if _INT_MIN <= value <= _INT_MAX else "an integer out of range"
    if isinstance(value, float):
        return None if math.isfinite(value) else "a non-finite number"
    return f"a value of type {type(value).__name__}"


def _repair(value):
    if isinstance(value, str):
        return repair_text(value)
    if isinstance(value, float):
        return None
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return repair_text(str(value))


def storable(value):
    """Return ``(value, problems)``: ``value`` made storable, and what was repaired.

    Walks dicts, lists and tuples iteratively, so a deeply nested payload
    cannot hit the recursion limit. When nothing needs repair the original
    object is returned untouched (no copy), which is the common case on the
    hot path. ``problems`` is a sorted list of short, distinct descriptions.
    """
    problems = set()
    if not _needs_repair(value, problems):
        return value, []
    return _rebuild(value), sorted(problems)


def _needs_repair(value, problems):
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    problems.add("a non-text key")
                elif _problem(key):
                    problems.add("a key with an unencodable character")
                stack.append(child)
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
        else:
            problem = _problem(item)
            if problem:
                problems.add(problem)
    return bool(problems)


def _rebuild(value):
    # Iterative copy: containers are created first and filled as their
    # children are visited, so depth costs heap, not stack.
    def fresh(item):
        if isinstance(item, dict):
            return {}
        if isinstance(item, (list, tuple)):
            return []
        return item if _problem(item) is None else _repair(item)

    root = fresh(value)
    stack = [(value, root)]
    while stack:
        source, target = stack.pop()
        if isinstance(source, dict):
            for key, child in source.items():
                key = repair_text(key) if isinstance(key, str) else repair_text(str(key))
                copy = fresh(child)
                target[key] = copy
                if isinstance(child, (dict, list, tuple)):
                    stack.append((child, copy))
        elif isinstance(source, (list, tuple)):
            for child in source:
                copy = fresh(child)
                target.append(copy)
                if isinstance(child, (dict, list, tuple)):
                    stack.append((child, copy))
    return root
