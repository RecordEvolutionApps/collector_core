"""Meaningful, catchable error types for the collector core.

Every operational failure a collector surfaces to the operator is a
``CollectorError``. Beyond the technical message (``str(exc)``) each carries a
``user_message`` — a short, plain-language line for the board's error toast.

Protocol adapters raise these (see the ``ProtocolAdapter`` / ``ProtocolSession``
contract in ``collector_core.adapter``) so the core can report both the
technical detail and a friendly display line via the SDK's ``report_error``
(``msg`` = technical, ``user_message`` = display). Implementations may also
catch them in their own code.

The core catches ``CollectorError`` at its boundaries and reports it with the
carried ``user_message`` (``Collector.report_exception``); an unexpected
non-``CollectorError`` is still reported, using a per-site fallback message.

``DatapointsChanged`` (in ``collector_core.adapter``) is deliberately NOT a
``CollectorError``: it is a control signal for datapoint re-discovery, not a
reportable failure, so ``except CollectorError`` never swallows it.
"""


class CollectorError(Exception):
    """Base class for reportable collector failures.

    Args:
        message: technical error text — goes to the error-logs ``msg`` column
            and the logs.
        user_message: operator-facing display line for the toast. Falls back to
            the subclass ``default_user_message`` when omitted. Keep it short,
            plain-language and self-contained (name the asset/device).
    """

    # Operator-facing default used when a raiser passes no ``user_message``.
    # The base class has none (a bare CollectorError carries only the technical
    # message); subclasses set a sensible sentence.
    default_user_message = None

    def __init__(self, message, *, user_message=None):
        super().__init__(message)
        self.user_message = user_message or self.default_user_message


class ConfigurationError(CollectorError):
    """The asset/datapoint configuration is invalid — bad ``datapoint_spec``,
    a missing required field, an unknown profile. Collection cannot proceed
    until the user fixes the configuration."""

    default_user_message = "The configuration is invalid — check the asset settings."


class DeviceConnectionError(CollectorError):
    """The device could not be reached or connected to — unreachable host,
    refused connection, or timeout before any data was read."""

    default_user_message = "The device could not be reached — check the network and address."


class DeviceReadError(CollectorError):
    """A connection was established but reading or decoding the response failed
    — an unparsable payload, an unexpected reply, a protocol error mid-read."""

    default_user_message = "Reading data from the device failed."


class SettingsError(CollectorError):
    """Gateway-level settings could not be applied; the collector falls back to
    the adapter's built-in defaults."""

    default_user_message = "Gateway settings could not be applied; using defaults."
