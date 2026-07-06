"""Protocol-agnostic collector core for IronFlock data collector apps.

See ARCHITECTURE.md for the full core contract and data model.
"""

from collector_core.adapter import (
    PLATFORM_COLUMNS,
    DatapointsChanged,
    DatapointStore,
    ProtocolAdapter,
    ProtocolSession,
)
from collector_core.collector import Collector
from collector_core.errors import (
    CollectorError,
    ConfigurationError,
    DeviceConnectionError,
    DeviceReadError,
    SettingsError,
)

__all__ = [
    "Collector",
    "ProtocolAdapter",
    "ProtocolSession",
    "DatapointStore",
    "DatapointsChanged",
    "PLATFORM_COLUMNS",
    # Error hierarchy — adapters raise these; the core reports them.
    "CollectorError",
    "ConfigurationError",
    "DeviceConnectionError",
    "DeviceReadError",
    "SettingsError",
]
