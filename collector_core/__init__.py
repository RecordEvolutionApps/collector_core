"""Protocol-agnostic collector core for IronFlock data collector apps.

See ARCHITECTURE.md for the full core contract and data model.
"""

from collector_core.adapter import (
    DEMO_DATAPOINT_COLUMNS,
    PLATFORM_COLUMNS,
    USER_DATAPOINT_COLUMNS,
    DatapointsChanged,
    DatapointStore,
    ProtocolAdapter,
    ProtocolSession,
)
from collector_core.collector import Collector
from collector_core.demo import DemoSignal
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
    # Demo mode — adapters with a non-scalar raw shape drive their own
    # DemoSignal; the columns are the per-datapoint contract behind it.
    "DemoSignal",
    "PLATFORM_COLUMNS",
    "USER_DATAPOINT_COLUMNS",
    "DEMO_DATAPOINT_COLUMNS",
    # Error hierarchy — adapters raise these; the core reports them.
    "CollectorError",
    "ConfigurationError",
    "DeviceConnectionError",
    "DeviceReadError",
    "SettingsError",
]
