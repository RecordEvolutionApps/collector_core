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

__all__ = [
    "Collector",
    "ProtocolAdapter",
    "ProtocolSession",
    "DatapointStore",
    "DatapointsChanged",
    "PLATFORM_COLUMNS",
]
