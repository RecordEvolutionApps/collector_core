# ironflock-collector-core

The shared, protocol-agnostic core of the IronFlock data collector app family
(MTConnect, Modbus, BACnet, IO-Link, OPC UA, Profinet).

It owns the IronFlock side of every collector: gateway settings, asset and
datapoint configuration with live table subscriptions, one collection task
per asset (polled or protocol-pushed), measurements assembly, asset
online/offline/paused state tracking, and data-write gating. Each collector
app contributes only a protocol adapter and its platform templates.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the core contract, the data model
and the per-protocol mapping. The canonical table structure every app must
provide in its backend ships as
[collector_core/data-template.yml](collector_core/data-template.yml).

## Usage in a collector app

```
# requirements.txt
ironflock-collector-core @ git+https://github.com/RecordEvolutionApps/collector_core.git@main
```

```python
from collector_core import Collector
from protocols.myprotocol import MyAdapter

collector = Collector(device_name=DEVICE_NAME, device_key=DEVICE_KEY, adapter=MyAdapter())
ironflock = IronFlock(mainFunc=collector.run)
collector.set_ironflock(ironflock)
ironflock.run()
```

## LICENSE

Copyright 2026 Record Evolution GmbH
All rights reserved
