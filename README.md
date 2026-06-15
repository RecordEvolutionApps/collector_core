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

## Releasing a new version

There is no PyPI package: collector apps install the core straight from this Git
repo and pin a tag (see "Usage" above, but prefer a tag over `@main` for
reproducible builds):

```
ironflock-collector-core @ git+https://github.com/RecordEvolutionApps/collector_core.git@v2.3.0
```

So a release is a version bump committed on `main` plus a matching `vX.Y.Z` tag,
after which each consuming app re-pins to the new tag. Keep the Git tag and the
`pyproject.toml` `version` identical.

### Versioning (semver)

Judge `MAJOR.MINOR.PATCH` by the contract the core exposes to apps — the
`ProtocolAdapter` / `ProtocolSession` interface (`collector_core/adapter.py`),
the canonical table schema (`collector_core/data-template.yml`), and the
gateway/asset/datapoint columns the core reads:

- **PATCH** — internal fix, no contract change (e.g. a buffer-flush tweak).
- **MINOR** — backward-compatible addition (new optional adapter hook or column,
  a new opt-in capability).
- **MAJOR** — a breaking change apps must adapt to (renamed/removed adapter
  method, changed signature, newly required column, changed table shape).

### SDK version requirement

The core declares **zero dependencies** — the `ironflock` SDK handle is injected
by the app — but it does call methods on that handle. If a release starts using
a newer SDK method, record the new minimum here and in the release notes so apps
bump their own `ironflock` pin. **Current minimum: `ironflock >= 1.4.1`** (the
buffer flush uses `append_rows_to_table`).

### Steps

1. Pick the new version per semver above.
2. Bump `version` in `pyproject.toml`.
3. Update `README.md` / `ARCHITECTURE.md` if the contract changed; note the new
   minimum `ironflock` version if it changed.
4. Commit on `main`, e.g. `Bulk buffer flush (v2.3.0)`.
5. Tag and push (tag == `pyproject.toml` version):

   ```bash
   git tag v2.3.0
   git push origin main --tags
   ```

6. Re-pin each consuming app's `requirements.txt` to `@v2.3.0` (and bump its
   `ironflock` pin if this release raised the minimum), then redeploy.

### Re-pin the consuming apps

Every app that pins this repo — the collector family listed at the top
(`iolink_collector`, `modbus_collector`, `mtconnect_collector`,
`bacnet_collector`, `plc_collector`). An app keeps running on its old pinned tag
until you bump it, so apps can be migrated one at a time.

### Verify

In one app, install the new tag and smoke-test in demo mode before rolling out:

```bash
pip install --force-reinstall \
  "ironflock-collector-core @ git+https://github.com/RecordEvolutionApps/collector_core.git@v2.3.0"
```

## LICENSE

Copyright 2026 Record Evolution GmbH
All rights reserved
