# ironflock-collector-core

The shared, protocol-agnostic core of the IronFlock data collector app family
(MTConnect, Modbus, BACnet, IO-Link, OPC UA, Profinet).

It owns the IronFlock side of every collector: the gateway's registration,
following the users' gateway settings, assets and datapoint settings live,
one collection task per asset (polled or protocol-pushed), the datapoints
catalog, measurements assembly, asset online/offline/paused state tracking,
data-write gating, and refreshing the app's transforms after its writes. Each
collector app contributes only a protocol adapter and its platform templates.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the core contract, the data model
and the per-protocol mapping. The canonical table structure every app must
provide in its backend ships as
[collector_core/data-template.yml](collector_core/data-template.yml).

## Tables

Seven tables, each with one writer (`gateway_id` is the gateway's
`DEVICE_KEY` everywhere):

| Table | Written by |
|---|---|
| `gateway_registry` | the gateway about itself, at startup; the board writes only `{gateway_id, deleted: true}` |
| `gateway_settings` | the board (per-gateway settings: `store_data`, `buffer_size`, app columns) |
| `assets` | the board's forms and the AI assistant (config_core) |
| `datapoints` | the gateway (the catalog) |
| `datapoint_settings` | the board and the AI assistant (`enabled`, `change_detection`, demo tuning, `configured_by`) |
| `measurements` | the gateway |
| `assetstatus` | the gateway |

## Usage in a collector app

```
# requirements.txt
ironflock-collector-core @ git+https://github.com/RecordEvolutionApps/collector_core.git@main
```

```python
from collector_core import Collector
from protocols.myprotocol import MyAdapter

collector = Collector(
    device_name=DEVICE_NAME,
    device_key=DEVICE_KEY,
    adapter=MyAdapter(),
    # optional: transforms to refresh after the core writes or applies a
    # table, per table in bottom-up order (needs ironflock >= 1.11.0)
    refresh_transforms={
        "assetstatus": ["asset_status_counts", "asset_overview"],
        "assets": ["asset_overview"],
        "gateway_registry": ["gateway_overview"],
    },
)
ironflock = IronFlock(mainFunc=collector.run)
collector.set_ironflock(ironflock)
ironflock.run()
```

Constructor arguments: `device_name`, `device_key`, `adapter`, `store_data`
(default when `gateway_settings` sets none), `on_error`, `ports` (remote-access
ports for the gateway's `url` column), `refresh_transforms`. An adapter that
writes a table of its own maps it in `refresh_transforms` and calls
`collector.request_refresh(table)` after writing.

## Migrating from v2

v3 splits the users' settings out of the tables the gateway writes. In the
app:

1. **Data template.** Take the seven tables from
   [collector_core/data-template.yml](collector_core/data-template.yml):
   - add `gateway_registry` (identity `maintainLatestFlagFor:
     [gateway_id]`), which replaces v2's `gateways`. IronFlock refuses to
     change the entity key of an existing table, and v2's `gateways` is keyed
     by `gateway_name`, so the registry is a new table. Keep v2's
     `gateways` table in the template unchanged; the core no longer writes
     it, and the board no longer reads it.
   - `assets`: remove `dropAfter` (nothing rewrites an unchanged asset, so
     its row must not age out).
   - add `gateway_settings` (`gateway_id`, `store_data`, `buffer_size`) and
     move the app's own per-gateway settings columns there (e.g.
     `iodd_autofetch`).
   - `datapoints`: drop `enabled` and `change_detection`; `demo_value` /
     `demo_variance` stay as optional defaults the adapter derives.
   - add `datapoint_settings` (`gateway_id`, `asset_name`, `datapoint_id`,
     `enabled`, `change_detection`, `demo_value`, `demo_variance`,
     `configured_by`), without `dropAfter`.
2. **Board.** The gateway settings form writes `gateway_settings` with the
   gateway's `gateway_id`. The gateway delete form writes `{gateway_id,
   deleted: true}` to `gateway_registry`. The datapoint form writes
   `datapoint_settings` (`configured_by: "board"`); the AI assistant writes
   the same table with `configured_by: "agent"`. Bindings that read
   `gateways` read `gateway_registry` and join on `gateway_id`, not
   `gateway_name`.
3. **Code.** Pass `refresh_transforms` for the transforms the board shows.
   `collector.gateway` is now `collector.settings` (the `gateway_settings`
   row); `apply_settings` receives that row. `Collector.soft_delete_asset`,
   `Collector.load_gateway` and `USER_DATAPOINT_COLUMNS` are gone
   (`DATAPOINT_SETTINGS_COLUMNS` names the columns the core reads).
   Incomplete asset rows are no longer deleted by the gateway: the user
   deletes them on the board.
4. **Data.** Existing per-gateway settings revert to their defaults once:
   they lived in `gateways` keyed by `gateway_name`, and the core reads
   `gateway_settings` keyed by `gateway_id`. Per-datapoint switches set in v2
   lived in the catalog and revert the same way unless the app copies them
   into `datapoint_settings`.

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
bump their own `ironflock` pin. **Current minimum: `ironflock >= 1.10.0`.**
Apps that pass **`refresh_transforms`** need **`ironflock >= 1.11.0`** for
`refresh_transform`; with an older SDK the core refreshes nothing (it says so
once) and the transforms follow their schedule.
From 1.9.0 the SDK rides out a platform restart in its table writes: a write
waits up to the SDK's `reconnect_window` (60 s) for the connection and is
retried while the data backend is not serving yet, so a router or data-backend
restart no longer counts as an outage here. 1.10.0 is the version the core is
tested against. (The core still runs on `ironflock >= 1.4.1`, the first with
`append_rows_to_table`, and `>= 1.5.3` for apps that pass `ports=`.) Apps that
pass `ports=` and run development installs need **`ironflock >= 1.11.1`**: the
gateway `url` column's `cloud` URL comes straight from the SDK's
`getRemoteAccessUrlForPort`, and only 1.11.1 gives a development run its own
`-dev` tunnel URL.

The SDK does not hold rows beyond that window: a write still failing after it
raises, and a write in flight when the link drops fails at once. The core's own
bounded buffer (`gateway_settings.buffer_size`) therefore stays the store for longer
outages, and the outage report on recovery comes from it.

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
`bacnet_collector`, `plc_collector`, `mqtt_collector`, `smartview`,
`TRUMPF_QDS`) and `data_relay`, which imports `collector_core.errors`. Find
them with `grep -rn collector_core.git ../*/requirements.txt ../*/*/requirements.txt`. An app keeps running on its old pinned tag
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
