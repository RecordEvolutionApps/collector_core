# Collector Core Architecture

`collector_core` is the shared, protocol-agnostic core of the IronFlock data
collector app family (MTConnect, Modbus, BACnet, and future IO-Link, OPC UA,
Profinet collectors). It owns everything that is identical regardless of the
wire protocol; each collector app contributes only a protocol adapter and its
platform templates.

```
┌────────────────────── collector app (per protocol) ──────────────────────┐
│ main.py            — env (DEVICE_NAME/KEY), IronFlock handle, wiring      │
│ protocols/x.py     — ProtocolAdapter + ProtocolSession implementation     │
│ .ironflock/        — data-template.yml (core contract + protocol columns) │
│                      board-template.yml (app boards)                      │
├────────────────────── collector_core (this package) ─────────────────────┤
│ Collector          — gateway settings, asset/datapoint config + live      │
│                      subscriptions, one task per asset, measurements      │
│                      assembly, assetstatus tracking, store_data gating    │
│ DatapointStore     — column-agnostic datapoints catalog (upsert/prune)    │
│ ProtocolAdapter/ProtocolSession/DatapointsChanged — the protocol seam     │
└───────────────────────────────────────────────────────────────────────────┘
```

## Data model (five tables)

The canonical core contract ships as
[`collector_core/data-template.yml`](collector_core/data-template.yml).
Every app's own `.ironflock/data-template.yml` must provide these tables with
the core columns and may extend them with protocol-specific columns.

| Table | Purpose | Identity (latest-flag) | Core columns |
|---|---|---|---|
| `gateways` | registry + per-gateway settings | gateway_name | tsp, gateway_name, deleted, store_data, info (json) |
| `assets` | connected protocol endpoints | gateway_id, asset_name | tsp, asset_name, gateway_id, datapoint_spec, collect_interval, enabled, demo_mode, deleted |
| `datapoints` | value-stream catalog per asset | gateway_id, asset_name, datapoint_id | tsp, datapoint_id, asset_name, gateway_id, name, units, path, enabled, change_detection, deleted |
| `measurements` | timeseries recordings | — (append-only) | tsp, asset_name, gateway_id, data (json) |
| `assetstatus` | state transitions (core-owned) | gateway_id, asset_name | tsp, asset_name, gateway_id, status, detail, deleted |

Key semantics:

- **assets.enabled** — pause/run switch. Only an explicit `false` pauses
  collection (no task, demo included); null/missing means running, so rows
  and board forms that omit the column keep working.
- **assets.collect_interval** — seconds between collected batches. Poll
  period for polled protocols, publish/batch window for subscription or
  streaming protocols, downsample window for high-rate feeds.
- **assets.datapoint_spec** — protocol-defined selection/decode spec
  (MTConnect/BACnet/OPC UA: id filter list; Modbus/Profinet: authored
  register/record map in YAML). Empty = collect everything discoverable.
- **datapoints.enabled / datapoints.change_detection** — per-datapoint write
  gating applied by the core in `_publish_batch`, read live from the catalog
  so board edits take effect on the next cycle (no asset restart, no adapter
  change). `enabled` is the null-safe pause switch (only an explicit `false`
  drops the datapoint from every row, mirroring `assets.enabled`).
  `change_detection` writes the datapoint only when its `value` differs from
  the last published value (the first reading after start/enable is the
  baseline). Both are user-owned: the store preserves them across metadata
  re-discovery (`USER_DATAPOINT_COLUMNS`). Defaults (null) keep the prior
  full-snapshot behaviour.
- **measurements.data** — `{datapoint_id: {name, units, value, ...extras}}`.
  Rows are **snapshots**: each row carries the asset's full current state
  (push protocols merge last-known values per window); changes-only rows are
  an explicit opt-in, either protocol-level or per datapoint via
  `change_detection`/`enabled` (then rows are partial — only the datapoints
  that passed the gate). Well-known optional extras: `status`,
  `status_flags`, `source_tsp`, `description`. Soft limit ~500 datapoints
  per asset — beyond that, filter via datapoint_spec or split assets.
- **assetstatus** — written by the core on transitions only:
  `online` (first successful publish), `offline` (collection error, with
  error text in `detail`), `paused` (enabled=false). Latest row = current
  state; history = availability log.
- **gateways.store_data** — `false` switches the app to forward-only mode:
  configuration is still read live, but measurements/datapoints/assetstatus
  are not written. Config writes (gateway registry, asset auto-registration)
  remain active.
- **gateways rows are echo-merged**: the core re-appends the existing row's
  columns at startup so user-edited settings survive the registry write.
- **Secrets never go into tables** (boards can read them). Credentials and
  certificates come from device environment or mounted volumes; tables hold
  only non-secret configuration.
- Protocol events/conditions (OPC UA events, BACnet alarms, MTConnect
  Condition) are modeled as string-valued datapoints; a dedicated events
  table is a possible later extension.

## The protocol seam

### ProtocolAdapter (one instance per collector)

| Member | Kind | Purpose |
|---|---|---|
| `resolve_each_cycle` | class attr | False: resolve datapoints once at (re)configure (cached on `asset["datapoint_list"]`); True: re-resolve every poll cycle (user-authored specs, live edits) |
| `prepare_datapoints(asset, store)` | abstract | Return datapoint dicts (`id`, `name`, `units` + protocol-private keys); discover and persist catalog metadata via the store |
| `create_session(asset)` | abstract | Create the per-asset session. **No I/O in constructors** — sessions are also created in demo mode |
| `format_value(asset, datapoint, raw)` | abstract | Raw protocol value → scalar for `value` |
| `make_entry(asset, datapoint, raw)` | overridable | Build the full measurements entry; the hook for per-reading quality/timestamps and live metadata |
| `apply_settings(gateway_row)` | overridable | Consume gateway-level settings columns; called before asset configuration and on live row changes |
| `start_background(collector)` | overridable | Spawn protocol background work (network discovery that auto-registers asset rows) |

### ProtocolSession (one per asset task)

| Member | Kind | Purpose |
|---|---|---|
| `stream(datapoints_provider, interval)` | overridable async generator | Yields `(datapoints, {datapoint_id: raw})` batches forever. **Default = poll loop** (provider → `read_values` → yield → sleep). Push protocols override: subscribe and yield one merged batch per window |
| `read_values(datapoints)` | default raises | Pull protocols implement; one poll cycle, raises on failure |
| `demo_values(datapoints)` | abstract | Synthetic raw values in the **same shape** as the real path |
| `close()` | overridable | Idempotent teardown; always awaited by the core |

`DatapointsChanged` may be raised mid-stream when the source's datapoint set
changed (IO-Link hot-plug, OPC UA namespace change): the core drops the
cached datapoint list and restarts the session immediately, re-running
discovery. Raise only on an actually observed change.

Stream contract details:

- Raw keys must be a subset of the yielded datapoint list's ids; absent keys
  produce no entry (an empty map writes no row).
- Any other exception ends the stream → core closes the session, marks the
  asset offline, waits one interval, recreates the session and stream.
- The core closes the generator with `aclose()`; keep `finally` blocks at
  yield points brief and put heavy teardown in `close()`.
- `stream` is not called in demo mode — the core paces `demo_values` itself,
  which is why it must work without any server.
- Push overrides must keep accumulation bounded (merge by datapoint id).

## Per-protocol mapping

| | stream | read_values | DatapointsChanged | start_background | make_entry extras |
|---|---|---|---|---|---|
| MTConnect | default | HTTP `/current` (raises on fetch/parse failure) | – | – | – |
| Modbus | default | per-register reads | – | – | – |
| BACnet | default | batched ReadPropertyMultiple | – | Who-Is scan, auto-registers assets | status_flags, live metadata |
| IO-Link | default | one REST `getdatamulti` (pdin + ident + port status) | port ident ≠ snapshot → raise | – (v1) | port_status |
| OPC UA | **override**: subscription per asset, chunked monitored items, yield merged snapshot per window | not implemented | NamespaceArray mismatch on reconnect | – (v1) | source_tsp, status |
| Profinet | default (acyclic record polls); later cyclic sidecar override with downsampled yields | DCE/RPC record reads | – | DCP identify-all, auto-registers assets (needs raw sockets/host networking) | aggregates later |

## Distribution

Apps depend on this package via `requirements.txt`:

```
ironflock-collector-core @ git+https://github.com/RecordEvolutionApps/collector_core.git@main
```

Pin a tag (e.g. `@v2.0.0`) for reproducible fleet builds. App Docker images
need the `git` binary for pip's git support (`apt-get install git` in the
Dockerfile). The package has zero runtime dependencies — the ironflock SDK
handle is injected by each app's `main.py`:

```python
collector = Collector(device_name=DEVICE_NAME, device_key=DEVICE_KEY, adapter=MyAdapter())
ironflock = IronFlock(mainFunc=collector.run)
collector.set_ironflock(ironflock)
ironflock.run()
```
