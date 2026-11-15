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
│ Collector          — gateway registration, follows the board's settings  │
│                      and assets, one task per asset, measurements         │
│                      assembly, assetstatus tracking, store_data gating,   │
│                      transform refreshes                                  │
│ DatapointStore     — column-agnostic datapoints catalog (upsert/prune)    │
│ DemoSignal         — per-datapoint synthetic values for demo mode         │
│ ProtocolAdapter/ProtocolSession/DatapointsChanged — the protocol seam     │
└───────────────────────────────────────────────────────────────────────────┘
```

## Data model (seven tables)

The canonical core contract ships as
[`collector_core/data-template.yml`](collector_core/data-template.yml).
Every app's own `.ironflock/data-template.yml` must provide these tables with
the core columns and may extend them with protocol-specific columns.

Every table has exactly one writer. The gateway writes what it is and what it
measures; the board (and the AI assistant, through config_core) writes what
users decide. `gateway_id` is the gateway's `DEVICE_KEY` in every table.

| Table | Writer | Identity (latest-flag) | Core columns |
|---|---|---|---|
| `gateways` | the gateway, about itself (the board writes only delete rows) | gateway_id | tsp, gateway_id, gateway_name, info (json), url (json), deleted |
| `gateway_settings` | the board | gateway_id | tsp, gateway_id, store_data, buffer_size |
| `assets` | the board's forms and the AI assistant | gateway_id, asset_name | tsp, asset_name, gateway_id, datapoint_spec, collect_interval, enabled, demo_mode, deleted |
| `datapoints` | the gateway (the catalog) | gateway_id, asset_name, datapoint_id | tsp, datapoint_id, asset_name, gateway_id, name, units, path, demo_value, demo_variance, deleted |
| `datapoint_settings` | the board and the AI assistant | gateway_id, asset_name, datapoint_id | tsp, gateway_id, asset_name, datapoint_id, enabled, change_detection, demo_value, demo_variance, configured_by |
| `measurements` | the gateway | — (append-only) | tsp, asset_name, gateway_id, data (json) |
| `assetstatus` | the gateway | gateway_id, asset_name | tsp, asset_name, gateway_id, status, detail, deleted |

Apps add their own columns: connection parameters on `assets`, protocol
metadata on `datapoints`, their own per-gateway settings on `gateway_settings`
(e.g. `iodd_autofetch`).

### gateways

The gateway registers itself at startup with one row:
`{tsp, gateway_id, gateway_name, info, url, deleted: false}` plus the app's
`gateway_extra` columns (the core-owned columns win). `gateway_id` is the
identity; `gateway_name` is the device's `DEVICE_NAME` as a plain value, so a
renamed device stays the same gateway. `url` is written only when the app
declares remote-access ports (see [below](#remote-access-urls-gatewaysurl)).
Nothing of an earlier row is echoed.

A user deletes a gateway on the board with a row `{gateway_id, deleted:
true}`. Nothing deletes a gateway automatically, and a deleted gateway that
starts again writes `deleted: false` and reappears. The core neither reads
nor follows `gateways`.

### gateway_settings

The users' per-gateway settings, one row per gateway, written by the board.
The core follows this gateway's row (see [Following](#following-the-board-written-tables)):

- **store_data** — `false` switches the gateway to forward-only mode:
  configuration is still read live, but measurements, datapoints and
  assetstatus are not written. The registration row is still written.
  Null/missing means the constructor's `store_data` (default true).
- **buffer_size** — rows held in the outbound buffer while the link is down;
  null/missing means `DEFAULT_BUFFER_SIZE` (10000).

Every applied row is also handed to `adapter.apply_settings(row)`, and at
startup the adapter gets the row read then, or `{}` when the board has written
none. Without a row the defaults apply.

### assets

Written by the board's forms and the AI assistant; the core only reads it.

- **enabled** — pause/run switch. Only an explicit `false` pauses
  collection (no task, demo included); null/missing means running, so rows
  and board forms that omit the column keep working.
- **collect_interval** — seconds between collected batches. Poll period for
  polled protocols, publish/batch window for subscription or streaming
  protocols, downsample window for high-rate feeds.
- **datapoint_spec** — protocol-defined selection/decode spec
  (MTConnect/BACnet/OPC UA: id filter list; Modbus/Profinet: authored
  register/record map in YAML). Empty = collect everything discoverable.
- **deleted** — `true` stops the asset; the core then soft-deletes the
  asset's `datapoints` rows (it is the catalog's writer) and its
  `assetstatus`. Catalog rows of an asset deleted while the gateway was not
  running are soft-deleted at the next start. With `store_data` false nothing
  is written.
- An incomplete row (no `asset_name`, or no `gateway_id`) is skipped and
  reported once as a warning, telling the user to complete or delete it on the
  board. A row without a gateway is reported by every gateway; a nameless row
  only by the gateway it names.

### datapoints

The catalog of value streams per asset, written only by the gateway through
`DatapointStore` (discovery, the datapoint spec, soft-deletes of datapoints
that are gone). It holds what the device and the adapter know and no user
settings: `upsert` writes the adapter's payload as given, nothing of the
previous row is carried over. `demo_value` / `demo_variance` are optional
defaults an adapter derives from its spec. The core reads this gateway's
catalog once at startup; the store keeps the in-memory list current as it
writes, so the table is not followed.

### datapoint_settings

The user's per-datapoint switches, written by the board (`configured_by:
"board"`) and the AI assistant (`configured_by: "agent"`), never by the
gateway. The template gives the table no `dropAfter`: nothing rewrites these
rows, so they must not age out. The core follows this gateway's rows into
`Collector.datapoint_settings`, keyed `(asset_name, datapoint_id)`.

- **enabled / change_detection** — per-datapoint write gating applied in
  `_publish_batch`, so a board edit takes effect on the next cycle (no asset
  restart, no adapter change). `enabled` is the null-safe pause switch (only
  an explicit `false` drops the datapoint from every row, mirroring
  `assets.enabled`). `change_detection` writes the datapoint only when its
  `value` differs from the last published value; the first reading after
  start, and after every new settings row of the datapoint, is the baseline.
  Without a row both are off: full snapshots.
- **demo_value / demo_variance** — what demo mode should produce for this
  datapoint, so synthetic data is plausible in context (a temperature sits at
  75 °C ±2, not a random 0–100). `demo_value` is the value the reading sits
  at, in **engineering units** — adapters must not apply their
  raw-to-engineering `scale` on top of it; `demo_variance` is how far it may
  wander (± , same units; `0` pins it to a constant). A boolean `demo_value`
  instead gives a state resting there, with `demo_variance` as the per-sample
  chance of blipping out of it. Per column the settings value wins when it is
  not null, then the catalog's default, then the adapter's resolved dict; all
  null = the 0–100 range. See [Demo mode](#demo-mode).

### measurements

- **data** — `{datapoint_id: {name, units, value, ...extras}}`. Rows are
  **snapshots**: each row carries the asset's full current state (push
  protocols merge last-known values per window); changes-only rows are an
  explicit opt-in, either protocol-level or per datapoint via
  `change_detection`/`enabled` (then rows are partial — only the datapoints
  that passed the gate). Well-known optional extras: `status`,
  `status_flags`, `source_tsp`, `description`. Soft limit ~500 datapoints
  per asset — beyond that, filter via datapoint_spec or split assets.

### assetstatus

Written by the core on transitions only: `online` (first successful
publish), `offline` (collection error, with error text in `detail`), `paused`
(enabled=false). Latest row = current state; history = availability log.

### Following the board-written tables

`gateway_settings`, `assets` and `datapoint_settings` are followed
subscribe-first: the core subscribes, then reads the current rows with the
SDK's `{"latest": True}` marker (`gateway_settings` and `datapoint_settings`
filtered on `gateway_id = DEVICE_KEY`), so a row saved during startup is not
lost. Every row, read or delivered, is applied only when it belongs to this
gateway and its `tsp` is newer than the last row applied for that entity
(rows within 1 ms count as the same row: a read returns epoch milliseconds,
an event ISO text), so an older read never undoes a newer edit. `deleted` is
null-safe like `enabled`: only an explicit `true` deletes, so rows a board
form wrote without the flag are live. Rows of one asset are applied one at a
time under a per-asset lock, so a slow discovery for a stale row cannot
override a later edit. After applying a row the core requests a refresh of
the transforms mapped to that table (see
[Refreshing the app's transforms](#refreshing-the-apps-transforms)).

### Writing

- **Rows are made storable before they are buffered.** Measurements and
  assetstatus share one bounded buffer, and a row the platform cannot carry
  (a lone surrogate from a JSON escape, NaN/Infinity, an integer outside
  64 bits, a set or other object) would stay at its head and block every
  asset's writes. `collector_core.storable` repairs such values instead of
  dropping the row — bad code points are escaped, non-finite floats become
  null, the rest become text — and the core reports the repair as a warning
  at most once an hour per asset, table and kind. Catalog writes and error
  reports go through the same repair, and `Collector.run` sets stdout/stderr
  to `backslashreplace`, so logging such text cannot end an asset's task. A
  healthy write path prints nothing.
- **A send never waits for the platform.** `_send` puts the row into the
  buffer and starts a drain as a task of its own; at most one drain runs and
  one waits behind it for the rows appended meanwhile. The SDK
  (ironflock >= 1.9) holds a table write for up to its `reconnect_window`
  (60 s) while the link is down, and an asset's collection loop waiting for
  that would take one reading a minute during an outage.
- **A cancelled caller never takes buffered rows with it.** The drain is not
  part of the asset's collection task, which an app cancels whenever it
  reconfigures that asset (every row edit), and the flush loop's drain runs
  shielded: the rows a drain took out of the shared buffer - every asset's -
  are written once, or re-queued if the write fails.
- **Latest entity rows are kept alive.** Table retention (`dropAfter`) drops
  whole chunks, the latest row of an entity included, so a datapoint or an
  asset status that never changes would age out. Every `REASSERT_INTERVAL`
  (6 h) the core rewrites the full latest `datapoints` rows and `assetstatus`
  rows it owns that are older than `Collector.reassert_after` (default 7
  days). Keep every app's `dropAfter` for those tables far above that. The
  board-written tables are not rewritten: `assets` is kept for 3 years, and
  `gateways`, `gateway_settings` and `datapoint_settings` have no `dropAfter`.
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
| `apply_settings(settings_row)` | overridable | Consume this gateway's `gateway_settings` row (`{}` without one); called before asset configuration and on every newer row |
| `start_background(collector)` | overridable | Spawn protocol background work (network discovery that auto-registers asset rows) |

### ProtocolSession (one per asset task)

| Member | Kind | Purpose |
|---|---|---|
| `stream(datapoints_provider, interval)` | overridable async generator | Yields `(datapoints, {datapoint_id: raw})` batches forever. **Default = poll loop** (provider → `read_values` → yield → sleep). Push protocols override: subscribe and yield one merged batch per window |
| `read_values(datapoints)` | default raises | Pull protocols implement; one poll cycle, raises on failure |
| `demo_values(datapoints)` | overridable | Synthetic raw values in the **same shape** as the real path. **Default = the core's per-datapoint scalars**; override only for a richer raw shape |
| `demo_signal` | property | The session's `DemoSignal` (lazy, one per session). An override calls `self.demo_signal.value(dp)` for the number |
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

## Demo mode

`assets.demo_mode` makes an asset produce synthetic data instead of reading a
device, so names, units, dashboards and alarm limits can be checked before the
hardware is reachable. The core paces it (`stream` is never called) and shapes
the values from the effective `demo_value` / `demo_variance` of each datapoint
(see [datapoint_settings](#datapoint_settings)).

`collector_core.demo.DemoSignal` turns them into a **mean-reverting random
walk**: each sample steps from the previous one and is pulled back towards
`demo_value`, staying inside the band. Consecutive readings are therefore
correlated — a wandering line on a chart rather than a hairbrush, which is what
makes a demo dashboard judgeable at all. Unparsable settings fall back to the
defaults instead of raising: one mistyped datapoint must not take an asset's
demo stream down.

Protocols whose raw value **is** the scalar need nothing — the default
`demo_values` already returns core-shaped values. A protocol with a richer raw
shape wraps the number:

```python
def demo_values(self, datapoints):
    return {dp["id"]: {"value": self.demo_signal.value(dp),
                       "quality": "good", "source_tsp": _now()}
            for dp in datapoints if dp.get("id")}
```

Two rules for adapters: keep **one** `DemoSignal` for the session's lifetime
(the walk state lives in it — `self.demo_signal` handles this), and do not
apply `scale` to a demo value in `format_value`, since it is already in
engineering units.

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

## Remote-access URLs (gateways.url)

To surface a gateway's own web UIs (a config UI, a live viewer) for embedding
on a board, the app passes the ports it declares in its
`.ironflock/port-template.yml` to the collector, which resolves each to a public
tunnel URL via the SDK's `getRemoteAccessUrlForPort` and writes them to the
gateway row's `url` column at registration:

```python
collector = Collector(
    device_name=DEVICE_NAME,
    device_key=DEVICE_KEY,
    adapter=MyAdapter(),
    ports=Collector.load_ports_from_template(),  # reads .ironflock/port-template.yml
)
```

`ports` is a list of port specs — `{name, port, protocol?, main?,
remote_port_environment?}`, the same shape as the template's `ports:` entries;
pass a literal list instead of the helper if the app prefers not to depend on
PyYAML. The resolved `url` column is a JSON object **keyed by the port number**
(as a string) → `{name, protocol, main, local?, cloud?}`, so the frontend can
index a port directly. Each scope is a ready-to-use absolute URL (iframe `src`
for `http`/`https` ports), and a consumer picks one explicitly:

- `cloud: {url}` — the tunnel URL **exactly as the SDK's
  `getRemoteAccessUrlForPort` returns it**. The core composes no tunnel URL of
  its own: the label, the `secure-` prefix, a development run's `-dev` suffix,
  an instance device's `i{INSTANCE_KEY}-` route, the domains and the announced
  tcp/udp remote ports are the SDK's rules. Absent while the SDK knows no URL
  (no identity yet, a tcp/udp tunnel not up yet).
- `local: {ip, port, url}` — the LAN endpoint, from `DEVICE_LAN_IP` and
  `DEVICE_PORT_FOR_<port>`.

For example, a `{port: 55000, name: "Config UI", main: true}` spec on device 42
resolves to:

```json
{ "55000": { "name": "Config UI", "protocol": "http", "main": true,
             "cloud": { "url": "https://42-app-55000.app.ironflock.com" } } }
```

and, in a development run (`ENV` other than `PROD`), to the run's own tunnel
`https://42-app-55000-dev.app.ironflock.com`.

Resolution is best-effort and never blocks registration. Using this feature requires **`ironflock >= 1.5.3`** (the
`protocol` / `remote_port_environment` arguments), and **`>= 1.11.1`** for a
development run's `-dev` URL (older SDKs return the PROD install's URL there);
apps that declare no ports are unaffected and keep the old minimum.

## Refreshing the app's transforms

An app's board often shows a transform (a view in its data-template) over the
core's tables, such as a count of the assets per state over `assetstatus`. A
transform reaches the board on its `schedule` tick, or at once when the app's
code calls the SDK's `refresh_transform` (ironflock >= 1.11.0). The app maps
each table to the transforms that read it, in bottom-up order (a transform
before the transforms that read it):

```python
collector = Collector(
    device_name=DEVICE_NAME,
    device_key=DEVICE_KEY,
    adapter=MyAdapter(),
    refresh_transforms={
        "assetstatus": ["asset_status_counts", "asset_overview"],
        "assets": ["asset_overview"],
        "gateways": ["gateway_overview"],
    },
)
```

The core requests a refresh of a table's transforms:

- after it writes the table: each chunk a drain of the outbound buffer wrote
  (measurements, assetstatus), each `DatapointStore.upsert` / `prune` that
  wrote catalog rows, the registration row, the keep-alive's rewrites;
- after it applies a row of its own gateway from a followed table (`assets`,
  `gateway_settings`, `datapoint_settings`): the container that follows a
  board-written table refreshes once it applied the change.

With `store_data` false no data rows are written, so only the registration
and the applied rows refresh. An adapter that writes a table of its own (the
IO-Link adapter's `iodds`) maps it in `refresh_transforms` and calls
`collector.request_refresh("iodds")` after its writes.

`request_refresh` returns at once; the refreshes run as background tasks,
never inside collection or `_send`. A table's list is refreshed one transform
after the other, in its order. Requests coalesce: per table one pass over its
list runs and one more waits for the requests made meanwhile; per transform
one refresh is in flight and one waits, and two refreshes of the same
transform are at least `REFRESH_MIN_INTERVAL` (2 s) apart, so a mapping
driven by `measurements` cannot hammer the data backend. A refresh refused
with `URI_REFRESH_IN_PROGRESS` (a scheduled refresh holds the view) is retried
once after 2 s; any other failure is printed, never raised, and the
transform's schedule still runs. With an SDK that has no `refresh_transform`
nothing is refreshed, and the core says so once.
