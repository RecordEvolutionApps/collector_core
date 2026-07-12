# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`collector_core` is the shared, protocol-agnostic core of the IronFlock data collector app family (MTConnect, Modbus, BACnet, IO-Link, OPC UA, Profinet, PLC). Collector apps (e.g. the sibling `../plc_collector`) install it straight from this Git repo pinned to a `vX.Y.Z` tag — there is no PyPI package. Each app contributes only a `ProtocolAdapter`/`ProtocolSession` implementation and its platform templates; the core owns everything else (gateway settings, asset/datapoint config with live table subscriptions, one collection task per asset, measurements assembly, assetstatus tracking, buffered writes, data-write gating).

**Read ARCHITECTURE.md before touching the contract** — it defines the five-table data model, the adapter/session seam, the stream contract, and the per-protocol mapping. The canonical table schema ships as `collector_core/data-template.yml`.

## Commands

```bash
just setup                 # create .venv with editable install + test deps
just test                  # run pytest (uses .venv if present)
just test -k flush         # run a subset (args pass through to pytest)
just test tests/test_errors.py::test_name   # single test
just release 2.6.0         # bump pyproject version, test, commit, tag, push (main only, clean tree)
just release-patch         # auto-bump patch and release
```

pytest runs with `asyncio_mode = "auto"` — async test functions need no decorator.

## Hard constraints

- **Zero runtime dependencies.** The core never imports the `ironflock` SDK; the app injects the handle via `collector.set_ironflock(ironflock)`. Don't add anything to `dependencies` in pyproject.toml. If a change starts calling a newer SDK method, bump the minimum `ironflock` version recorded in README.md.
- **Python >= 3.10.**
- **Semver is judged by the contract exposed to apps**: the adapter/session interface (`collector_core/adapter.py`), the table schema (`collector_core/data-template.yml`), and the gateway/asset/datapoint columns the core reads. Breaking any of those is MAJOR. Git tag and `pyproject.toml` version must stay identical (the `just release` recipe enforces this).
- After a release, each consuming app's `requirements.txt` must be re-pinned to the new tag — apps stay on their old tag until bumped.

## Code layout

Four modules in `collector_core/`:

- `collector.py` — the `Collector` class: gateway registration/settings, asset config + live subscription handlers, per-asset task management, the collection loop (`collect_asset` → `_publish_batch`), buffered outbound writes (`_flush_loop`), and error reporting (`report_error`/`report_exception`).
- `adapter.py` — the protocol seam: `ProtocolAdapter`, `ProtocolSession` (default `stream` = poll loop; push protocols override), `DatapointStore`, `DatapointsChanged`.
- `errors.py` — typed error hierarchy. All reportable failures are `CollectorError` subclasses carrying a technical message plus an operator-facing `user_message`. `DatapointsChanged` is deliberately NOT a `CollectorError` — it is a control signal for datapoint re-discovery, so `except CollectorError` must never swallow it.
- `data-template.yml` — canonical core table schema every app's own template must include.

Semantics worth knowing when editing behavior (details in ARCHITECTURE.md): `assets.enabled` and `datapoints.enabled` are null-safe pause switches (only explicit `false` pauses); measurements rows are full snapshots unless `change_detection` opts a datapoint into changes-only; `gateways.store_data=false` means forward-only (config reads/writes stay live, no data writes); gateway rows are echo-merged at startup so user edits survive; secrets never go into tables.
