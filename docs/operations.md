# Operations

This document owns Fidget Spinner's state, migration, service, browser
authority, telemetry, and recovery contracts. CLI syntax remains canonical in
`fidget-spinner-cli --help`.

## State Discovery

The default project-store root is the platform's local per-user state
directory beneath `fidget-spinner/projects/`. On Linux it is:

```text
${XDG_STATE_HOME:-~/.local/state}/fidget-spinner/projects/
```

macOS uses the user Application Support directory; Windows uses local AppData.
Each directory contains a `state.sqlite` whose metadata names its canonical
project root. `FIDGET_SPINNER_STATE_HOME=/absolute/path` replaces the platform
state root; Spinner still appends `fidget-spinner/projects/`. Moving a Git
checkout changes its canonical identity and is not an automatic store
migration. Retain the original path or recover from a backup with operator
assistance; do not rename store directories by hand.

## Upgrade And Recovery

Version 1.0 writes store format 20 and automatically accepts formats 9 through
19. Each migration changes schema, data, and `PRAGMA user_version` in one SQLite
transaction. Failed initialization removes its partial database. Initializing
an existing database is rejected.

Before an upgrade that changes the store format:

1. Stop the navigator and MCP processes that use the project.
2. Copy `state.sqlite` to durable storage.
3. Install the new binary and open the project once.
4. Run `project status`, then inspect `system.health` through MCP.

Format-compatible executable upgrades do not require an MCP restart. The local
installer publishes the successor with an atomic rename. Each idle Unix MCP
host polls the canonical executable, waits for one stable successor
observation, and then replaces its process image while retaining its stdio
pipes, initialized session, project binding, request journal, and telemetry.
Partially written or temporarily absent successors are never executed. With
the enhanced Linux installation, the navigator remains a systemd-managed
process and is restarted by the installer. Other installations restart it
through their chosen process supervisor.

If an upgrade fails, preserve both the database and the exact error. Retry with
the same binary only after correcting an external cause such as permissions or
disk exhaustion. To roll back, stop Spinner, restore the pre-upgrade copy, and
run the binary that owns that format. Spinner will not reinterpret an unknown
format.

Ledger removal is deliberately separate from program removal. The installer’s
`--uninstall` mode leaves every project store intact.

## Linux User Service

The installer creates `fidget-spinner-ui.service` under the absolute
`$XDG_CONFIG_HOME/systemd/user` or its `~/.config/systemd/user` fallback and
enables it on first install. It marks every installed artifact, refuses to
replace unowned paths, and preserves foreign contents during removal. Ordinary
controls are:

```bash
systemctl --user status fidget-spinner-ui.service
systemctl --user restart fidget-spinner-ui.service
journalctl --user -u fidget-spinner-ui.service
```

Set `FIDGET_SPINNER_UI_BIND` while installing to choose another loopback
listener. Set `FIDGET_SPINNER_INSTALL_SYSTEMD=0` to manage the process yourself.

## Browser Authority

The navigator has supervisor mutation authority and no login surface. Its safe
default is loopback. On loopback it admits only `localhost`, `127.0.0.0/8`, and
`[::1]` authorities for the actual listener port. Browser writes must be
same-origin according to `Origin` and `Sec-Fetch-Site` when those headers are
present. Local non-browser clients remain usable without browser-only headers.

`--allow-remote` permits a non-loopback listener but adds no identity,
confidentiality, or authorization. Place such a listener behind an
authenticated, encrypted boundary and restrict network reachability. Do not
expose it directly to an untrusted network.

## Telemetry

`system.telemetry` reports bounded, payload-free host telemetry:

- request, success, error, retry, worker-restart, and rollout counts
- per-operation latency and stable fault-code counts
- the aggregation window start and most recent fault

The operation map is cardinality-bounded; unknown overflow is folded into
`other`. Telemetry contains no arguments, SQL, experiment prose, paths beyond
ordinary health output, or result payloads. It survives proactive in-process
binary rollouts through the private libmcp snapshot capsule and resets on an
ordinary host restart. These aggregates are the evidence surface for deciding
whether additive argument tolerance or aliases are warranted.

## Frontier SQL

`frontier.query.sql` opens a separate read-only SQLite connection, exposes only
the documented `q_*` views, installs an authorizer, and accepts one `SELECT`.
The default envelope is 200 rows and 250 ms. Hard limits are 1,000 rows, 2 s,
32 KiB of SQL, and 256 KiB of result data. The deadline includes synthetic
metric materialization when the selected views require it; unrelated queries
do not pay that cost. Use `frontier.query.schema` as the column authority.

## Performance Gate

`scripts/ui-e2e.mjs` launches the real release binary and Chromium against a
reflink copy of a supplied database. Cold budgets are 1.0 s for the project and
tag pages, 2.0 s for the metric registry, and 2.5 s for a frontier; warm budgets
are stricter. The metric page is capped at 1.5 MB; the frontier and Results
fragment are capped at 1.1 MB, and a cached large-ledger Results transition at
1 s. The same run checks desktop and phone geometry, visible control names,
duplicate IDs, browser mutation, chart toggles, linked hover inspection,
horizontal zoom, PNG clipboard export, client filtering, and keyboard
dismissal. By default, initial Results tables render at most 250 matching rows;
zoom or filters select another subset. Source databases are opened only by the
copying process and are never mutated.
