# Fidget Spinner

Fidget Spinner is a local-first, agent-first experimental DAG for autonomous
program optimization, source capture, and experiment adjudication.

It is aimed at the ugly, practical problem of replacing sprawling experiment
markdown in worktree-heavy optimization projects such as `libgrid` with a
structured local system of record.

The current shape is built around four ideas:

- the DAG is canonical truth
- frontier state is a derived projection
- project payload schemas are local and flexible
- core-path work is hypothesis-owned and experiment-gated

## Current Scope

Implemented today:

- typed Rust core model
- per-project SQLite store under `.fidget_spinner/`
- project-local schema file
- light-touch project field types: `string`, `numeric`, `boolean`, `timestamp`
- hidden and visible node annotations
- core-path and off-path node classes
- CLI for local project work
- hardened stdio MCP host via `mcp serve`
- minimal web navigator via `ui serve`
- replay-aware disposable MCP worker runtime
- MCP health and telemetry tools
- bundled `fidget-spinner` base skill
- bundled `frontier-loop` specialization

Not implemented yet:

- long-lived daemon
- full web UI
- remote runners
- strong markdown migration
- cross-project indexing

## Local Install

Install the release CLI into `~/.local/bin` and refresh the bundled skill
symlinks in `~/.codex/skills` with:

```bash
./scripts/install-local.sh
```

Pass a different local install root or skill destination explicitly if needed:

```bash
./scripts/install-local.sh /tmp/fidget-local /tmp/codex-skills
```

## Quickstart

Initialize the current directory as a Fidget Spinner project:

```bash
cargo run -p fidget-spinner-cli -- init --project . --name fidget-spinner --namespace local.fidget-spinner
```

Create a frontier:

```bash
cargo run -p fidget-spinner-cli -- frontier init \
  --project . \
  --label "repo evolution" \
  --objective "improve the local MVP" \
  --contract-title "fidget spinner self-host frontier" \
  --benchmark-suite smoke \
  --promotion-criterion "cleaner and more capable" \
  --primary-metric-key research_value \
  --primary-metric-unit count \
  --primary-metric-objective maximize
```

Register project-level metric and run-dimension vocabulary before recording a
lot of experiments:

```bash
cargo run -p fidget-spinner-cli -- schema upsert-field \
  --project . \
  --name scenario \
  --class hypothesis \
  --class analysis \
  --presence recommended \
  --severity warning \
  --role projection-gate \
  --inference manual-only \
  --type string
```

```bash
cargo run -p fidget-spinner-cli -- metric define \
  --project . \
  --key wall_clock_s \
  --unit seconds \
  --objective minimize \
  --description "elapsed wall time"
```

```bash
cargo run -p fidget-spinner-cli -- dimension define \
  --project . \
  --key scenario \
  --type string \
  --description "workload family"
```

```bash
cargo run -p fidget-spinner-cli -- dimension define \
  --project . \
  --key duration_s \
  --type numeric \
  --description "time budget in seconds"
```

Record low-ceremony off-path work:

```bash
cargo run -p fidget-spinner-cli -- tag add \
  --project . \
  --name dogfood/mvp \
  --description "Self-hosted MVP dogfood notes"
```

```bash
cargo run -p fidget-spinner-cli -- source add \
  --project . \
  --title "next feature slate" \
  --summary "Investigate the next tranche of high-value product work." \
  --body "Investigate pruning, richer projections, and libgrid schema presets." \
  --tag dogfood/mvp
```

```bash
cargo run -p fidget-spinner-cli -- note quick \
  --project . \
  --title "first tagged note" \
  --summary "Tag-aware note capture is live." \
  --body "Tag-aware note capture is live." \
  --tag dogfood/mvp
```

Record a core-path hypothesis and open an experiment against it:

```bash
cargo run -p fidget-spinner-cli -- hypothesis add \
  --project . \
  --frontier <frontier-id> \
  --title "inline metric table" \
  --summary "Rendering candidate metrics on cards will improve navigator utility." \
  --body "Surface experiment metrics and objective-aware deltas directly on change cards."
```

```bash
cargo run -p fidget-spinner-cli -- experiment open \
  --project . \
  --frontier <frontier-id> \
  --hypothesis-node <hypothesis-node-id> \
  --title "navigator metric card pass" \
  --summary "Evaluate inline metrics on experiment-bearing cards."
```

```bash
cargo run -p fidget-spinner-cli -- metric keys --project .
```

```bash
cargo run -p fidget-spinner-cli -- metric best \
  --project . \
  --key wall_clock_s \
  --dimension scenario=belt_4x5 \
  --dimension duration_s=60 \
  --source run-metric
```

Serve the local MCP surface in unbound mode:

```bash
cargo run -p fidget-spinner-cli -- mcp serve
```

Serve the minimal local navigator:

```bash
cargo run -p fidget-spinner-cli -- ui serve --path . --bind 127.0.0.1:8913
```

`ui serve --path` is permissive: it accepts the project root, the
`.fidget_spinner/` directory itself, descendants inside that directory, or a
parent directory containing one unique descendant store.

Then bind the session from the client with:

```json
{"name":"project.bind","arguments":{"path":"<project-root-or-nested-path>"}}
```

If the target root is an existing empty directory, `project.bind` now
bootstraps `.fidget_spinner/` automatically instead of requiring a separate
`init` step. Non-empty uninitialized directories still fail rather than being
guessed into existence.

Install the bundled skills into Codex:

```bash
cargo run -p fidget-spinner-cli -- skill install
```

## Store Layout

Each initialized project gets:

```text
.fidget_spinner/
    project.json
    schema.json
    state.sqlite
    blobs/
```

`schema.json` is the model-facing contract for project-local payload fields and
their validation tiers. Fields may now optionally declare a light-touch
`value_type` of `string`, `numeric`, `boolean`, or `timestamp`; mismatches are
diagnostic warnings rather than ingest blockers.

`.fidget_spinner/` is local state. In git-backed projects it usually belongs in
`.gitignore` or `.git/info/exclude`.

## Model-Facing Surface

The current MCP tools are:

- `system.health`
- `system.telemetry`
- `project.bind`
- `project.status`
- `project.schema`
- `schema.field.upsert`
- `schema.field.remove`
- `tag.add`
- `tag.list`
- `frontier.list`
- `frontier.status`
- `frontier.init`
- `node.create`
- `hypothesis.record`
- `experiment.open`
- `experiment.list`
- `experiment.read`
- `node.list`
- `node.read`
- `node.annotate`
- `node.archive`
- `note.quick`
- `source.record`
- `metric.define`
- `metric.keys`
- `metric.best`
- `metric.migrate`
- `run.dimension.define`
- `run.dimension.list`
- `experiment.close`
- `skill.list`
- `skill.show`

Nontrivial MCP tools follow the shared presentation contract:

- `render=porcelain|json` chooses terse text vs structured JSON rendering
- `detail=concise|full` chooses triage payload vs widened detail
- porcelain is default and is intentionally not just pretty-printed JSON

Operationally, the MCP now runs as a stable host process that owns the public
JSON-RPC session and delegates tool execution to an internal worker subprocess.
Safe replay is only allowed for explicitly read-only operations and resources.
Mutating tools are never auto-replayed after worker failure.

Notes now require an explicit `tags` list. Tags are repo-local registry entries
created with `tag.add`, each with a required human description. `note.quick`
accepts `tags: []` when no existing tag applies, but the field itself is still
mandatory so note classification is always conscious.

`source.record` now also accepts optional `tags`, so rich imported documents
can join the same campaign/subsystem index as terse notes without falling back
to the generic escape hatch.

`note.quick`, `source.record`, and generic `node create` for `note`/`source`
now enforce the same strict prose split: `title` is terse identity, `summary`
is the triage/search layer, and `body` holds the full text. List-like surfaces
stay on `title` + `summary`; full prose is for explicit reads only.

Schema authoring no longer has to happen by hand in `.fidget_spinner/schema.json`.
The CLI exposes `schema upsert-field` / `schema remove-field`, and the MCP
surface exposes the corresponding `schema.field.upsert` / `schema.field.remove`
tools. The CLI uses space-separated subcommands; the MCP uses dotted tool names.

Metrics and run dimensions are now project-level registries. Frontier contracts
still declare the evaluation metric vocabulary, but closed experiments report
only thin `key=value` metrics plus typed run dimensions. `metric.define` can
enrich metric descriptions, CLI `dimension define` / MCP `run.dimension.define`
preregister slicers such as `scenario` or `duration_s`, `metric.keys`
discovers rankable numeric signals, and `metric.best` ranks one key within
optional exact dimension filters.
Legacy `benchmark_suite` data is normalized into a builtin string dimension on
store open, and `metric.migrate` can be invoked explicitly as an idempotent
repair pass.

The intended flow is:

1. inspect `system.health`
2. `project.bind` to the target project root or any nested path inside it
3. read `project.status`, `tag.list`, and `frontier.list`
4. read `experiment.list` if the session may be resuming in-flight work
5. read `project.schema` only when payload rules are actually relevant
6. pull context from the DAG
7. use `source.record` for documentary context and `note.quick` for atomic takeaways
8. record a `hypothesis` before core-path work
9. open the live experiment explicitly with `experiment.open`
10. seal core-path work with `experiment.close`

## Git And The Ledger

Git remains useful for code history, bisect, and sensible commit messages, but
the Fidget Spinner ledger is about the science rather than about reproducing git
inside the experiment record.

Core-path closure does not require a git-backed project. The canonical record is
the hypothesis, run slice, parsed metrics, verdict, and rationale.

## Workspace Layout

- `crates/fidget-spinner-core`: domain model and invariants
- `crates/fidget-spinner-store-sqlite`: per-project store and atomic writes
- `crates/fidget-spinner-cli`: CLI plus hardened stdio MCP host and worker
- `assets/codex-skills/fidget-spinner`: bundled base skill asset
- `assets/codex-skills/frontier-loop`: bundled skill asset

## Docs

- [docs/product-spec.md](docs/product-spec.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/libgrid-dogfood.md](docs/libgrid-dogfood.md)

## Checks

```bash
./check.py
./check.py deep
```
