# Fidget Spinner

Fidget Spinner is a local-first, agent-first experimental DAG for autonomous
program optimization and research.

It is aimed at the ugly, practical problem of replacing sprawling experiment
markdown in worktree-heavy optimization projects such as `libgrid` with a
structured local system of record.

The current shape is built around four ideas:

- the DAG is canonical truth
- frontier state is a derived projection
- project payload schemas are local and flexible
- core-path experiment closure is atomic

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

Record low-ceremony off-path work:

```bash
cargo run -p fidget-spinner-cli -- tag add \
  --project . \
  --name dogfood/mvp \
  --description "Self-hosted MVP dogfood notes"
```

```bash
cargo run -p fidget-spinner-cli -- research add \
  --project . \
  --title "next feature slate" \
  --body "Investigate pruning, richer projections, and libgrid schema presets."
```

```bash
cargo run -p fidget-spinner-cli -- note quick \
  --project . \
  --title "first tagged note" \
  --body "Tag-aware note capture is live." \
  --tag dogfood/mvp
```

Serve the local MCP surface in unbound mode:

```bash
cargo run -p fidget-spinner-cli -- mcp serve
```

Serve the minimal local navigator:

```bash
cargo run -p fidget-spinner-cli -- ui serve --project . --bind 127.0.0.1:8913
```

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
- `tag.add`
- `tag.list`
- `frontier.list`
- `frontier.status`
- `frontier.init`
- `node.create`
- `change.record`
- `node.list`
- `node.read`
- `node.annotate`
- `node.archive`
- `note.quick`
- `research.record`
- `experiment.close`
- `skill.list`
- `skill.show`

Operationally, the MCP now runs as a stable host process that owns the public
JSON-RPC session and delegates tool execution to an internal worker subprocess.
Safe replay is only allowed for explicitly read-only operations and resources.
Mutating tools are never auto-replayed after worker failure.

Notes now require an explicit `tags` list. Tags are repo-local registry entries
created with `tag.add`, each with a required human description. `note.quick`
accepts `tags: []` when no existing tag applies, but the field itself is still
mandatory so note classification is always conscious.

The intended flow is:

1. inspect `system.health`
2. `project.bind` to the target project root or any nested path inside it
3. read `project.status`, `tag.list`, and `frontier.list`
4. read `project.schema` only when payload rules are actually relevant
5. pull context from the DAG
6. use cheap off-path writes liberally
7. record a `change` before core-path work
8. seal core-path work with one atomic `experiment.close`

## Git-Backed Vs Plain Local Projects

Off-path work does not require git. You can initialize a local project and use:

- `research add`
- `tag add`
- `note quick`
- `node annotate`
- `mcp serve`

Full core-path experiment closure needs a real git-backed project, such as the
target `libgrid` worktree, because checkpoints and champion capture are git
backed.

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
