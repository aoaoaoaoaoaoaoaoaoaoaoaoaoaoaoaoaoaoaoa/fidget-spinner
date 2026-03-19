# Fidget Spinner

Fidget Spinner is a local-first, agent-first experimental DAG for autonomous
program optimization and research.

The current MVP is built around four ideas:

- the DAG is canonical truth
- frontier state is a derived projection
- project payload schemas are local and flexible
- core-path experiment closure is atomic

The immediate target is not open-ended science in the abstract. It is the ugly,
practical problem of replacing gigantic freeform experiment markdown in
worktree-heavy optimization projects such as `libgrid`.

## Current MVP

Implemented today:

- typed Rust core model
- per-project SQLite store under `.fidget_spinner/`
- project-local schema file
- hidden and visible node annotations
- core-path and off-path node classes
- CLI for bootstrap and repair
- hardened stdio MCP host via `mcp serve`
- replay-aware disposable MCP worker runtime
- MCP health and telemetry tools
- bundled `fidget-spinner` base skill
- bundled `frontier-loop` specialization

Not implemented yet:

- long-lived daemon
- web UI
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
cargo run -p fidget-spinner-cli -- research add \
  --project . \
  --title "next feature slate" \
  --body "Investigate pruning, richer projections, and libgrid schema presets."
```

Serve the local MCP surface in unbound mode:

```bash
cargo run -p fidget-spinner-cli -- mcp serve
```

Then bind the session from the client with:

```json
{"name":"project.bind","arguments":{"path":"<project-root-or-nested-path>"}}
```

Install the bundled skills into Codex:

```bash
./scripts/install-codex-skill.sh
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
their validation tiers.

## Model-Facing Surface

The current MCP tools are:

- `system.health`
- `system.telemetry`
- `project.bind`
- `project.status`
- `project.schema`
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

The intended flow is:

1. inspect `system.health`
2. `project.bind` to the target project root or any nested path inside it
3. read the schema and frontier
4. pull context from the DAG
5. use cheap off-path writes liberally
6. record a `change` before core-path work
7. seal core-path work with one atomic `experiment.close`

## Dogfood Reality

This repository is suitable for off-path dogfood even though it is not
currently a git repo.

That means:

- `research add`
- `note quick`
- `node annotate`
- `mcp serve`

all work here today.

Full core-path experiment closure needs a real git-backed project, such as the
target `libgrid` worktree.

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
