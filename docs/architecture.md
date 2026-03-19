# Fidget Spinner Architecture

## Current Shape

The current MVP implementation is intentionally narrower than the eventual full
product:

```text
agent host
    |
    | bundled fidget-spinner skills + stdio MCP
    v
spinner MCP host
    |
    +-- public JSON-RPC transport
    +-- session seed capture and restore
    +-- explicit project binding
    +-- tool catalog and replay contracts
    +-- health and telemetry
    +-- hot rollout / re-exec
    |
    +-- disposable MCP worker
    |   |
    |   +-- per-project SQLite store
    |   +-- per-project blob directory
    |   +-- git/worktree introspection
    |   +-- atomic experiment closure
    |
    v
<project root>/.fidget_spinner/
```

There is no long-lived daemon yet. The first usable slice runs MCP from the CLI
binary, but it already follows the hardened host/worker split required for
long-lived sessions and safe replay behavior.

## Package Boundary

The package currently contains three coupled layers:

- `fidget-spinner-core`
- `fidget-spinner-store-sqlite`
- `fidget-spinner-cli`

And two bundled agent assets:

- `assets/codex-skills/fidget-spinner/SKILL.md`
- `assets/codex-skills/frontier-loop/SKILL.md`

Those parts should be treated as one release unit.

## Storage Topology

Every initialized project owns a private state root:

```text
<project root>/.fidget_spinner/
    project.json
    schema.json
    state.sqlite
    blobs/
```

Why this shape:

- schema freedom stays per project
- migrations stay local
- backup and portability stay simple
- we avoid premature pressure toward a single global schema

Cross-project search can come later as an additive index.

## State Layers

### 1. Global engine spine

The engine depends on a stable, typed spine stored in SQLite:

- nodes
- node annotations
- node edges
- frontiers
- checkpoints
- runs
- metrics
- experiments
- event log

This layer powers traversal, indexing, archiving, and frontier projection.

### 2. Project payload layer

Each node stores a project payload as JSON, namespaced and versioned by the
project schema in `.fidget_spinner/schema.json`.

This is where domain-specific richness lives.

### 3. Annotation sidecar

Annotations are stored separately from payload and are default-hidden unless
explicitly surfaced.

That separation is important. It prevents free-form scratch text from silently
mutating into a shadow schema.

## Validation Model

Validation has three tiers.

### Storage validity

Hard-fail conditions:

- malformed engine envelope
- broken ids
- invalid enum values
- broken relational integrity

### Semantic quality

Project field expectations are warning-heavy:

- missing recommended fields emit diagnostics
- missing projection-gated fields remain storable
- ingest usually succeeds

### Operational eligibility

Specific actions may refuse incomplete records.

Examples:

- core-path experiment closure requires complete run/result/note/verdict state
- future promotion helpers may require a projection-ready change payload

## SQLite Schema

### `nodes`

Stores the global node envelope:

- id
- class
- track
- frontier id
- archived flag
- title
- summary
- schema namespace
- schema version
- payload JSON
- diagnostics JSON
- agent session id
- timestamps

### `node_annotations`

Stores sidecar free-form annotations:

- annotation id
- owning node id
- visibility
- optional label
- body
- created timestamp

### `node_edges`

Stores typed DAG edges:

- source node id
- target node id
- edge kind

The current edge kinds are enough for the MVP:

- `lineage`
- `evidence`
- `comparison`
- `supersedes`
- `annotation`

### `frontiers`

Stores derived operational frontier records:

- frontier id
- label
- root contract node id
- status
- timestamps

Important constraint:

- the root contract node itself also carries the same frontier id

That keeps frontier filtering honest.

### `checkpoints`

Stores committed candidate or champion checkpoints:

- checkpoint id
- frontier id
- anchoring node id
- repo/worktree metadata
- commit hash
- disposition
- summary
- created timestamp

In the current codebase, a frontier may temporarily exist without a champion if
it was initialized outside a git repo. Core-path experimentation is only fully
available once git-backed checkpoints exist.

### `runs`

Stores run envelopes:

- run id
- run node id
- frontier id
- backend
- status
- code snapshot metadata
- benchmark suite
- command envelope
- started and finished timestamps

### `metrics`

Stores primary and supporting run metrics:

- run id
- metric key
- value
- unit
- optimization objective

### `experiments`

Stores the atomic closure object for core-path work:

- experiment id
- frontier id
- base checkpoint id
- candidate checkpoint id
- change node id
- run node id and run id
- optional analysis node id
- decision node id
- verdict
- note payload
- created timestamp

This table is the enforcement layer for frontier discipline.

### `events`

Stores durable audit events:

- event id
- entity kind
- entity id
- event kind
- payload
- created timestamp

## Core Types

### Node classes

Core path:

- `contract`
- `change`
- `run`
- `analysis`
- `decision`

Off path:

- `research`
- `enabling`
- `note`

### Node tracks

- `core_path`
- `off_path`

Track is derived from class, not operator whim.

### Frontier projection

The frontier projection currently exposes:

- frontier record
- champion checkpoint id
- active candidate checkpoint ids
- experiment count

This projection is derived from canonical state and intentionally rebuildable.

## Write Surfaces

### Low-ceremony off-path writes

These are intentionally cheap:

- `note.quick`
- `research.record`
- generic `node.create` for escape-hatch use
- `node.annotate`

### Low-ceremony core-path entry

`change.record` exists to capture intent before worktree state becomes muddy.

### Atomic core-path closure

`experiment.close` is the important write path.

It persists, in one transaction:

- run node
- run record
- candidate checkpoint
- decision node
- experiment record
- lineage and evidence edges
- frontier touch and champion demotion when needed

That atomic boundary is the answer to the ceremony/atomicity pre-mortem.

## MCP Surface

The MVP MCP server is stdio-only and follows newline-delimited JSON-RPC message
framing. The public server is a stable host. It owns initialization state,
replay policy, telemetry, and host rollout. Execution happens in a disposable
worker subprocess.

### Host responsibilities

- own the public JSON-RPC session
- enforce initialize-before-use
- classify tools and resources by replay contract
- retry only explicitly safe operations after retryable worker faults
- expose health and telemetry
- re-exec the host binary while preserving initialization seed and counters

### Worker responsibilities

- open the per-project store
- execute tool logic and resource reads
- return typed success or typed fault records
- remain disposable without losing canonical state

### Fault model

Faults are typed by:

- kind: `invalid_input`, `not_initialized`, `transient`, `internal`
- stage: `host`, `worker`, `store`, `transport`, `protocol`, `rollout`

Those faults are surfaced both as JSON-RPC errors and as structured tool
errors, depending on call type.

### Replay contracts

The tool catalog explicitly marks each operation as one of:

- `safe_replay`
- `never_replay`

Current policy:

- reads such as `project.status`, `project.schema`, `frontier.list`,
  `frontier.status`, `node.list`, `node.read`, `skill.list`, `skill.show`, and
  resource reads
  are safe to replay once after a retryable worker fault
- mutating tools such as `frontier.init`, `node.create`, `change.record`,
  `node.annotate`, `node.archive`, `note.quick`, `research.record`, and
  `experiment.close` are never auto-replayed

This is the hardening answer to side-effect safety.

Implemented server features:

- tools
- resources

### Tools

Implemented tools:

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

### Resources

Implemented resources:

- `fidget-spinner://project/config`
- `fidget-spinner://project/schema`
- `fidget-spinner://skill/fidget-spinner`
- `fidget-spinner://skill/frontier-loop`

### Operational tools

`system.health` returns a typed operational snapshot:

- initialization state
- binding state
- worker generation and liveness
- current executable path
- launch-path stability
- rollout-pending state
- last recorded fault

`system.telemetry` returns cumulative counters:

- requests
- successes
- errors
- retries
- worker restarts
- host rollouts
- per-operation counts and last latencies

### Rollout model

The host fingerprints its executable at startup. If the binary changes on disk,
or if a rollout is explicitly requested, the host re-execs itself after sending
the current response. The re-exec carries forward:

- initialization seed
- project binding
- telemetry counters
- request id sequence
- worker generation
- one-shot rollout and crash-test markers

This keeps the public session stable while still allowing hot binary replacement.

## CLI Surface

The CLI remains thin and operational.

Current commands:

- `init`
- `schema show`
- `frontier init`
- `frontier status`
- `node add`
- `node list`
- `node show`
- `node annotate`
- `node archive`
- `note quick`
- `research add`
- `experiment close`
- `mcp serve`
- hidden internal `mcp worker`
- `skill list`
- `skill install`
- `skill show`

The CLI is not the strategic write plane, but it is the easiest repair and
bootstrap surface.

## Bundled Skill

The bundled `fidget-spinner` and `frontier-loop` skills should
be treated as part of the product, not stray prompts.

Their job is to teach agents:

- DAG first
- schema first
- cheap off-path pushes
- disciplined core-path closure
- archive rather than delete
- and, for the frontier-loop specialization, how to run an indefinite push

The asset lives in-tree so it can drift only via an explicit code change.

## Full-Product Trajectory

The full product should add, not replace, the MVP implementation.

Planned next layers:

- `spinnerd` as a long-lived local daemon
- HTTP and SSE
- read-mostly local UI
- runner orchestration beyond direct process execution
- interruption recovery and resumable long loops
- archive and pruning passes
- optional cross-project indexing

The invariant for that future work is strict:

- keep the DAG canonical
- keep frontier state derived
- keep project payloads local and flexible
- keep off-path writes cheap
- keep core-path closure atomic
- keep host-owned replay contracts explicit and auditable
