# Fidget Spinner Product Spec

## Thesis

Fidget Spinner is a local-first, agent-first frontier machine for autonomous
program optimization and research.

The immediate target is brutally practical: replace gigantic freeform
experiment markdown with a machine that preserves evidence as structure.

The package is deliberately two things at once:

- a local MCP-backed DAG substrate
- bundled skills that teach agents how to drive that substrate

Those two halves should be versioned together and treated as one product.

## Product Position

This is not a hosted lab notebook.

This is not a cloud compute marketplace.

This is not a collaboration shell with experiments bolted on.

This is a local machine for indefinite frontier pushes, with agents as primary
writers and humans as auditors, reviewers, and occasional editors.

## Non-Goals

These are explicitly out of scope for the core product:

- OAuth
- hosted identity
- cloud tenancy
- billing, credits, and subscriptions
- managed provider brokerage
- chat as the system of record
- mandatory remote control planes
- replacing git

Git remains the code substrate. Fidget Spinner is the evidence substrate.

## Locked Design Decisions

These are the load-bearing decisions to hold fixed through the MVP push.

### 1. The DAG is canonical truth

The canonical record is the DAG plus its normalized supporting tables.

Frontier state is not a rival authority. It is a derived, rebuildable
projection over the DAG and related run/checkpoint/experiment records.

### 2. Storage is per-project

Each project owns its own local store under:

```text
<project root>/.fidget_spinner/
    state.sqlite
    project.json
    schema.json
    blobs/
```

There is no mandatory global database in the MVP.

### 3. Node structure is layered

Every node has three layers:

- a hard global envelope for indexing and traversal
- a project-local structured payload
- free-form sidecar annotations as an escape hatch

The engine only hard-depends on the envelope. Project payloads remain flexible.

### 4. Validation is warning-heavy

Engine integrity is hard-validated.

Project semantics are diagnostically validated.

Workflow eligibility is action-gated.

In other words:

- bad engine state is rejected
- incomplete project payloads are usually admitted with diagnostics
- projections and frontier actions may refuse incomplete nodes later

### 5. Core-path and off-path work must diverge

Core-path work is disciplined and atomic.

Off-path work is cheap and permissive.

The point is to avoid forcing every scrap of research through the full
benchmark/decision bureaucracy while still preserving it in the DAG.

### 6. Completed core-path experiments are atomic

A completed experiment exists only when all of these exist together:

- base checkpoint
- candidate checkpoint
- measured result
- terse note
- explicit verdict

The write surface should make that one atomic mutation, not a loose sequence of
low-level calls.

### 7. Checkpoints are git-backed

Dirty worktree snapshots are useful as descriptive context, but a completed
core-path experiment should anchor to a committed candidate checkpoint.

Off-path notes and research can remain lightweight and non-committal.

## Node Model

### Global envelope

The hard spine should be stable across projects. It includes at least:

- node id
- node class
- node track
- frontier id if any
- archived flag
- title
- summary
- schema namespace and version
- timestamps
- diagnostics
- hidden or visible annotations

This is the engine layer: the part that powers indexing, traversal, archiving,
default enumeration, and model-facing summaries.

### Project-local payload

Every project may define richer payload fields in:

`<project root>/.fidget_spinner/schema.json`

That file is a model-facing contract. It defines field names and soft
validation tiers without forcing global schema churn.

Per-field settings should express at least:

- presence: `required`, `recommended`, `optional`
- severity: `error`, `warning`, `info`
- role: `index`, `projection_gate`, `render_only`, `opaque`
- inference policy: whether the model may infer the field

These settings are advisory at ingest time and stricter at projection/action
time.

### Free-form annotations

Any node may carry free-form annotations.

These are explicitly sidecar, not primary payload. They are:

- allowed everywhere
- hidden from default enumeration
- useful as a scratchpad or escape hatch
- not allowed to become the only home of critical operational truth

If a fact matters to automation, comparison, or promotion, it must migrate into
the spine or project payload.

## Node Taxonomy

### Core-path node classes

These are the disciplined frontier-loop classes:

- `contract`
- `change`
- `run`
- `analysis`
- `decision`

### Off-path node classes

These are deliberately low-ceremony:

- `research`
- `enabling`
- `note`

They exist so the product can absorb real thinking instead of forcing users and
agents back into sprawling markdown.

## Frontier Model

The frontier is a derived operational view over the canonical DAG.

It answers:

- what objective is active
- what the current champion checkpoint is
- which candidate checkpoints are still alive
- how many completed experiments exist

The DAG answers:

- what changed
- what ran
- what evidence was collected
- what was concluded
- what dead ends and side investigations exist

That split is deliberate. It prevents "frontier state" from turning into a
second unofficial database.

## First Usable MVP

The first usable MVP is the first cut that can already replace a meaningful
slice of the markdown habit without pretending the whole full-product vision is
done.

### MVP deliverables

- per-project `.fidget_spinner/` state
- local SQLite backing store
- local blob directory
- typed Rust core model
- thin CLI for bootstrap and repair
- hardened stdio MCP host exposed from the CLI
- disposable MCP worker execution runtime
- bundled `fidget-spinner` base skill
- bundled `frontier-loop` skill
- low-ceremony off-path note and research recording
- atomic core-path experiment closure

### Explicitly deferred from the MVP

- long-lived `spinnerd`
- web UI
- remote runners
- multi-agent hardening
- aggressive pruning and vacuuming
- strong markdown migration tooling
- cross-project indexing

### MVP model-facing surface

The model-facing surface is a local MCP server oriented around frontier work.

The initial tools should be:

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

The important point is not the exact names. The important point is the shape:

- cheap read access to project and frontier context
- cheap off-path writes
- low-ceremony change capture
- one atomic "close the experiment" tool
- explicit operational introspection for long-lived agent sessions
- explicit replay boundaries so side effects are never duplicated by accident

### MVP skill posture

The bundled skills should instruct agents to:

1. inspect `system.health` first
2. bind the MCP session to the target project before project-local reads or writes
3. read project schema and frontier state
4. pull context from the DAG instead of giant prose dumps
5. use `note.quick` and `research.record` freely off path
6. use `change.record` before worktree thrash becomes ambiguous
7. use `experiment.close` to atomically seal core-path work
8. archive detritus instead of deleting it
9. use the base `fidget-spinner` skill for ordinary DAG work and add
   `frontier-loop` only when the task becomes a true autonomous frontier push

### MVP acceptance bar

The MVP is successful when:

- a project can be initialized locally with no hosted dependencies
- an agent can inspect frontier state through MCP
- an agent can inspect MCP health and telemetry through MCP
- an agent can record off-path research without bureaucratic pain
- a git-backed project can close a real core-path experiment atomically
- retryable worker faults do not duplicate side effects
- stale nodes can be archived instead of polluting normal enumeration
- a human can answer "what changed, what ran, what is the current champion,
  and why?" without doing markdown archaeology

## Full Product

The full product grows outward from the MVP rather than replacing it.

### Planned additions

- `spinnerd` as a long-lived local daemon
- local HTTP and SSE
- read-mostly graph and run inspection UI
- richer artifact handling
- model-driven pruning and archive passes
- stronger interruption recovery
- local runner backends beyond direct process execution
- optional global indexing across projects
- import/export and subgraph packaging

### Invariant for all later stages

No future layer should invalidate the MVP spine:

- DAG canonical
- frontier derived
- project-local store
- layered node model
- warning-heavy schema validation
- cheap off-path writes
- atomic core-path closure
