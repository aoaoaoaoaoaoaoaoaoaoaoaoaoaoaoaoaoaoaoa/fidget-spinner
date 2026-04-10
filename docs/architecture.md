# Fidget Spinner Architecture

## Runtime Shape

The current runtime is intentionally simple and hardened:

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
    |   +-- frontier / hypothesis / experiment / artifact services
    |   +-- navigator projections
    |
    v
~/.local/state/fidget-spinner/projects/<project>-<stable-id>/
```

There is no long-lived daemon yet. The CLI binary owns the stdio host and the
local navigator.

## Package Boundary

The package contains three coupled crates:

- `fidget-spinner-core`
- `fidget-spinner-store-sqlite`
- `fidget-spinner-cli`

And two bundled agent assets:

- `assets/codex-skills/fidget-spinner/SKILL.md`
- `assets/codex-skills/frontier-loop/SKILL.md`

These are one release unit.

## Storage Topology

Every initialized project owns a private centralized state root:

```text
~/.local/state/fidget-spinner/projects/<project>-<stable-id>/
    project.json
    state.sqlite
```

Why this shape:

- git can no longer clobber the live ledger by accident
- migrations stay local to one deterministic per-project path
- no database service is required
- git remains the code substrate instead of being mirrored into Spinner

## Canonical Types

### Frontier

Frontier is a scope and grounding object, not a graph vertex.

It owns:

- label
- objective
- status
- brief

And it partitions hypotheses and experiments.

### Hypothesis

Hypothesis is a true graph vertex. It carries:

- title
- summary
- exactly one paragraph of body
- tags
- influence parents

### Experiment

Experiment is also a true graph vertex. It carries:

- one mandatory owning hypothesis
- optional influence parents
- title
- summary
- tags
- status
- outcome when closed

The outcome contains:

- backend
- command envelope
- run dimensions
- primary metric
- supporting metrics
- verdict
- rationale
- optional analysis
- closing commit hash captured from clean git `HEAD`

### Artifact

Artifact is metadata plus a locator for an external thing. It attaches to
frontiers, hypotheses, and experiments. Spinner never reads or stores the
artifact body.

## Graph Semantics

Two relations matter:

### Ownership

Every experiment has exactly one owning hypothesis.

This is the canonical tree spine.

### Influence

Hypotheses and experiments may both cite later hypotheses or experiments as
influence parents.

This is the sparse DAG over the canonical tree.

The product should make the ownership spine easy to read and the influence
network available without flooding the hot path.

## SQLite Shape

The store is normalized around the new ontology:

- `frontiers`
- `frontier_briefs`
- `hypotheses`
- `experiments`
- `vertex_influences`
- `artifacts`
- `artifact_attachments`
- `metric_definitions`
- `run_dimension_definitions`
- `experiment_metrics`
- `events`

The important boundary is this:

- hypotheses and experiments are the scientific ledger
- artifacts are reference sidecars
- frontier projections are derived

## Presentation Model

The system is designed to be hostile to accidental context burn.

`frontier.open` is the only sanctioned overview dump. It should be enough to
answer:

- where the frontier stands
- which tags are active
- which metrics are live
- which hypotheses are active
- which experiments are open

Everything after that should require deliberate traversal:

- `hypothesis.read`
- `experiment.read`
- `artifact.read`

Artifact reads stay metadata-only by design.

## Replay Model

The MCP host owns:

- the public JSON-RPC session
- initialize-before-use semantics
- replay contracts
- health and telemetry
- host rollout

The worker owns:

- project-store access
- tool execution
- typed success and fault results

Reads and safe operational surfaces may be replayed after retryable worker
faults. Mutating operations are never auto-replayed unless they are explicitly
designed to be safe.

## Navigator

The local navigator mirrors the same philosophy:

- root page lists frontiers
- frontier page is the only overview page
- hypothesis and experiment pages are detail reads
- artifacts are discoverable but never expanded into body dumps

The UI should help a model or operator walk the graph conservatively, not tempt
it into giant all-history feeds.
