# Fidget Spinner Product Spec

## Thesis

Fidget Spinner is a local-first, agent-first frontier ledger for autonomous
optimization work.

It is not a notebook. It is not a generic DAG memory. It is not an inner
platform for git. It is a hard experimental spine whose job is to preserve
scientific truth with enough structure that agents can resume work without
reconstructing everything from prose.

The package is deliberately two things at once:

- a local MCP-backed frontier ledger
- bundled skills that teach agents how to drive that ledger

Those two halves are one product and should be versioned together.

## Product Position

This is a machine for long-running frontier work in local repos.

Humans and agents should be able to answer:

- what frontier is active
- which hypotheses are live
- which experiments are still open
- what the latest accepted, kept, parked, and rejected outcomes are
- which metrics matter right now

without opening a markdown graveyard.

## Non-Goals

These are explicitly out of scope for the core product:

- hosted identity
- cloud tenancy
- billing or credits
- chat as the system of record
- mandatory remote control planes
- replacing git
- storing or rendering bulky external prose

Git remains the code substrate. Fidget Spinner is the experimental ledger.

## Locked Design Decisions

### 1. The ledger is austere

The only freeform overview surface is the frontier brief, read through
`frontier.open`.

Everything else should require deliberate traversal one selector at a time.
Slow is better than burning tokens on giant feeds.

### 2. The ontology is small

The canonical object families are:

- `frontier`
- `hypothesis`
- `experiment`

There are no canonical `note` or `source` ledger nodes.

### 3. Frontier is scope, not a graph vertex

A frontier is a named scope and grounding object. It owns:

- objective
- status
- brief

And it partitions hypotheses and experiments.

### 4. Hypothesis and experiment are the true graph vertices

A hypothesis is a terse intervention claim.

An experiment is a stateful scientific record. Every experiment has:

- one mandatory owning hypothesis
- optional influence parents drawn from hypotheses or experiments

This gives the product a canonical tree spine plus a sparse influence network.

### 5. Experiment closure is atomic

A closed experiment exists only when all of these exist together:

- conditions
- primary metric
- verdict
- rationale
- optional supporting metrics
- optional analysis
- commit hash captured from a clean git `HEAD`

Closing an experiment is one atomic mutation, not a loose pile of lower-level
writes. Spinner must reject closes from a dirty worktree.

### 6. Live metrics are derived

The hot-path metric surface is not “all metrics that have ever existed.”

The hot-path metric surface is the derived live set for the active frontier.
That set should stay small, frontier-relevant, and queryable.

## Canonical Data Model

### Frontier

Frontier is a scope/partition object with one mutable brief.

The brief is the sanctioned grounding object. It should stay short and answer:

- situation
- roadmap
- unknowns

### Hypothesis

A hypothesis is a disciplined claim:

- title
- summary
- exactly one paragraph of body
- tags
- influence parents

It is not a design doc and not a catch-all prose bucket.

### Experiment

An experiment is a stateful object:

- open while the work is live
- closed when the result is in

A closed experiment stores:

- conditions
- primary metric
- supporting metrics
- verdict: `accepted | kept | parked | rejected`
- rationale
- optional analysis
- closing commit hash

## Token Discipline

`frontier.open` is the only sanctioned overview dump. It should return:

- frontier brief
- active tags
- KPIs
- live metric keys
- active hypotheses with deduped current state
- open experiments

After that, the model should walk explicitly:

- `hypothesis.read`
- `experiment.read`

No broad list surface should dump large prose.

## Storage

Every project owns a private centralized state root:

```text
~/.local/state/fidget-spinner/projects/<project>-<stable-id>/
    state.sqlite
```

There is no required database service. The project root remains the binding
identity, but Spinner state stays out of the git worktree.

## MVP Surface

The current model-facing surface is:

- `system.health`
- `system.telemetry`
- `project.bind`
- `project.status`
- `tag.add`
- `tag.list`
- `frontier.create`
- `frontier.list`
- `frontier.read`
- `frontier.open`
- `frontier.update`
- `frontier.history`
- `hypothesis.record`
- `hypothesis.list`
- `hypothesis.read`
- `hypothesis.update`
- `hypothesis.history`
- `experiment.open`
- `experiment.list`
- `experiment.read`
- `experiment.update`
- `experiment.close`
- `experiment.nearest`
- `experiment.history`
- `metric.define`
- `metric.keys`
- `metric.best`
- `condition.define`
- `condition.list`

## Explicitly Deferred

Still out of scope:

- remote runners
- hosted multi-user control planes
- giant auto-generated context dumps
- replacing git or reconstructing git inside the ledger
