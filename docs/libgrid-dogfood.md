# Libgrid Dogfood Plan

## Why Libgrid

`libgrid` is the right first serious dogfood target because it has exactly the
failure mode Fidget Spinner is designed to kill:

- long autonomous optimization loops
- heavy worktree usage
- benchmark-driven decisions
- huge markdown logs that blur evidence, narrative, and verdicts

That is the proving ground.

## Immediate MVP Goal

The MVP does not need to solve all of `libgrid`.

It needs to solve this specific problem:

replace the giant freeform experiment log with a machine in which the active
frontier, the accepted lines, the live evidence, and the dead ends are all
explicit and queryable.

When using a global unbound MCP session from a `libgrid` worktree, the first
project-local action should be `project.bind` against the `libgrid` worktree
root or any nested path inside it. The session should not assume the MCP host's
own repo.

## Mapping Libgrid Work Into The Model

### Frontier

One optimization objective becomes one frontier:

- improve MILP solve quality
- reduce wall-clock time
- reduce LP pressure
- improve node throughput
- improve best-bound quality

### Contract node

The root contract should state:

- objective in plain language
- benchmark suite set
- primary metric
- supporting metrics
- promotion criteria

### Change node

Use `hypothesis.record` to capture:

- what hypothesis is being tested
- what benchmark suite matters
- any terse sketch of the intended delta

### Run node

The run node should capture:

- exact command
- cwd
- backend kind
- run dimensions
- resulting metrics

### Decision node

The decision should make the verdict explicit:

- accepted
- kept
- parked
- rejected

### Off-path nodes

Use these freely:

- `source` for ideas, external references, algorithm sketches
- `source` for scaffolding that is not yet a benchmarked experiment
- `note` for quick observations

This is how the system avoids forcing every useful thought into experiment
closure.

## Suggested Libgrid Project Schema

The `libgrid` project should eventually define richer payload conventions in
`.fidget_spinner/schema.json`.

The MVP does not need hard rejection. It does need meaningful warnings.

Good first project fields:

- `hypothesis` on `hypothesis`
- `benchmark_suite` on `hypothesis` and `run`
- `body` on `hypothesis`, `source`, and `note`
- `comparison_claim` on `analysis`
- `rationale` on `decision`

Good first metric vocabulary:

- `wall_clock_s`
- `solved_instance_count`
- `nodes_expanded`
- `best_bound_delta`
- `lp_calls`
- `memory_bytes`

## Libgrid MVP Workflow

### 1. Seed the frontier

1. Initialize the project store.
2. Create a frontier contract.

### 2. Start a line of attack

1. Read the current frontier and the recent DAG tail.
2. Record a `hypothesis`.
3. If needed, attach off-path `source` or `note` nodes first.

### 3. Execute one experiment

1. Modify the worktree.
2. Run the benchmark protocol.
3. Close the experiment atomically.

### 4. Judge and continue

1. Mark the line accepted, kept, parked, or rejected.
2. Archive dead ends instead of leaving them noisy and active.
3. Repeat.

## Benchmark Discipline

For `libgrid`, the benchmark evidence needs to be structurally trustworthy.

The MVP should always preserve at least:

- run dimensions
- primary metric
- supporting metrics
- command envelope

This is the minimum needed to prevent "I think this was faster" folklore.

## What The MVP Can Defer

These are useful but not required for the first real dogfood loop:

- strong markdown migration
- multi-agent coordination
- rich artifact bundling
- pruning or vacuum passes beyond archive
- UI-heavy analysis

The right sequence is:

1. start a clean front
2. run new work through Fidget Spinner
3. backfill old markdown only when it is worth the effort

## Repo-Local Dogfood Before Libgrid

This repository itself is a valid off-path dogfood target even though it is not
a benchmark-heavy repo.

That means we can already use it to test:

- project initialization
- schema visibility
- frontier creation and status projection
- off-path source recording
- hidden annotations
- MCP read and write flows

What it cannot honestly test is heavy benchmark ingestion and the retrieval
pressure that comes with it. That still belongs in a real optimization corpus
such as the `libgrid` worktree.

## Acceptance Bar For Libgrid

Fidget Spinner is ready for serious `libgrid` use when:

- an agent can run for hours without generating a giant markdown graveyard
- the operator can identify accepted, kept, parked, and rejected lines mechanically
- each completed experiment has result, note, and verdict
- off-path side investigations stay preserved but do not pollute the core path
- the system feels like a machine for evidence rather than a diary with better
  typography
