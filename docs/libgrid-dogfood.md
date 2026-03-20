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
frontier, the current champion, the candidate evidence, and the dead ends are
all explicit and queryable.

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
- what base checkpoint it starts from
- what benchmark suite matters
- any terse sketch of the intended delta

### Run node

The run node should capture:

- exact command
- cwd
- backend kind
- benchmark suite
- code snapshot
- resulting metrics

### Decision node

The decision should make the verdict explicit:

- promote to champion
- keep on frontier
- revert to champion
- archive dead end
- needs more evidence

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
- `base_checkpoint_id` on `hypothesis`
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
3. Capture the incumbent git checkpoint if available.

### 2. Start a line of attack

1. Read the current frontier and the recent DAG tail.
2. Record a `hypothesis`.
3. If needed, attach off-path `source` or `note` nodes first.

### 3. Execute one experiment

1. Modify the worktree.
2. Commit the candidate checkpoint.
3. Run the benchmark protocol.
4. Close the experiment atomically.

### 4. Judge and continue

1. Promote the checkpoint or keep it alive.
2. Archive dead ends instead of leaving them noisy and active.
3. Repeat.

## Benchmark Discipline

For `libgrid`, the benchmark evidence needs to be structurally trustworthy.

The MVP should always preserve at least:

- benchmark suite identity
- primary metric
- supporting metrics
- command envelope
- host/worktree metadata
- git commit identity

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
currently a git repo.

That means we can already use it to test:

- project initialization
- schema visibility
- frontier creation without a champion
- off-path source recording
- hidden annotations
- MCP read and write flows

What it cannot honestly test is full git-backed core-path experiment closure.
That still belongs in a real repo such as the `libgrid` worktree.

## Acceptance Bar For Libgrid

Fidget Spinner is ready for serious `libgrid` use when:

- an agent can run for hours without generating a giant markdown graveyard
- the operator can identify the champion checkpoint mechanically
- each completed experiment has checkpoint, result, note, and verdict
- off-path side investigations stay preserved but do not pollute the core path
- the system feels like a machine for evidence rather than a diary with better
  typography
