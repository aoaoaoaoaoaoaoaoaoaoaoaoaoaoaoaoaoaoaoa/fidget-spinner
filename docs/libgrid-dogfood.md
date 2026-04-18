# Libgrid Dogfood Plan

## Why Libgrid

`libgrid` is the right first serious dogfood target because it has exactly the
failure mode Fidget Spinner is designed to kill:

- long autonomous optimization loops
- heavy benchmark slicing
- worktree churn
- huge markdown logs that blur intervention, result, and verdict

That is the proving ground.

## Immediate Goal

The goal is not “ingest every scrap of prose.”

The goal is to replace the giant freeform experiment log with a machine in
which the active frontier, live hypotheses, current experiments, verdicts, and
best benchmark lines are explicit and queryable.

## Mapping Libgrid Work Into The Model

### Frontier

One optimization objective becomes one frontier:

- root cash-out
- LP spend reduction
- primal improvement
- search throughput
- cut pipeline quality

The frontier brief should answer where the campaign stands right now, not dump
historical narrative.

### Hypothesis

A hypothesis should capture one concrete intervention claim:

- terse title
- one-line summary
- one-paragraph body

If the body wants to become a design memo, it is too large.

### Experiment

Each measured slice becomes one experiment under exactly one hypothesis.

The experiment closes with:

- conditions such as `instance`, `profile`, `duration_s`
- primary metric
- supporting metrics
- verdict: `accepted | kept | parked | rejected`
- rationale
- optional analysis

If a tranche doc reports multiple benchmark slices, it should become multiple
experiments, not one prose blob.

## Libgrid Workflow

### 1. Ground

1. Bind the MCP to the libgrid worktree.
2. Read `frontier.open`.
3. Decide whether the next move is a new hypothesis, a new experiment on an
   existing hypothesis, or a frontier brief update.

### 2. Start a line of attack

1. Record a hypothesis.
2. Open one experiment for the concrete slice being tested.

### 3. Execute

1. Modify the worktree.
2. Run the benchmark protocol.
3. Make a fast commit for the recoverable implementation state. Bypass heavy hooks if they get in the way; the point is recovery, not polish.
4. Close the experiment atomically with parsed metrics and an explicit verdict.

### 4. Judge and continue

1. Use `accepted`, `kept`, `parked`, and `rejected` honestly.
2. Let the frontier brief summarize the current strategic state.

## Benchmark Discipline

For `libgrid`, the minimum trustworthy record is:

- conditions
- primary metric
- supporting metrics that materially explain the verdict
- rationale
- closing commit hash

This is the minimum needed to prevent “I think this was faster” folklore.

## Active Metric Discipline

`libgrid` will accumulate many niche metrics.

The hot path should care about live metrics only: the metrics touched by the
active experimental frontier and its immediate comparison set. Old, situational
metrics may remain in the registry without dominating `frontier.open`.

## Acceptance Bar

Fidget Spinner is ready for serious `libgrid` use when:

- an agent can run for hours without generating a markdown graveyard
- `frontier.open` gives a truthful, bounded orientation surface
- active hypotheses and open experiments are obvious
- closed experiments carry parsed metrics rather than prose-only results
- the system feels like a machine for evidence rather than a diary with better
  typography
