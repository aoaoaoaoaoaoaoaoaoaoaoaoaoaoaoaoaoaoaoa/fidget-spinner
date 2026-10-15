---
name: frontier-loop
description: Run an indefinite autonomous experimental or optimization loop when progress can be measured. Use with fidget-spinner, which holds the record, when the task is an open-ended frontier push with a real evaluation signal.
---

# Frontier loop

Use this loop when the task is open-ended but progress can be measured credibly: a benchmark, a score, a win rate, a
proof obligation, a test suite, or another signal that separates progress from churn. Fidget Spinner holds the
record; frontier state belongs there, not in prose or memory.

## Before starting

The objective, the gauge (what is measured, how, and on what) and the incumbent baseline must be clear. Infer them
from context when they are; otherwise settle them first, because a reading is comparable only within its regime.

Read `frontier.status` before the first iteration and again after any interruption or compaction. Pass the project
when the server is elsewhere, and name the frontier when the project has several.

## Loop

Continue until the user or a real external constraint stops the work. One plausible idea, a clean intermediate result
or a progress report is no reason to stop; end the turn only when blocked or told to stop.

1. Start from the incumbent baseline or the most credible live line.
2. Study the evidence: the status summary, the related hypotheses, and prior art across the project with
   `frontier.status {"scope":"project","filter":{"text":"LNS"}}`. Old-regime trial counts are labelled; remeasure
   when the next decision requires current-regime evidence.
3. Search outward, through documentation, papers, source code and competing implementations, when the local frontier
   looks exhausted or the steps are getting small.
4. Record promising mechanisms as hypotheses as soon as they appear.
5. Measure the chosen hypothesis. Debug a surprising, noisy or broken result only far enough to understand it.
6. Record the trial, its disposition and any follow-up hypotheses in one patch.
7. When accepted changes are integrated, record and accept the new baseline.
8. When a line is dead, reject or shelve it, return to the incumbent baseline and attack differently.

## Posture

Prefer bold, testable moves to cosmetic churn, and stop investing in a line that no longer earns belief. When progress
stalls, widen the search rather than polishing the same idea.

Not every code change is a trial. Tooling, instrumentation, harness repair and refactoring are implementation work;
record them only when the change itself is tested as a mechanism expected to move or explain a KPI. A change for
which no KPI can be named is not a trial.
