---
name: frontier-loop
description: Run an autonomous research or optimization loop against a measurable objective, choosing, testing and settling hypotheses against the incumbent baseline. Use for open-ended frontier work with a credible evaluation signal; fidget-spinner keeps the record.
---

# Frontier loop

This skill decides what to try next, how to test it and when to change course. `$fidget-spinner` keeps the record:
every hypothesis, trial, disposition and baseline goes through it, and after an interruption or compaction its status
summary, not memory, is the state of the work.

## Before the first trial

The objective, the gauge and the incumbent baseline must be known. Take them from the frontier or the context. When
the evaluation signal is vague, define it first: readings taken under an unclear measurement cannot be compared. A new
frontier starts with its objective, its first regime and a measured baseline (`$fidget-spinner`, Start a frontier).

## Iteration

1. Choose the hypothesis whose test is expected to move a KPI most per unit of cost, counting what a negative result
   would teach. Start from the incumbent baseline or the most credible live line, and search the project's prior art
   before testing an idea that may already have been tried.
2. State the claim before measuring: the result that would accept the hypothesis. A bar chosen after the result is
   not evidence.
3. Measure against the incumbent baseline. Debug a surprising, noisy or broken result only far enough to understand
   it. Replicate a difference that lies within the measurement's noise before settling on it.
4. Settle the hypothesis: accept it if it met its claim, reject it if it failed, and shelve it if it no longer earns
   its cost. Record the follow-up hypotheses the result suggests in the same patch.
5. When accepted changes are integrated, record the new baseline and accept it; later trials are compared against it.
6. When a line is dead, return to the incumbent baseline and attack from another direction.

## Posture

- Search beyond the local neighbourhood: documentation, papers, source code, issue trackers, benchmarks, adjacent
  fields and competing implementations. When progress stalls or the steps become small, widen the search instead of
  polishing the same idea.
- Prefer bold, testable moves to cosmetic changes, and stop investing in a line once it no longer earns belief.
- Hypotheses are cheap to record. Record mechanisms, suspicions and branches as soon as they appear, without waiting
  until one is ready to test, and shelve stale ones when you notice them.

## What counts as a trial

Not every code change is a trial. Tooling, instrumentation, harness repair and refactoring are implementation work;
record them only when the change itself is tested as a mechanism expected to move or explain a KPI. A change for which
no KPI can be named is not a trial.

When the measurement itself changes, whether its cohort, machine, limits, protocol or the meaning of a metric, record
a regime change and remeasure what the next decision depends on: readings from different regimes are not comparable.
