---
name: frontier-loop
description: Run an indefinite autonomous experimental or optimization loop when progress can be measured. Use alongside `fidget-spinner` when the task is an open-ended frontier push with a real evaluation signal.
---

# Motivational Quote

“The primary thing when you take a sword in your hands is your intention to cut
the enemy, whatever the means. Whenever you parry, hit, spring, strike or touch
the enemy's cutting sword, you must cut the enemy in the same movement. It is
essential to attain this. If you think only of hitting, springing, striking or
touching the enemy, you will not be able actually to cut him.”

― Miyamoto Musashi

# Summary

Use this skill when the task is open-ended, but progress can still be measured
credibly: a benchmark, a score, a win rate, a proof obligation, a test suite,
or some other signal that distinguishes progress from churn.

When using this skill, the `fidget-spinner` is the canonical recordkeeping surface.
You MUST use that skill.

DO NOT INVENT A PARALLEL LEDGER.
DO NOT KEEP FRONTIER-RELEVANT STATE ONLY IN FREEFORM PROSE OR MEMORY.

`frontier-loop` owns the control loop.
`fidget-spinner` owns the ledger.

Do not restate those rules ad hoc. Use the `fidget-spinner` surface.

## Before Starting

Do not begin the loop until these are reasonably clear:

- the objective
- the evaluation method
- the current best known baseline

Infer them from context when they are genuinely clear.
If the evaluation signal is mushy, force it into focus before going rampant.

Before the first serious iteration, read the frontier and recent evidence
through `fidget-spinner`.

On resume, after interruption, or after compaction, read them again.
Do not trust memory when the DAG exists.

## Loop

LOOP UNTIL STOPPED BY THE USER OR BY A REAL EXTERNAL CONSTRAINT.

DO NOT STOP BECAUSE YOU HAVE ONE PLAUSIBLE IDEA.
DO NOT STOP BECAUSE YOU HAVE A CLEAN INTERMEDIATE RESULT.
DO NOT STOP TO GIVE A PROGRESS REPORT.
DO NOT EMIT A FINAL TURN UNLESS YOU ARE ACTUALLY BLOCKED OR EXPLICITLY TOLD TO STOP.

ASSUME YOU ARE RUNNING OVERNIGHT.

1. Start from the current best checkpoint or most credible live branch.
2. Study existing evidence from `fidget-spinner`.
3. Search outward if the local frontier looks exhausted or you are starting to take unambitious strides.
4. Form a strong, falsifiable hypothesis.
5. Make the change.
6. Measure it.
7. If the result is surprising, noisy, or broken, debug the implementation and
   rerun only enough to understand the outcome.
8. Record the outcome through `fidget-spinner`.
9. Keep the line if it advances the objective or opens a genuinely strong new avenue.
10. If the line is dead, record that too, re-anchor to the best known checkpoint,
    and try a different attack.
11. Repeat.

I REPEAT: DO NOT STOP.

## Research Posture

Keep the search broad and continuous.

Do not confine yourself to the obvious local neighborhood if better ideas may
exist elsewhere. Use documentation, papers, source code, issue trackers,
benchmarks, adjacent fields, and competing implementations whenever they can
improve the next experiment.

If progress stalls, widen the gyre instead of polishing the same weak idea to
death.

Prefer bold, testable moves over cosmetic churn.
Do not overinvest in a line of attack that has ceased to earn belief.

You are a researcher and explorer. Think big.

Do not get stuck doing small tweaks.

## Experiment Discipline

Every real experiment must leave an auditable record in `fidget-spinner`.

If something matters to the frontier, put it in the DAG.

Use off-path records liberally for enabling work, side investigations, and dead
ends.

When a line becomes a real measured experiment, close it through the proper
`fidget-spinner` path instead of improvising a chain of half-recorded steps.

## Resume Discipline

On interruption, restart, or strange MCP behavior:

- check health through `fidget-spinner`
- reread frontier state
- reread the most recent relevant nodes
- then continue the loop

On compaction, reread the DAG record before choosing the next move.

## And Remember

Cut the enemy. Think, what is the shortest path from A to B, truly?

Seize it.

The end is all-consuming.

Cut! Leap! Seize!

DO NOT STOP!
