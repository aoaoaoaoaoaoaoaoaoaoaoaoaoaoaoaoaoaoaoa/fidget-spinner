---
name: fidget-spinner
description: Record an open-ended, measured inquiry in Fidget Spinner: frontiers, hypotheses, trials against the incumbent baseline, dispositions, reviews and baselines. Use when a project keeps its experimental record there, and with frontier-loop for autonomous optimization.
---

# Fidget Spinner

A frontier is an objective pursued under a gauge, the measurement contract. Hypotheses are claims; trials test them
against the incumbent baseline; dispositions settle them with a rationale; reviews study sets of hypotheses and end in
lessons. Accepting a baseline opens an epoch; changing the gauge opens a regime. The tool descriptions define the
arguments.

## Resume

Call `frontier.status` once. Its summary is the frontier's situation: the incumbent baseline and its readings, docket
progress, running and tested work, ready hypotheses, recent dispositions and lessons, and findings. Read a hypothesis
or review only when the next decision depends on it. The project defaults to the working directory; name the frontier
when the project has several.

Before recording a new idea, search for prior art: `frontier.status` with `text` and `scope: "project"` searches every
frontier of the project, concluded ones included.

## Record

Write each result and its consequences in one `frontier.patch`:

```jsonl
{"op":"trial","hypothesis":"lp.lattice-guard","note":"R77v2: cost ratio 0.976; closures unchanged; no case slower than 2x.","readings":{"r77_ratio":0.976},"artifacts":["7fec404f","/data/main/libgrid-evidence/lp-20261015/r77"]}
{"op":"accept","hypothesis":"lp.lattice-guard","rationale":"A 2.4% gain exceeds twice the margin with closures unchanged."}
{"op":"hypothesis","slug":"lp.lattice-guard-tighten","title":"Tighten clause-2 bounds with the measured lattice error","claim":"rocII-5-11's primal gap falls below 0.3 with no R77 closure lost.","after":["lp.lattice-guard"]}
```

- A trial's note states what ran and what happened, with the numbers that matter; its artifacts cite the commit and
  the evidence path. The trial is compared with the incumbent baseline unless `against` names another baseline.
- A rationale states why the disposition follows from the evidence. Reject what failed and shelve what is no longer
  worth pursuing; both are information.
- Record hypotheses as soon as they appear. A title suffices; a mechanism and a claim make one testable.
- An edge, through `after` or `edge`, makes its target wait until its source is settled.
- Readings use the gauge's metric keys. A value may carry a unit, such as `"1500 ms"`, and is converted. A key outside
  the gauge is kept on the trial as an unlisted reading and is never charted; when such a key recurs, declare it in
  the gauge or stop recording it.
- Void a trial or baseline whose evidence is invalid; it stays visible. Remove only mistakes.
- Rehearse a large patch with `dry_run`.

## Baselines, epochs and regimes

When an integration lands, record it as a baseline with its artifacts, readings and the hypotheses it adopts, and
accept it: it becomes the incumbent baseline and opens an epoch, and the unsettled hypotheses on the previous docket
carry over. A baseline's docket lists the hypotheses due against it.

When the measurement contract changes, whether its cohort, machine, limits, protocol or the meaning of a metric,
install a new regime with a reason and bridge it by remeasuring the incumbent baseline's commit under the new gauge.
Adding a metric is not a regime change.

## Findings

Findings are reminders computed from the state; they never block. Resolve one by recording what it reports missing,
or waive it with a note when the situation is intended.
