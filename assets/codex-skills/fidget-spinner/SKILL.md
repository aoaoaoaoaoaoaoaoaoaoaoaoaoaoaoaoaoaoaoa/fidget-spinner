---
name: fidget-spinner
description: Keep an inquiry's record in Fidget Spinner across sessions: frontiers, isets (questions judged once), trials (attempts planned, then reported once against a control baseline), schemas, baselines and supervision. Use when a project keeps its research or experimental record there.
---

# Fidget Spinner

Fidget Spinner is an inquiry's memory across compactions, handoffs and sessions: what is open, who pursues it, what
was judged over which evidence, and why. It records; it never runs experiments, verifies evidence or blocks method.
Refusals protect only the record's representation; warnings and findings flag drift and never block. Admissibility of
evidence and method are project policy.

Work is trials and isets. A trial is an attempt: planned, amended by `edit`, and reported once against a control
baseline. An iset (integration set) poses a question over member work and is judged once; its answer may land either
way. Inputs (`after`) order work; membership is not dependency. Every record resolves at most once and is voided at
most once: nothing is reopened, corrected or unvoided, and a later answer is a new iset.

The tool descriptions define the operations; `schema.read` without `schema` gives the definition grammar.
`frontier.patch` requires a frontier slug; every tool accepts an explicit project path. Porcelain is the default;
request `render: "json"` only for structured input.

## Start a frontier

A patch whose first operation is `frontier` creates it. Measurement is optional; when work is measured, install the
gauge and the first baseline in the same patch, and define the schemas the frontier's work will use:

```json
[
{"op":"frontier","objective":"Halve cold-start latency of the request path without changing responses."},
{"op":"regime","contract":"Corpus fixtures/requests-v1, 200 requests, one worker, release build, median of 5 runs.","metrics":[{"key":"p50_latency","unit":"ms","better":"lower","kpi":true}]},
{"op":"baseline","slug":"b1","artifacts":["a1b2c3d"],"readings":{"p50_latency":"84 ms"}},
{"op":"elect","baseline":"b1","rationale":"First measurement of main."},
{"op":"define","schema":"hypothesis","definition":{"carrier":"iset","title":"Hypothesis",
  "data":{"claim":{"type":"text","presence":"required","show":"headline"},"mechanism":{"type":"text","presence":"optional"}},
  "answer":{"verdict":{"type":{"enum":["holds","fails","unclear"]},"presence":"required","show":"badge"},"rationale":{"type":"text","presence":"required"}}}},
{"op":"define","schema":"attempt","definition":{"carrier":"trial","title":"Attempt",
  "plan":{"change":{"type":"text","presence":"required","show":"headline"}},
  "report":{"outcome":{"type":"text","presence":"required"}}}}
]
```

The contract names everything a reading depends on: cohort, envelope and protocol. Declare only the metrics decisions
use; a KPI is charted and needs a direction. The hypothesis schema is a template for frontiers that want a
conventional verdict; FS reads no verdict from any answer. Untyped work is allowed; a schema makes data checkable and
gives the navigator headlines and badges.

## Resume

Call `frontier.status` once; it is the memory. The summary gives the incumbent baseline and its KPIs, counts by
state, running work with owners, tested isets awaiting judgment, ready work, recent judgments and findings. Read a
record with `record.read` only when the next decision depends on it. The project defaults to the working directory;
name the frontier when the project has several unconcluded ones.

Before recording a new question, search for prior art: `frontier.status` with
`{"scope":"project","filter":{"text":"cache"}}` searches every frontier of the project, concluded ones included.
`filter.under` scopes list views to an iset's members at any depth. Reuse a cursor only for its original query; on a
cursor refusal, rerun without it.

## Plan, report, judge

Plan before work leaves the context, at a dispatch, handoff or compaction, so the record shows what was intended
before it ran:

```json
[
{"op":"iset","slug":"cache.templates","schema":"hypothesis","data":{"claim":"Caching parsed templates cuts p50 latency without changing responses."}},
{"op":"plan","into":["cache.templates"],"schema":"attempt","data":{"change":"Memoize template parsing per worker."}},
{"op":"assign","work":"cache.templates#1","owner":"worker-2","instructions":"Run the requests-v1 protocol; report p50_latency and any response diffs."}
]
```

- A trial without a name joins its first `into` iset and is named `slug#n`; the receipt names it. Its control
  defaults to the incumbent baseline; `control` names another, and `treatment` may name what ran: a baseline or an
  opaque identity such as a commit.
- `edit` amends an open trial's plan, and every revision stays readable. Editing work that existed before the patch
  names its `rev`, which reads show; a stale `rev` is refused with the actual revision.
- `start` and `stop` assert execution. `assign` binds an owner to the work's current revision; a later write raises
  `stale` until the work is reassigned.
- `report` resolves a trial once with readings, artifacts (commit and evidence path) and data. Reporting a name that
  names no trial records a retrospective trial. Name the plan revision executed when it is not the current one, or
  `"unknown"`.
- `judge` answers an iset once, over the done work its membership reaches. Pass `seen` with the seq you read: the
  judgment is refused if anything it consumes changed since, and reads show it as guarded. Judging while members are
  open draws a warning.
- Readings use the gauge's keys; a value may carry a unit, such as `"1500 ms"`, converted within its kind. Other keys
  are kept as unlisted readings and never charted.
- Rehearse a large patch with `dry_run`. A refusal names the operation index, name and field; nothing applies until
  the whole patch does.

## Correct and sweep

Nothing resolved is rewritten. A misrecorded report takes a new trial whose data cites its predecessor, as in
`{"$fs":"ref","kind":"trial","id":"cache.templates#1"}`; new understanding takes a new iset containing the old answer
and the new evidence. Void invalid evidence with a reason: the receipt lists the resolutions resting on it and the
work waiting on it. Whoever voids sweeps, by judging a successor iset over the survivors or voiding what falls. A void
reaches one hop and its marks never clear. Remove only mistakes; history redacts removed records.

## Baselines and regimes

When an integration lands, record it as a baseline with its artifacts and readings and `elect` it, adopting the isets it
carries: it becomes the incumbent and opens an epoch, named by its baseline and election time. A baseline's artifacts
freeze at its first election or first use as a report's control or treatment; its readings freeze once an election, or a
report's control without `control_readings`, consumes them. The note never freezes.

When the measurement contract changes, whether its cohort, machine, limits, protocol or a metric's meaning, install a
new regime with a reason and a bridge: the incumbent's artifacts remeasured under the new gauge. Adding a metric is a
`gauge` edit, not a regime.

## Findings and conclusion

Findings are facts computed from the state: unsettled or unjudged isets, nonconforming or unvalidated data, stale
assignments, superseded defaulted controls, unbridged regimes, recurring unlisted readings, and an incumbent missing a
KPI reading. They never block and clear when their condition ends; resolve one by recording what it reports missing,
or let an intended exception stand.

`conclude` records the frontier's lesson once; afterwards only `void`, `remove` and `rename` apply. Archiving is the
owner's, in the navigator at http://127.0.0.1:8913/, which draws each frontier's isets and trials, its epochs and
regimes, and one chart per KPI.
