---
name: fidget-spinner
description: Use Fidget Spinner as the local system of record for source capture, hypothesis tracking, and experiment adjudication. Read health, schema, and frontier state first; keep off-path prose cheap; drive core-path work through hypothesis-owned experiments.
---

# Fidget Spinner

Use this skill when working inside a project initialized with Fidget Spinner or
when the task is to inspect or mutate the project DAG through the packaged MCP.

Start every session by reading `system.health`.

If the session is unbound, or bound to the wrong repo, call `project.bind`
with the target project root or any nested path inside that project.

If the target root exists and is empty, `project.bind` will bootstrap the local
store automatically.

Then read:

- `project.status`
- `tag.list`
- `frontier.list`
- `frontier.status` for the active frontier
- `experiment.list` if you may be resuming in-flight core-path work

Read `project.schema` only when payload authoring, validation rules, or local
field vocabulary are actually relevant. When in doubt, start with
`detail=concise` and widen to `detail=full` only if the summary is insufficient.

If you need more context, pull it from:

- `node.list`
- `node.read`
- `experiment.read`

## Posture

- the DAG is canonical truth
- frontier state is a derived projection
- project payload validation is warning-heavy at ingest
- annotations are sidecar and hidden by default
- `source` and `note` are off-path memory
- `hypothesis` and `experiment` are the disciplined core path

## Choose The Cheapest Tool

- `tag.add` when a new note taxonomy token is genuinely needed; every tag must carry a description
- `tag.list` before inventing note tags by memory
- `schema.field.upsert` when one project payload field needs to become canonical without hand-editing `schema.json`
- `schema.field.remove` when one project payload field definition should be purged cleanly
- `source.record` for imported source material, documentary context, or one substantial source digest; always pass `title`, `summary`, and `body`, and pass `tags` when the source belongs in a campaign/subsystem index
- `note.quick` for atomic reusable takeaways, always with an explicit `tags` list plus `title`, `summary`, and `body`; use `[]` only when no registered tag applies
- `hypothesis.record` before core-path work; every experiment must hang off exactly one hypothesis
- `experiment.open` once a hypothesis has a concrete slice and is ready to be tested
- `experiment.list` or `experiment.read` when resuming a session and you need to recover open experimental state
- `metric.define` when a project-level metric key needs a canonical unit, objective, or human description
- `run.dimension.define` when a new experiment slicer such as `scenario` or `duration_s` becomes query-worthy
- `run.dimension.list` before guessing which run dimensions actually exist in the store
- `metric.keys` before guessing which numeric signals are actually rankable; pass exact run-dimension filters when narrowing to one workload slice
- `metric.best` when you need the best closed experiments by one numeric key; pass `order` for noncanonical payload fields and exact run-dimension filters when comparing one slice
- `node.annotate` for scratch text that should stay off the main path
- `experiment.close` only for an already-open experiment and only when you have measured result, note, and verdict; attach `analysis` when the result needs explicit interpretation
- `node.archive` to hide stale detritus without deleting evidence
- `node.create` only as a true escape hatch

## Workflow

1. Preserve source texture with `source.record` only when keeping the source itself matters.
2. Extract reusable claims into `note.quick`.
3. State the intended intervention with `hypothesis.record`.
4. Open a live experiment with `experiment.open`.
5. Do the work.
6. Close the experiment with `experiment.close`, including metrics, verdict, and optional analysis.

Do not dump a whole markdown tranche into one giant prose node and call that progress.
If a later agent should enumerate it by tag or node list, it should usually be a `note.quick`.
If the point is to preserve or digest a source document, it should be `source.record`.
If the point is to test a claim, it should become a hypothesis plus an experiment.

## Discipline

1. Pull context from the DAG, not from sprawling prompt prose.
2. Prefer off-path records unless the work directly advances or judges the frontier.
3. Do not let critical operational truth live only in annotations.
4. If the MCP behaves oddly or resumes after interruption, inspect `system.health`
   and `system.telemetry` before pushing further.
5. Keep fetches narrow by default; widen only when stale or archived context is
   actually needed.
6. Treat metric keys as project-level registry entries and run dimensions as the
   first-class slice surface for experiment comparison; do not encode scenario
   context into the metric key itself.
7. A source node is not a dumping ground for every thought spawned by that source.
   Preserve one source digest if needed, then extract reusable claims into notes.
8. A hypothesis is not an experiment. Open the experiment explicitly; do not
   smuggle “planned work” into off-path prose.
9. The ledger is scientific, not git-forensic. Do not treat commit hashes as experiment identity.
10. Porcelain is the terse triage surface. Use `detail=full` only when concise
   output stops being decision-sufficient.
11. When the task becomes a true indefinite optimization push, pair this skill
    with `frontier-loop`.
