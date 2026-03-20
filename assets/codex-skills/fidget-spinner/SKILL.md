---
name: fidget-spinner
description: Use Fidget Spinner as the local system of record for structured research and optimization work. Read health, schema, and frontier state first; prefer cheap off-path DAG writes; reserve atomic experiment closure for benchmarked core-path work.
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

Read `project.schema` only when payload authoring, validation rules, or local
field vocabulary are actually relevant. When in doubt, start with
`detail=concise` and widen to `detail=full` only if the summary is insufficient.

If you need more context, pull it from:

- `node.list`
- `node.read`

## Posture

- the DAG is canonical truth
- frontier state is a derived projection
- project payload validation is warning-heavy at ingest
- annotations are sidecar and hidden by default

## Choose The Cheapest Tool

- `tag.add` when a new note taxonomy token is genuinely needed; every tag must carry a description
- `tag.list` before inventing note tags by memory
- `schema.field.upsert` when one project payload field needs to become canonical without hand-editing `schema.json`
- `schema.field.remove` when one project payload field definition should be purged cleanly
- `research.record` for exploratory work, design notes, dead ends, and enabling ideas; always pass `title`, `summary`, and `body`, and pass `tags` when the research belongs in a campaign/subsystem index
- `note.quick` for terse state pushes, always with an explicit `tags` list plus `title`, `summary`, and `body`; use `[]` only when no registered tag applies
- `metric.define` when a project-level metric key needs a canonical unit, objective, or human description
- `run.dimension.define` when a new experiment slicer such as `scenario` or `duration_s` becomes query-worthy
- `run.dimension.list` before guessing which run dimensions actually exist in the store
- `metric.keys` before guessing which numeric signals are actually rankable; pass exact run-dimension filters when narrowing to one workload slice
- `metric.best` when you need the best closed experiments by one numeric key; pass `order` for noncanonical payload fields and exact run-dimension filters when comparing one slice
- `node.annotate` for scratch text that should stay off the main path
- `change.record` before core-path work
- `experiment.close` only when you have checkpoint, measured result, note, and verdict
- `node.archive` to hide stale detritus without deleting evidence
- `node.create` only as a true escape hatch

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
7. Porcelain is the terse triage surface. Use `detail=full` only when concise
   output stops being decision-sufficient.
8. When the task becomes a true indefinite optimization push, pair this skill
   with `frontier-loop`.
