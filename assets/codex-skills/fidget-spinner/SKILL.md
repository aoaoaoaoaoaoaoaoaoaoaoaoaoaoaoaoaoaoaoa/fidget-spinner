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

Then read:

- `project.status`
- `project.schema`
- `frontier.list`
- `frontier.status` for the active frontier

If you need more context, pull it from:

- `node.list`
- `node.read`

## Posture

- the DAG is canonical truth
- frontier state is a derived projection
- project payload validation is warning-heavy at ingest
- annotations are sidecar and hidden by default

## Choose The Cheapest Tool

- `research.record` for exploratory work, design notes, dead ends, and enabling ideas
- `note.quick` for terse state pushes
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
6. When the task becomes a true indefinite optimization push, pair this skill
   with `frontier-loop`.
