---
name: fidget-spinner
description: Use Fidget Spinner as the local system of record for frontier grounding, hypothesis tracking, and experiment adjudication. Read health first, ground through frontier.open, and walk the graph deliberately one selector at a time.
---

# Fidget Spinner

Use this skill when working inside a project initialized with Fidget Spinner or
when the task is to inspect or mutate a frontier through the packaged MCP.

Start every session by reading `system.health`.

If the session is unbound, or bound to the wrong repo, call `project.bind`
with the target repo root, the repo’s `.git` directory, or any nested path
inside that project.

Do not create `.fidget_spinner` directories by hand. Spinner state is
centralized under `~/.local/state/fidget-spinner/`, not stored in the repo.

If the canonical project root does not have a Spinner store yet,
`project.bind` will bootstrap it automatically in the centralized state path.

Then read:

- `project.status`
- `tag.list`
- `frontier.list`
- `frontier.open` for the active frontier

`frontier.open` is the only sanctioned overview surface. It is allowed to give
you the frontier brief, active tags, live metrics, active hypotheses, and open
experiments in one call.

If you need more context, pull it from:

- `hypothesis.read`
- `experiment.read`

## Posture

- `frontier` is scope and grounding, not a graph vertex
- `hypothesis` and `experiment` are the true graph nodes
- hypotheses are free, eager, and wild: record plausible ideas as soon as they
  appear, even before they have become a polished experiment plan
- hypotheses and experiments are for KPI-directed scientific work; if a change
  is not meant to move or explain a frontier KPI, it usually does not belong in
  Spinner at all
- stale hypotheses are cheap too; retire an obviously dead or superseded one
  when you notice it rather than keeping the active surface ceremonially tidy
- every experiment has one mandatory owning hypothesis
- experiments and hypotheses may also cite other hypotheses or experiments as influence parents
- the frontier brief is the one sanctioned freeform overview
- token austerity matters more than convenience dumps

## Choose The Cheapest Tool

- `tag.add` when a new campaign or subsystem token is genuinely needed; every tag must carry a description, and supervisor locks may reject model-created tags
- `tag.list` before inventing tags by memory; it also reports supervisor-defined families, mandatory-family rules, locks, and stale-name guidance
- `frontier.update` when the objective, situation, roadmap, or unknowns need to change
- `frontier.query.schema` when you need the stable SQL view contract for advanced frontier-local mining; it lists the public `q_*` views and columns
- `frontier.query.sql` when the normal read tools are too narrow and you need a compact read-only SQL table over one frontier; query only `q_*` views, prefer small projections, and rely on the frontier envelope rather than adding frontier filters
- `hypothesis.record` whenever you get a plausible KPI-moving idea, mechanism, suspicion, or branch; hypotheses are cheap idea-capture nodes, not a ritual preamble to one experiment, and every new hypothesis must set `expected_yield` and `confidence` as crude `low|medium|high` vibe checks
- `hypothesis.update` when the title, summary, body, expected yield, confidence, tags, or influence parents need tightening; reprioritization should usually update `expected_yield` and/or `confidence` directly, and hypotheses are not archived, so clean stale wording/tags/parents in place and leave non-frontier visibility policy to the supervisor UI
- `experiment.open` once a hypothesis has a concrete KPI-relevant slice and is ready to be tested
- `experiment.list` or `experiment.read` when resuming a session and you need to recover open or recently closed state
- `experiment.update` while the experiment is still live and its summary, tags, or influence parents need refinement
- `experiment.close` only for an already-open experiment and only when you have measured result, verdict, and rationale; it requires a clean git worktree and records `HEAD` automatically, anchoring to `command.working_directory` when provided, so make a fast commit in the actual implementation worktree first and attach `analysis` only when the result needs interpretation beyond the rationale
- `experiment.nearest` when you need the nearest accepted, kept, rejected, or champion comparator for one structural slice
- `metric.define` when a project-level observed metric key needs a dimension, objective, aggregation, or description; use `display_unit` only as presentation, and keep the key focused on the measured concept rather than the unit. Synthetic metrics are supervisor-defined only: you may query them through `metric.keys`, `metric.best`, `kpi.best`, Results, and frontier SQL, but you must report their observed leaf metrics rather than reporting the synthetic key itself
- `kpi.create` before `hypothesis.record` on a new frontier, promoting one existing metric into a frontier KPI; supervisor locks may reject KPI creation, and there is intentionally no bulk KPI mutation tool
- `kpi.list` or `metric.keys --scope kpi` before guessing which mandatory frontier metrics define the real hill
- `kpi.best` when you need the frontier ranking for one KPI metric
- `metric.keys --scope live` before guessing which numeric signals matter now
- `metric.best` when you need the best closed experiments by one numeric key; pass exact condition filters when comparing one like-for-like slice
- `condition.define` when a new experimental condition such as `instance`, `profile`, `seed`, or `hardware` becomes query-worthy
- `condition.list` before guessing which conditions actually exist in the store

## Workflow

1. Ground through `frontier.open`.
2. Record KPI-relevant ideas eagerly with `hypothesis.record` as they occur;
   there is no penalty for many hypotheses, and each one should carry an
   explicit `expected_yield` and `confidence` vibe check.
3. Choose or record the hypothesis that owns the concrete KPI slice, then open
   a live experiment with `experiment.open`.
4. Do the work.
5. Make a fast commit for the recoverable implementation state before closing the experiment. Bypass heavyweight hooks when necessary; the bar here is recoverability, not release readiness.
6. Close the experiment with `experiment.close`, including conditions, metrics, verdict, rationale, and optional analysis. Spinner will reject a dirty worktree and store the closing commit hash automatically.

## Discipline

1. `frontier.open` is the only overview dump. After that, walk the graph one selector at a time.
2. Pull context from hypotheses and experiments, not from sprawling prompt prose.
3. Treat tag policy errors as instructions, not transient failures: use the replacement tag named by the error, satisfy mandatory families, or ask the supervisor if the tag surface is locked.
4. If the MCP behaves oddly or resumes after interruption, inspect `system.health`
   and `system.telemetry` before pushing further.
5. Keep fetches narrow by default; slow is better than burning tokens.
6. Treat metric keys as project-level registry entries and conditions as the
   first-class setup surface for experiment comparison. Conditions describe
   like-for-like context such as instance, profile, implementation, seed,
   timeout, hardware, or dataset; measured outcomes belong in metrics. Do not
   encode scenario context or Hungarian unit notation into the metric key: prefer
   `presolve_wallclock` with `dimension=time` over `presolve_ms`, and
   `report_size` with `dimension=bytes` over `report_bytes`. Report-time units
   belong on observations, not in the key.
   Synthetic metrics are formulas over metric quantities; addition/subtraction
   requires the same quantity, multiplication/division compose quantities, and
   geometric means use exact rational exponents. They are readable but not
   definable from MCP.
7. A hypothesis is not an experiment and does not need to justify itself by
   immediately producing one. Open experiments explicitly; do not smuggle
   planned work or stray ideas into the frontier brief.
8. Not all implementation work deserves a Spinner node. Pure tooling,
   instrumentation, telemetry, harness repair, refactors, and other enabling
   changes should usually live only in git unless the change itself is being
   tested as a KPI-moving mechanism.
9. If you cannot name the KPI the work is trying to move or explain, do not
   open an experiment for it.
10. Experiments are the scientific record for KPI-directed work. If a KPI-relevant
    fact matters later, it should usually live in a closed experiment outcome
    rather than in freeform text.
11. Spinner records the closing commit hash as a recoverability anchor, not as experiment identity.
12. If you reprioritize a hypothesis, update `expected_yield` and/or
    `confidence` instead of trying to smuggle the new stance into prose alone.
13. If you run into an obviously stale hypothesis, retire it; stale cleanup is
    healthy and does not invalidate the experiments it once organized.
14. Porcelain is the terse triage surface. Use `detail=full` only when concise
    output stops being decision-sufficient.
15. Raw SQL is an escape hatch for trusted, advanced frontier-local inspection,
    not a second write API. Start with `frontier.query.schema`, query only the
    stable `q_*` views, keep result sets narrow, and never expect physical table
    names or cross-frontier data to exist.
16. When the task becomes a true indefinite optimization push, pair this skill
    with `frontier-loop`.
