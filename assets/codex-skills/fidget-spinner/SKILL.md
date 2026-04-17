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
- `hypothesis.record` whenever you get a plausible idea, mechanism, suspicion, or branch; hypotheses are cheap idea-capture nodes, not a ritual preamble to one experiment
- `hypothesis.update` when the title, summary, body, tags, influence parents, or active/retired state need tightening; retire stale hypotheses with `state=retired`
- `experiment.open` once a hypothesis has a concrete slice and is ready to be tested
- `experiment.list` or `experiment.read` when resuming a session and you need to recover open or recently closed state
- `experiment.update` while the experiment is still live and its summary, tags, or influence parents need refinement
- `experiment.close` only for an already-open experiment and only when you have measured result, verdict, and rationale; it requires a clean git worktree and records `HEAD` automatically, anchoring to `command.working_directory` when provided, so make a fast commit in the actual implementation worktree first and attach `analysis` only when the result needs interpretation beyond the rationale
- `experiment.nearest` when you need the nearest accepted, kept, rejected, or champion comparator for one structural slice
- `metric.define` when a project-level metric key needs a dimension, objective, aggregation, or description; use `display_unit` only as presentation, and keep the key focused on the measured concept rather than the unit
- `kpi.create` before `hypothesis.record` on a new frontier, promoting one existing metric into a frontier KPI; supervisor locks may reject KPI creation, and there is intentionally no bulk KPI mutation tool
- `kpi.list` or `metric.keys --scope kpi` before guessing which mandatory frontier metrics define the real hill
- `kpi.best` when you need the frontier ranking for one KPI metric
- `metric.keys --scope live` before guessing which numeric signals matter now
- `metric.best` when you need the best closed experiments by one numeric key; pass exact run-dimension filters when comparing one slice
- `run.dimension.define` when a new experiment slicer such as `instance` or `duration_s` becomes query-worthy
- `run.dimension.list` before guessing which run dimensions actually exist in the store

## Workflow

1. Ground through `frontier.open`.
2. Record ideas eagerly with `hypothesis.record` as they occur; there is no
   penalty for many hypotheses.
3. Choose or record the hypothesis that owns the concrete slice, then open a
   live experiment with `experiment.open`.
4. Do the work.
5. Make a fast commit for the recoverable implementation state before closing the experiment. Bypass heavyweight hooks when necessary; the bar here is recoverability, not release readiness.
6. Close the experiment with `experiment.close`, including dimensions, metrics, verdict, rationale, and optional analysis. Spinner will reject a dirty worktree and store the closing commit hash automatically.

## Discipline

1. `frontier.open` is the only overview dump. After that, walk the graph one selector at a time.
2. Pull context from hypotheses and experiments, not from sprawling prompt prose.
3. Treat tag policy errors as instructions, not transient failures: use the replacement tag named by the error, satisfy mandatory families, or ask the supervisor if the tag surface is locked.
4. If the MCP behaves oddly or resumes after interruption, inspect `system.health`
   and `system.telemetry` before pushing further.
5. Keep fetches narrow by default; slow is better than burning tokens.
6. Treat metric keys as project-level registry entries and run dimensions as the
   first-class slice surface for experiment comparison. Do not encode scenario
   context or Hungarian unit notation into the metric key: prefer
   `presolve_wallclock` with `dimension=time` over `presolve_ms`, and
   `report_size` with `dimension=bytes` over `report_bytes`. Report-time units
   belong on observations, not in the key.
7. A hypothesis is not an experiment and does not need to justify itself by
   immediately producing one. Open experiments explicitly; do not smuggle
   planned work or stray ideas into the frontier brief.
8. Experiments are the scientific record. If a fact matters later, it should
   usually live in a closed experiment outcome rather than in freeform text.
9. Spinner records the closing commit hash as a recoverability anchor, not as experiment identity.
10. If you run into an obviously stale hypothesis, retire it; stale cleanup is
    healthy and does not invalidate the experiments it once organized.
11. Porcelain is the terse triage surface. Use `detail=full` only when concise
   output stops being decision-sufficient.
12. When the task becomes a true indefinite optimization push, pair this skill
    with `frontier-loop`.
