# Metric And KPI Governance

Status: current policy reference.

A metric is a project-global instrument. A KPI is a frontier-local edge saying
that one metric defines the hill.

There is no scoreboard.

## Metrics

A metric has:

- stable ID
- key
- metric quantity
- display unit
- aggregation
- objective
- description
- kind: observed or synthetic

Keys name concepts, not units. Prefer `presolve_wallclock` with display unit
`milliseconds`, not `presolve_ms`.

The CLI and MCP retain `dimension` as the compatible boundary spelling for a
metric quantity. Internal policy and new prose use **quantity** only.

## Quantities And Units

Quantities are algebraic:

- `time`
- `count`
- `bytes`
- `dimensionless`
- products, quotients, and exact rational powers

Units are input/display sugar. Values normalize on ingress.

Addition and subtraction require identical quantities. Multiplication and
division compose quantities. `gmean` requires at least one term and takes an
exact rational root. Zero exponents are discarded during construction and
deserialization, so one algebraic quantity has one representation.

Reported observations must be finite. `NaN` and positive or negative infinity
are invalid at every ingress. An experiment may have no primary metric only
when a scuffed closure records that the procedure yielded no meaningful point.
Supporting observations never become primary by position. Duplicate metric
keys in one outcome are rejected.

## KPIs

A KPI is only:

- frontier
- metric
- canonical order

Active MCP-visible frontiers must have at least one KPI before hypothesis or
experiment work.

KPI order drives:

- metric-designer order
- plot color order
- default result-table tab

## Synthetic Metrics

Synthetic metrics are supervisor-defined formulas over metrics.

Allowed nodes:

- metric reference
- finite constant
- `+`
- `-`
- `*`
- `/`
- `gmean`

Synthetics may depend on synthetics. The graph must be acyclic. MCP may query
synthetics but may not define or report them.

A synthetic metric can become a KPI only if every transitive observed leaf is
already a KPI on that frontier.

## Reference Lines

A KPI reference is a named `(frontier, KPI)` value. Results plots render it as a
horizontal comparison line, with its name and value in the reference legend
below the plot.

Use it for baselines, rivals, targets, or theoretical bounds.

Do not use references as experiment observations, running bests, or a scratchpad
for new benchmark/playtest measurements. The operator’s primary progress view is
the closed-experiment record. Any material hypothesis-driven result must enter
that record through `experiment.close`, with its conditions, metric values,
verdict, rationale, and owning hypothesis.

## MCP

Model-facing tools:

- `metric.define`
- `metric.keys`
- `metric.best`
- `kpi.create`
- `kpi.list`
- `kpi.best`
- `kpi.reference.set`
- `kpi.reference.list`
- `kpi.reference.delete`
- `frontier.query.schema`
- `frontier.query.sql`

No MCP synthetic-definition tool. No MCP KPI demotion. No bulk KPI mutation.

Scuffed experiments remain auditable but do not enter plots, rankings, or
running-best calculations. A rejected experiment is valid negative evidence
and remains visible in analytical surfaces unless the caller narrows it.
