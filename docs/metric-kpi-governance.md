# Metric And KPI Governance

Status: current policy reference.

A metric is a project-global instrument. A KPI is a frontier-local edge saying
that one metric defines the hill.

There is no scoreboard.

## Metrics

A metric has:

- stable ID
- key
- metric dimension
- display unit
- aggregation
- objective
- description
- kind: observed or synthetic

Keys name concepts, not units. Prefer `presolve_wallclock` with display unit
`milliseconds`, not `presolve_ms`.

## Dimensions And Units

Dimensions are algebraic quantities:

- `time`
- `count`
- `bytes`
- `dimensionless`
- products, quotients, and exact rational powers

Units are input/display sugar. Values normalize on ingress.

Addition and subtraction require identical dimensions. Multiplication and
division compose dimensions. `gmean` takes an exact rational root.

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
horizontal comparison line.

Use it for baselines, rivals, targets, or theoretical bounds.

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
