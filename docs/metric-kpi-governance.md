# Metric KPI Governance

## Intent

Metrics need the same supervisor sharpening as tags, but the durable object is
not a family, contract, or dashboard group. A metric is a project-global
measurement instrument. A KPI is the frontier-local decision that one existing
metric is mandatory and important enough to define the hill the model is
climbing.

The former scoreboard concept is deleted. It was neither a policy contract nor
a pure UI preference, so it invited arbitrary metric bundles that models treated
as important without knowing why. If the navigator later wants pinned metrics,
pinning is frontend state. The backend knows metric definitions, observed metric
values, and frontier KPI metric edges.

The product stance:

- Every active model-enumerable frontier has at least one KPI metric.
- A KPI is a privileged metric, not an indirection layer with its own name.
- Supersession is not modeled as KPI alternatives. If two measurements remain
  useful, keep both as KPI metrics and let the plot show two lines.
- If an old metric should stop being reported, use an explicit supervisor lock
  or stale-name guidance rather than hiding it inside a KPI bucket.
- The results page opens on KPI plots by default.
- Metric registry cleanup is supervisor-owned; MCP writes obey policy locks.
- Units are not backend identities. Values are normalized by metric quantity at
  write time.
- Synthetic metrics are supervisor-defined formulas over other metrics. Models
  can query them, but cannot define them or report them directly.

## Vocabulary

### Metric Definition

A metric definition is a project-global instrument.

Examples:

- `presolve_wallclock`
- `presolve_wallclock_gmean`
- `residual_nz`
- `pmu_cycles`
- `soundness_specimens_passed`

Fields:

- stable metric id
- human key
- metric quantity
- display unit
- aggregation kind
- objective
- description
- kind: observed or synthetic
- revision
- created/updated timestamps

Metric keys are human handles. They are not durable foreign keys. As with tags,
rename, merge, and delete preserve stale-name guidance so a running model gets a
porcelain correction instead of an opaque unknown-metric failure.

Metric keys name the measured concept, not the rendering unit. Avoid Hungarian
unit notation such as `_ms`, `_s`, `_bytes`, `_pct`, or `_count` in newly
defined metric keys. Use `presolve_wallclock` with display unit `milliseconds`,
not `presolve_ms`; use `report_size` with display unit `bytes`, not
`report_bytes`. Units, dimensions, and aggregation shape are registry fields.

### Metric Quantity

A metric quantity defines the physical or semantic quantity being stored.
It is stricter than a display unit and supports algebraic composition.

Base quantities:

- `time`: canonical duration-like magnitude.
- `count`: cardinal magnitude.
- `byte`: storage magnitude.
- `dimensionless`: empty quantity.

Composite quantities are products of base quantities with exact rational
exponents, such as `count/time` or `time^1/2*count^1/2`. Addition and
subtraction require identical quantities. Multiplication and division add and
subtract exponents. Geometric mean is an exact rational root of the product
quantity.

Public MCP/UI wording still says "metric dimension" where a scalar observed
metric chooses one of the simple base quantities, because that is the
least-surprise scientific word. Experimental setup fields are conditions and
must never be called dimensions.

### Display Unit

A display unit is presentation and input sugar within a quantity.

For time, accept `ns`, `us`, `ms`, and `s`, but normalize immediately. The
backend must not treat seconds and milliseconds as distinct metric species. They
are renderings of the same quantity.

For dimensionless values, accept fraction-like and percent spellings, but store
the canonical value as a dimensionless scalar. Percent is a display unit, not a
backend dimension.

### Aggregation Kind

Aggregation kind records the statistical shape of the metric.

Initial values:

- `point`
- `mean`
- `geomean`
- `median`
- `p95`
- `min`
- `max`
- `sum`

Aggregation is part of the metric identity. `presolve_wallclock` and
`presolve_wallclock_gmean` can share a dimension and unit while still remaining
separate instruments. If both are important for a frontier, promote both as KPI
metrics. Overlap on the plot is acceptable and usually honest.

### KPI Metric

A KPI metric is a frontier-local edge from a frontier to an existing metric.

Fields:

- stable KPI edge id
- frontier id
- metric id
- revision
- created/updated timestamps

There is no separate KPI name, objective, description, mandatory flag, default
plot flag, or ordered alternative list. The metric already supplies name,
objective, unit, aggregation, and description. KPI status supplies only
frontier-local privilege.

## Frontier Invariant

An active frontier must have at least one KPI metric before it appears in normal
MCP enumeration or accepts MCP frontier work. In particular, MCP-origin
`hypothesis.record`, `experiment.open`, and `experiment.close` are rejected until
a KPI exists. The clean implementation is to require KPI promotion as part of
frontier activation.

The supervisor UI may need a temporary draft affordance later. If introduced,
draft frontiers are invisible to model enumeration and exempt from the KPI
invariant. Do not weaken the active-frontier invariant to accommodate drafts.

## Synthetic Metrics

Synthetic metrics are formulas, not observations.

Allowed expression nodes:

- metric reference
- finite canonical constant
- `+`
- `-`
- `*`
- `/`
- `gmean` over one or more terms

Synthetics may depend on other synthetics. The dependency graph must be acyclic,
and quantity inference is recursive. The evaluator returns no point for an
experiment if any required leaf observation is missing, a division denominator
is zero, or a geometric-mean term is non-positive.

Supervisor UI may define synthetic metrics. MCP may not. MCP can still see
synthetic metrics in `metric.keys`, rank them with `metric.best` and `kpi.best`,
plot them in Results, and query synthetic points through frontier SQL views.
MCP-origin experiment closes must report observed leaf metrics, not synthetic
keys.

Synthetic KPI promotion is deliberately stricter than ordinary KPI promotion:
a synthetic metric can become a frontier KPI only when every transitive observed
leaf is already a KPI on that frontier. This preserves the existing close-time
contract without adding a second mandatory-leaf policy surface.

## Experiment Close Policy

When an MCP-origin close supplies metric observations, the store checks them
against the active KPI metrics for the experiment frontier.

Algorithm:

1. Load active KPI metrics for the frontier.
2. For each observed KPI metric, find the reported observation with that metric
   key.
3. For each synthetic KPI metric, require every transitive observed leaf metric
   instead.
4. If an observation is missing, reject the close with a porcelain policy error
   naming the KPI and the reportable metric keys.
5. Persist all reported observations when every mandatory KPI is present.

Supervisor-origin outcome edits remain authoritative and may repair historical
records during cleanup. Policy locks constrain MCP writes only.

Good MCP error:

```text
mandatory KPI metric `presolve_wallclock_gmean` is missing; report `presolve_wallclock_gmean`
```

## Supersession

Supersession is deliberately flattened out of the KPI model.

If a frontier moves from `presolve_wallclock` to `presolve_wallclock_gmean`,
there are three clean choices:

- Promote both as KPI metrics while old and new observations coexist.
- Demote the old metric once history no longer needs it as a mandatory signal.
- Later add a metric-level MCP observation lock that rejects new writes to the
  old metric with guidance such as `metric is locked; report
  presolve_wallclock_gmean`.

Do not introduce a KPI family solely to avoid two lines on a plot. The plot can
carry the overlap, and the table can show the metric actually reported.

## Units And Canonical Values

The backend should normalize convertible units inside a metric quantity.
Unit strings are not identity.

Target model:

- Metric definitions declare a metric quantity.
- Observations may carry an input display unit when the quantity is
  convertible.
- The store normalizes to canonical representation before persistence.
- Projections carry both canonical value and display metadata.

Recommended canonical forms:

- `time`: nonnegative integer nanoseconds for raw duration-like values.
- `count`: integer when exact, decimal only when aggregation requires it.
- `byte`: integer bytes.
- `dimensionless`: decimal scalar.
- composite quantities: canonical product/division of canonical leaf units.

Aggregated time can be fractional at sub-nanosecond precision. If exactness
later matters, add a decimal canonical numeric type rather than reintroducing
unit strings as identity.

## Metric Registry Governance

Metrics should receive the same supervisor console pattern as tags.

Top state band:

- active metric count
- hidden-by-frontier count
- KPI metric count
- orphaned metric count
- `new metrics` MCP lock
- `registry edits` MCP lock

Registry operations:

- create metric definition
- rename metric key
- merge metric into target
- delete metric
- edit description/objective/dimension/display preference
- promote one metric to KPI for one frontier
- demote one metric from KPI for one frontier

Dimension edits are special. Once a metric has observations, its measurement
dimension is frozen. Changing dimension in place would reinterpret historical
canonical values. If a metric was defined with the wrong dimension, the
supervisor should create the correct metric and merge or tombstone the bad one
through an explicit repair path. Recanonicalization is a migration operation,
not a normal edit.

Objective edits should be similarly guarded. If objective needs to change after
observations exist, prefer a new metric and explicit cleanup over silently
flipping old comparisons.

Locks:

- `metrics/definition`: blocks MCP metric creation.
- `metrics/edit`: blocks MCP-origin metric registry edits if such tools are
  ever exposed.
- `kpis/assignment`, frontier-scoped: blocks MCP KPI creation/promotion for
  that frontier. Supervisor UI and CLI KPI edits remain authoritative.
- future `metrics/observation`, metric-scoped: blocks MCP reports for a
  superseded metric while returning porcelain guidance toward the replacement.

Stale-name dispositions mirror tags:

- `renamed`
- `merged`
- `deleted`

MCP use of stale metric names should receive porcelain guidance.

## KPI UI

Frontier results should surface KPI metrics as first-class navigation.

Results tab default:

- Plot all active KPI metric series by default.
- If one KPI exists, open directly on that KPI plot and table.
- If several KPIs exist, show compact metric tabs.
- Non-KPI metrics remain discoverable but secondary.
- A `KPIs` link jumps to the global metric supervisor with the current frontier
  selected.

Global metric supervisor:

- Select one active frontier.
- Show that frontier's KPI metrics as editable privileged edges.
- Promote exactly one metric per action.
- Demote exactly one metric per action.
- Keep all supervisor actions available even when MCP locks are on.

## MCP Surface

Model-facing surfaces should talk in KPI metrics, not contracts.

Tools:

- `metric.define`: defines a project-global metric.
- `metric.keys`: lists visible metric definitions; `--scope kpi` filters to a
  frontier's KPI metrics.
- `kpi.create`: promotes one existing metric into a frontier KPI metric.
- `kpi.list`: lists frontier KPI metrics.
- `kpi.best`: ranks experiments by one KPI metric.
- `metric.best`: remains for ad hoc metric inspection, but frontier loops
  should prefer KPIs.
- `frontier.query.sql`: exposes synthetic definitions and synthetic experiment
  metric points through stable frontier-scoped views.

There is intentionally no MCP synthetic-metric definition tool, no MCP demote
tool, and no bulk KPI interface. Promotion exists because a model may discover
that an existing instrument has become the real hill. Demotion and formula
definition are supervisor cleanup.

`frontier.open` exposes KPIs prominently because they define the hill the model
is climbing. `metric.keys --scope live` remains useful, but it is not the
contract.

## Data Model

Schema epoch, not compatibility theater.

```sql
frontier_kpis (
    id TEXT PRIMARY KEY NOT NULL,
    frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
    metric_id TEXT NOT NULL REFERENCES metric_definitions(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (frontier_id, metric_id)
);
```

Metric definitions remain project-global. Experiment metric observations remain
ordinary observations. KPI status is derived only by joining the frontier edge to
the metric definition.

Synthetic metric definitions add a formula table plus direct dependency edges.
The expression JSON is typed and stable enough for supervisor editing, while the
edge table keeps dependency inspection and delete safety relational.

## Migration Program

1. Drop backend scoreboard semantics.
2. Convert each accepted pre-flattening KPI row to a single frontier KPI metric
   using its highest-precedence metric alternative.
3. Drop KPI names, KPI descriptions, KPI objectives, and KPI alternatives.
4. Enforce at least one KPI metric for active model-enumerable frontiers.
5. Make Results default to KPI metric plots.
6. Keep unit normalization and metric-observation locks as follow-on work.

The migration is a one-way epoch. There is no compatibility shim and no dual
scoreboard/KPI path.

## Non-Goals

- Backend metric pinning.
- Generic metric families.
- KPI names.
- KPI alternatives.
- Backend supersession buckets.
- Backward-compatible dual scoreboard/KPI paths.
- Treating unit compatibility as KPI equivalence.
- MCP-defined synthetic formulas.
