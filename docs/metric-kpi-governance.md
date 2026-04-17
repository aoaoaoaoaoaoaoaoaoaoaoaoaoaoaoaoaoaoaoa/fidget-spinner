# Metric KPI Governance

## Intent

Metrics need the same supervisor sharpening as tags, but the governing object is
not a family. A metric is a measurement instrument. A KPI is the frontier-local
claim that a small ordered set of instruments measures the same optimization
idea well enough to stand in for one another.

The current scoreboard concept should be deleted. It is neither a contract nor a
pure UI preference, so it invites arbitrary metric bundles that models treat as
important without knowing why. If the navigator ever needs pinned metrics again,
pinning should be a frontend-only preference. The backend should know KPIs,
observed metrics, and metric definitions; nothing in between.

The product stance:

- Every active model-enumerable frontier has at least one KPI.
- KPIs are mandatory evidence contracts, not loose dashboard groups.
- KPI alternatives encode supersession: older and newer metrics can represent
  the same measurement idea with a strict precedence order.
- The metrics page opens on KPI plots by default.
- Metric registry cleanup is supervisor-owned; MCP writes obey policy locks.
- Units are not backend identities. Values are normalized by measurement
  dimension at write time.

## Vocabulary

### Metric Definition

A metric definition is a project-global instrument.

Examples:

- `presolve_wallclock_gmean`
- `presolve_wallclock_single`
- `residual_nz`
- `pmu_cycles`
- `soundness_specimens_passed`

Fields:

- stable metric id
- human key
- measurement dimension
- objective
- visibility/status
- aggregation kind
- display unit preference
- description
- created/updated timestamps

Metric keys are human handles. They are not durable foreign keys. As with tags,
rename/merge/delete must preserve stale-name guidance so a running model gets a
porcelain correction instead of an opaque unknown-metric failure.

Metric keys name the measured concept, not the rendering unit. Avoid Hungarian
unit notation such as `_ms`, `_s`, `_bytes`, `_pct`, or `_count` in newly defined
metric keys. Use `presolve_wallclock` with display unit `milliseconds`, not
`presolve_ms`; use `report_size` with display unit `bytes`, not
`report_bytes`. Units, dimensions, and aggregation shape are registry fields.

### Measurement Dimension

A measurement dimension defines the physical or semantic quantity being stored.
It is stricter than the current unit string.

Initial dimensions:

- `time`: canonical duration-like magnitude.
- `count`: unitless cardinal magnitude.
- `bytes`: storage magnitude.
- `ratio`: normalized fractional magnitude.
- `dimensionless`: arbitrary scalar with no conversion law.

The term "dimension" collides with run dimensions. In code, prefer a more exact
name such as `MetricDimension`, `MeasurementDimension`, or `QuantityDimension`.

### Display Unit

A display unit is presentation and input sugar within a dimension.

For time, accept `ns`, `us`, `ms`, and `s`, but normalize immediately. The
backend should not treat seconds and milliseconds as different metric units.
They are different renderings of the same dimension.

For ratio, accept fraction and percent spellings, but store the canonical value
as a ratio. Percent is a display unit, not a distinct backend unit.

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

This prevents a unit-compatible metric from masquerading as equivalent evidence.
`presolve_wallclock_single` and `presolve_wallclock_gmean` may both be time
metrics, but their aggregation semantics differ. A KPI may declare them
superseding alternatives, but that equivalence is an explicit frontier-local
judgment, not a consequence of shared units.

### KPI

A KPI is a frontier-local mandatory measurement contract.

Fields:

- stable KPI id
- frontier id
- name
- objective
- mandatory flag, initially always true
- default plot flag, initially always true
- ordered metric alternatives
- description
- revision

Example:

```text
KPI root_presolve_wallclock
objective: minimize
alternatives, highest precedence first:
  1. presolve_wallclock_gmean
  2. presolve_wallclock_single
```

This says both metrics are accepted evidence for the KPI, but if an experiment
reports both, the KPI value resolves through `presolve_wallclock_gmean`.

## Frontier Invariant

An active frontier must have at least one KPI before it appears in normal MCP
enumeration. The cleanest implementation is to require KPI creation as part of
frontier creation or before unarchiving/activating a frontier.

The supervisor UI may need a temporary draft affordance later. If introduced,
draft frontiers are invisible to model enumeration and exempt from the KPI
invariant. Do not weaken the active-frontier invariant to accommodate drafts.

## Experiment Close Policy

When an MCP-origin close supplies metric observations, the store resolves them
against active KPI contracts for the experiment frontier.

Algorithm:

1. Load active KPIs for the frontier.
2. For each KPI, find reported metric observations whose metric id appears in
   the KPI alternative list.
3. If none are present, reject the close with a porcelain policy error naming
   the missing KPI and accepted metric keys.
4. If several are present, choose the highest-precedence alternative as the KPI
   value.
5. Persist all reported observations, but persist the resolved KPI value or make
   it cheaply derivable from normalized observations plus KPI precedence.

Supervisor-origin outcome edits remain authoritative and may repair historical
records during cleanup. Policy locks constrain MCP writes only.

Good MCP error:

```text
mandatory KPI `root_presolve_wallclock` is missing; report one of: presolve_wallclock_gmean, presolve_wallclock_single
```

## Units And Canonical Values

The current backend permits `seconds`, `milliseconds`, `microseconds`, and
`nanoseconds` as separate metric units. That is the wrong abstraction.

Target model:

- Metric definitions declare `MetricDimension`.
- Observations may carry an input display unit when the dimension is
  convertible.
- The store normalizes to canonical representation before persistence.
- Projections carry both canonical value and display metadata.

Recommended canonical forms:

- `time`: nonnegative integer nanoseconds for raw duration-like values.
- `count`: integer when exact, decimal only when aggregation requires it.
- `bytes`: integer bytes.
- `ratio`: decimal fraction, normally bounded to `[0, 1]` unless explicitly
  marked unbounded.
- `dimensionless`: decimal scalar.

The one wrinkle is aggregated time. A geometric mean over durations can be
fractional at sub-nanosecond precision, but this is not a practical concern for
the current workloads. If exactness later matters, add a decimal canonical
numeric type rather than reintroducing unit strings as identity.

## Metric Registry Governance

Metrics should receive the same supervisor console pattern as tags.

Top state band:

- active metric count
- hidden/archived count
- KPI count
- orphaned metric count
- `new metrics` MCP lock
- `registry edits` MCP lock

Registry operations:

- create metric definition
- rename metric key
- merge metric into target
- delete/archive metric
- hide/unhide metric
- edit description/objective/dimension/display preference

Dimension edits are special. Once a metric has observations, its measurement
dimension is frozen. Changing dimension in place would reinterpret historical
canonical values. If a metric was defined with the wrong dimension, the
supervisor should create the correct metric and merge or tombstone the bad one
through an explicit repair path. Recanonicalization is a migration operation, not
a normal edit.

Objective edits should be similarly guarded. A KPI alternative must have the
same objective as the KPI. If objective needs to change after observations
exist, prefer a new metric plus explicit supersession rather than silently
flipping old comparisons.

Locks:

- `metrics/definition`: blocks MCP metric creation.
- `metrics/family` or a renamed `metrics/edit`: blocks MCP-origin metric
  registry edits if such tools are ever exposed.

Supervisor UI operations are never blocked by these locks.

Stale-name dispositions mirror tags:

- `renamed`
- `merged`
- `deleted`

MCP use of stale metric names should receive porcelain guidance.

## KPI UI

Frontier pages should surface KPIs as first-class contract objects.

Metrics tab default:

- Plot all active KPI resolved series by default.
- If one KPI exists, open directly on that KPI plot and table.
- If several KPIs exist, show compact KPI tabs.
- Non-KPI metrics remain discoverable but secondary.

KPI editor:

- Add KPI.
- Rename KPI.
- Add metric alternative.
- Reorder alternatives by precedence.
- Remove alternative.
- Show which experiments satisfy each KPI.
- Show which experiments use older superseded alternatives.

If a newer metric supersedes an older metric, the editor should make the
precedence visible without asking the user to invent a "family" name. The KPI is
the family-esque object.

KPI names should have stale-name history if they become model-addressable. A
renamed KPI should produce the same style of guided porcelain response as a
renamed metric or tag.

## MCP Surface

Delete scoreboard language from model-facing surfaces.

Replace:

```text
scoreboard_metric_keys
scoreboard_metrics
```

with:

```text
kpis
kpi_metrics
```

Likely tools:

- `metric.define`: still project-global.
- `metric.keys`: lists project-global metric definitions; can filter by KPI
  relevance for a frontier.
- `kpi.list`: lists frontier KPI contracts.
- `kpi.best`: ranks experiments by resolved KPI value.
- `metric.best`: remains for ad hoc metric inspection, but frontier loops should
  prefer KPIs.

`kpi.best` and KPI tables must surface the metric alternative used to resolve
each row. Mixed old/new alternatives are valid but not invisible. A long frontier
often needs to compare across a supersession boundary, but the UI and MCP output
should make it obvious when one experiment is ranked by `presolve_wallclock_gmean`
and another by `presolve_wallclock_single`. Add a strict mode that filters to
the highest-precedence available metric when apples-to-apples comparison matters.

`frontier.open` should expose KPIs prominently because they define the hill the
model is climbing. `metric.keys --scope live` remains useful, but it is not the
contract.

## Data Model Sketch

Schema epoch, not compatibility theater.

```sql
metric_definitions (
    id TEXT PRIMARY KEY NOT NULL,
    key TEXT NOT NULL UNIQUE,
    dimension TEXT NOT NULL,
    objective TEXT NOT NULL,
    aggregation TEXT NOT NULL,
    display_unit TEXT,
    visibility TEXT NOT NULL,
    description TEXT,
    revision INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

metric_name_history (
    name TEXT PRIMARY KEY NOT NULL,
    target_metric_id TEXT REFERENCES metric_definitions(id) ON DELETE SET NULL,
    target_metric_key TEXT,
    disposition TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
);

kpi_name_history (
    frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    target_kpi_id TEXT REFERENCES frontier_kpis(id) ON DELETE SET NULL,
    target_kpi_name TEXT,
    disposition TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (frontier_id, name)
);

frontier_kpis (
    id TEXT PRIMARY KEY NOT NULL,
    frontier_id TEXT NOT NULL REFERENCES frontiers(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    objective TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (frontier_id, name)
);

kpi_metric_alternatives (
    kpi_id TEXT NOT NULL REFERENCES frontier_kpis(id) ON DELETE CASCADE,
    metric_id TEXT NOT NULL REFERENCES metric_definitions(id) ON DELETE CASCADE,
    precedence INTEGER NOT NULL,
    PRIMARY KEY (kpi_id, metric_id),
    UNIQUE (kpi_id, precedence)
);

experiment_metric_observations (
    experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    metric_id TEXT NOT NULL REFERENCES metric_definitions(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    role TEXT NOT NULL,
    canonical_value TEXT NOT NULL,
    reported_value REAL,
    reported_unit TEXT,
    PRIMARY KEY (experiment_id, metric_id)
);
```

`canonical_value` is text to leave room for integer nanoseconds and later decimal
forms without SQLite numeric coercion surprises. The Rust domain type should
carry the exact variant.

For v1, there is at most one observation per metric per experiment. `ordinal`
preserves report order for display and provenance; it is not a repeated-sample
axis. If a benchmark emits samples, the samples belong in an artifact or in an
explicit aggregate metric such as `presolve_wallclock_gmean`.

## Migration Program

1. Introduce stable metric ids and metric name history.
2. Replace unit strings with metric dimensions plus display units.
3. Convert time definitions to dimension `time` and canonical display units.
4. Convert experiment metric rows from key references to metric id references.
5. Introduce frontier KPIs and KPI alternatives.
6. Convert existing `scoreboard_metric_keys` to initial KPIs only if the
   supervisor explicitly accepts the mapping; otherwise delete scoreboard data.
7. Remove scoreboard from frontier brief, MCP projections, UI labels, and skill
   text.
8. Enforce at least one KPI for active model-enumerable frontiers.
9. Make Metrics tab default to KPI plots.

Step 6 should be intentionally supervised. Scoreboards are too semantically
loose to auto-promote blindly.

Step 8 must be a hard gate. A frontier with no accepted KPI mapping cannot remain
normal model-enumerable `exploring` state. The migration should require the
supervisor to assign at least one KPI or explicitly demote the frontier out of
normal enumeration, for example by pausing or archiving it. Do not silently hide
or half-migrate frontiers.

## Non-Goals

- Backend metric pinning.
- Generic metric families.
- A taxonomy surface before KPI contracts exist.
- Backward-compatible dual scoreboard/KPI paths.
- Treating unit compatibility as KPI equivalence.

## Open Questions

- Should `metric.define` remain MCP-callable once `metrics/definition` is
  locked, or should models always ask the supervisor for new instruments during
  mature runs?
- Should active frontier creation require KPI definitions inline, or should
  there be a separate draft frontier state?
- Do we need target-threshold KPIs soon, or is objective-only enough for the
  current frontier loops?
- Should KPI alternatives require matching metric dimensions? The answer should
  be yes, except for rare legacy mistakes repaired by supervisor migration.
