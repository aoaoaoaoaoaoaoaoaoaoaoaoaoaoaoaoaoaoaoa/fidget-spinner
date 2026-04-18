# JSON Normalization Audit

Status: implemented in store format 12. The inventory below records the v11
problem statement and the cutover shape; operational reads and writes now use
the normalized tables, with only `events.snapshot_json` retained as an archival
snapshot exception.

## Intent

Spinner should not keep operational state in ad hoc JSON blobs once the shape is
stable enough to deserve first-class tables.

The current store is already mostly normalized, but a few JSON surfaces still
act as either:

- the primary source of truth for hot-path state, or
- a second hidden source of truth that shadows normalized tables.

This note audits those surfaces and proposes a one-shot cutover toward a store
where:

- live operational state is relational,
- entity reconstruction happens in Rust from normalized tables,
- JSON remains only on wire/protocol boundaries and, optionally, for immutable
  archival snapshots.

Given the current single-user, lockstep-deploy posture, the recommended rollout
is a decisive store-format bump rather than migration-chaining theater.

## Live Inventory

### In-DB JSON/Blob-Like Surfaces

| Surface | Current role | Assessment |
| --- | --- | --- |
| `frontiers.brief_json` | Stores `FrontierBrief { situation, roadmap, unknowns, revision, updated_at }` | Normalize |
| `experiments.outcome_json` | Stores the closed experiment outcome envelope, including command, conditions, metrics, verdict, rationale, analysis, commit hash, closed_at | Normalize |
| `experiment_dimensions.value_json` | Stores `RunDimensionValue` as tagged JSON, despite already having a relation for conditions | Normalize aggressively |
| `events.snapshot_json` | Immutable per-revision history snapshot for heterogeneous entity kinds | Keep for now as an explicit archival exception |

### Out-of-DB JSON Files

| Surface | Current role | Assessment |
| --- | --- | --- |
| `project.json` | Project metadata sidecar: display name, description, created_at, store format | Move into SQLite |

### Out of Scope

The following JSON surfaces are not store-design problems:

- JSON-RPC / MCP transport
- CLI `--outcome-json` input convenience
- projection/output rendering through `serde_json::Value`
- host re-exec seed snapshots

Those are protocol or process-boundary serialization, not the persistence model.

## Current Findings

### 1. `outcome_json` is still the real experiment truth

The schema already has:

- `experiment_metrics`
- `experiment_dimensions`

But the read path still reconstructs `ExperimentOutcome` by decoding
`experiments.outcome_json`, not from those tables.

Consequences:

- repairs feel uncanny because changing one metric really means changing two
  representations,
- normalization buys less than it should because the blob still drives reads,
- migration helpers must patch both relational rows and JSON payloads,
- dual-write drift is possible in principle even if current code is careful.

This is the highest-value normalization target.

### 2. `experiment_dimensions.value_json` is the worst of both worlds

Conditions already have a dedicated table, but each row stores a JSON-encoded
sum type.

That means:

- the schema still cannot express condition values in typed columns,
- SQL cannot inspect condition values without JSON decoding,
- the table exists but does not deliver true relational leverage,
- the read path often bypasses it anyway and pulls conditions from
  `outcome_json`.

This should become a truly typed relational surface.

### 3. `frontiers.brief_json` contradicts the intended architecture

The architecture doc already speaks as though `frontier_briefs` were a first
class table family, but the store still writes the whole brief as one JSON blob.

The brief is structurally simple and stable:

- one optional situation string,
- an ordered roadmap list,
- an ordered unknowns list,
- brief-local revision metadata.

This is cheap to normalize and would make the storage model match the product
story.

### 4. `project.json` splits store identity across two substrates

The SQLite store and `project.json` currently co-own project state.

That creates needless asymmetry:

- state root discovery lands in SQLite anyway,
- store-format migration touches both DB metadata and JSON file metadata,
- project description edits are not transactional with DB work,
- it makes “the store” slightly less singular than it should be.

This should become a singleton `project_metadata` row inside SQLite.

### 5. `events.snapshot_json` is qualitatively different

The history surface is not hot operational state. It is a revision ledger over
heterogeneous entity kinds:

- frontier
- hypothesis
- experiment
- metric
- tag
- tag family
- registry lock
- KPI

Fully normalizing snapshots would mean either:

- per-kind history tables mirroring the live schema, plus child revision tables,
  or
- a fully event-sourced store with replayable typed deltas rather than snapshots.

Both are much larger projects than removing operational blobs.

Recommendation:

- keep `events.snapshot_json` for now,
- but explicitly demote it to an archival exception rather than letting it
  masquerade as ordinary operational storage.

## Recommended End State

### Rule

No live entity should need JSON decoding to answer a first-class read.

Rust should assemble domain objects from normalized rows. JSON should not be
the source of truth for:

- frontier brief state,
- experiment closure state,
- condition values,
- project metadata.

### Frontier Brief Shape

Replace `frontiers.brief_json` with:

- `frontier_briefs`
  - `frontier_id PRIMARY KEY`
  - `situation TEXT NULL`
  - `revision INTEGER NOT NULL`
  - `updated_at TEXT NULL`
- `frontier_roadmap_items`
  - `frontier_id`
  - `ordinal`
  - `hypothesis_id`
  - `summary TEXT NULL`
- `frontier_unknowns`
  - `frontier_id`
  - `ordinal`
  - `body TEXT NOT NULL`

Notes:

- keep `frontiers` focused on identity and top-level scope,
- keep roadmap and unknown ordering explicit via `ordinal`,
- reconstruct `FrontierBrief` from these tables in Rust.

### Experiment Outcome Shape

Replace `experiments.outcome_json` with:

- `experiment_outcomes`
  - `experiment_id PRIMARY KEY`
  - `backend TEXT NOT NULL`
  - `verdict TEXT NOT NULL`
  - `rationale TEXT NOT NULL`
  - `analysis_summary TEXT NULL`
  - `analysis_body TEXT NULL`
  - `working_directory TEXT NULL`
  - `commit_hash TEXT NULL`
  - `closed_at TEXT NOT NULL`

- `experiment_command_argv`
  - `experiment_id`
  - `ordinal`
  - `arg TEXT NOT NULL`

- `experiment_command_env`
  - `experiment_id`
  - `key TEXT NOT NULL`
  - `value TEXT NOT NULL`

Keep:

- `experiment_metrics`
- `experiment_tags`
- `influence_edges`

Status remains on `experiments`, with closure presence (`experiment_outcomes`
row exists) enforcing the “closed experiments have outcomes” invariant.

### Run Dimension Shape

Replace `experiment_dimensions.value_json` with truly typed tables.

Preferred austere design:

- `experiment_dimension_strings`
  - `experiment_id`
  - `key`
  - `value TEXT NOT NULL`
- `experiment_dimension_numbers`
  - `experiment_id`
  - `key`
  - `value REAL NOT NULL`
- `experiment_dimension_booleans`
  - `experiment_id`
  - `key`
  - `value INTEGER NOT NULL CHECK (value IN (0, 1))`
- `experiment_dimension_timestamps`
  - `experiment_id`
  - `key`
  - `value TEXT NOT NULL`

Why this over one table with many nullable columns:

- no value-column option wall,
- the type split is explicit in the schema,
- illegal states are harder to represent,
- queries remain straightforward because condition filtering already knows the
  target type from `run_dimension_definitions`.

### Project Metadata Shape

Replace `project.json` with:

- `project_metadata`
  - singleton row or keyed row
  - `display_name TEXT NOT NULL`
  - `description TEXT NULL`
  - `created_at TEXT NOT NULL`

Use SQLite `PRAGMA user_version` for binary/store compatibility, not a second
copy inside a sidecar file.

If a second format marker is still desired for explicit introspection, keep it
in `project_metadata`, but the strong recommendation is that `user_version`
should be the single store-format authority.

## What Should Stay Denormalized

### Keep `events.snapshot_json`

This is the one JSON surface that still makes sense.

Reasoning:

- it is immutable archival history rather than live operational state,
- it spans many heterogeneous entity kinds,
- it is read infrequently,
- it wants exact revision snapshots, not just current relational truth,
- normalizing it would explode schema size and migration complexity for modest
  product value.

Policy recommendation:

- live state: no JSON blobs,
- archival event snapshots: allowed,
- all other persisted JSON surfaces: suspect by default.

If history eventually becomes hot enough to justify normalization, that should
be a separate project with explicit typed revision tables, not incidental creep
inside the operational migration.

## Migration Strategy

### Phase 1: Make normalized reads possible

Before dropping any blob columns:

- add Rust loaders that reconstruct `FrontierBrief` from normalized brief tables,
- add Rust loaders that reconstruct `ExperimentOutcome` from normalized outcome,
  metric, condition, and command tables,
- add Rust loaders that reconstruct typed condition maps from typed tables.

Do not keep the new tables as shadows forever.

### Phase 2: Frontier brief cutover

1. Add `frontier_briefs`, `frontier_roadmap_items`, `frontier_unknowns`.
2. Backfill from `brief_json`.
3. Flip reads to relational reconstruction.
4. Drop `frontiers.brief_json`.

This is the cleanest first move and validates the pattern on a small surface.

### Phase 3: Experiment outcome cutover

1. Add `experiment_outcomes`, `experiment_command_argv`, `experiment_command_env`.
2. Backfill from `outcome_json`.
3. Flip reads away from `outcome_json`.
4. Stop mutating `outcome_json`.
5. Drop `outcome_json`.

At this point `experiment_metrics` becomes first-class rather than shadow state.

### Phase 4: Typed dimension cutover

1. Add the four typed dimension tables.
2. Backfill from `experiment_dimensions.value_json`.
3. Flip reads and filtering to the typed tables.
4. Drop the old `experiment_dimensions` table.

This should probably happen in the same store-format bump as outcome
normalization, because the two are deeply entangled.

### Phase 5: Project metadata cutover

1. Add `project_metadata`.
2. Backfill from `project.json`.
3. Flip open/init/update to DB metadata.
4. Remove `project.json`.

This can happen in the same decisive migration or immediately after it.

## Recommended Scope Cut

The best near-term cut is:

1. normalize frontier briefs,
2. normalize experiment outcomes,
3. normalize typed dimension values,
4. move project metadata into SQLite,
5. explicitly retain `events.snapshot_json`.

That gets Spinner to an operationally normalized store without turning the
history subsystem into a second dissertation.

## Practical Notes For Implementation

### Do not preserve dual truth

The dangerous failure mode is:

- adding new relational tables,
- continuing to read from old blobs,
- and calling that “migration complete.”

Spinner is already partially in that state for experiment outcomes. The next
push should remove, not deepen, that split-brain pattern.

### Prefer one store-format bump

The product posture already tolerates decisive cutovers. Use that.

One reasonable implementation is:

- `v12`: frontier brief + experiment outcome + typed conditions + project metadata
- drop `brief_json`, `outcome_json`, `value_json`, and `project.json`
- keep `events.snapshot_json`

If the event-history question later matters, treat it as its own versioned
project rather than sneaking it into this cut.

### Add invariant tests

Before and after the cut, add tests that assert:

- reconstructed frontier brief matches legacy decode on fixture stores,
- reconstructed experiment outcome matches legacy decode on fixture stores,
- metric rename/merge updates no longer need to patch any outcome blob,
- manual store edits cannot leave conditions present in two inconsistent forms.

## Bottom Line

Spinner should have exactly one sanctioned persisted blob class:

- immutable event snapshots.

Everything else currently stored as JSON is mature enough to deserve proper
relational structure.
