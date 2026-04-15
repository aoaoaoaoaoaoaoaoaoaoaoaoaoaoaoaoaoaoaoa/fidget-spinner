# Supervisory Tag Governance

## Intent

Fidget Spinner should let driving models explore quickly without pretending they
will maintain a perfect taxonomy under pressure. The corrective force should be
a supervisor layer: the human cleans, merges, sharpens, and eventually locks
parts of the experimental surface while models keep running.

Tags are the first registry to receive this treatment. The same pattern should
later apply to metrics, run dimensions, verdict overrides, and frontier
freezing.

The product stance is strict:

- Models may create useful mess during exploration.
- The supervisor owns cleanup and policy.
- Cleanup must be safe while experiments are running.
- Policy changes must affect the next MCP write without service reload.
- MCP errors must be porcelain-clean enough for a model to self-correct.
- Supervisor cleanup operations must not be exposed as normal MCP tools.

## Actor Model

All mutating registry operations need an explicit origin.

```text
MutationOrigin::Mcp
MutationOrigin::Supervisor
```

Policy locks constrain `Mcp` writes. They do not constrain `Supervisor` writes.
This keeps the UI authoritative: a locked tag surface means "models cannot keep
inventing or changing tags", not "the human cannot clean the tag registry".

The store layer should receive the origin with every mutating operation that can
touch governed state. Enforcement belongs below the MCP service so every MCP
entrypoint is uniformly constrained, but the supervisor UI can still bypass the
policy deliberately by using `MutationOrigin::Supervisor`.

## Schema Epoch

This should be a schema epoch cutover, not a long compatibility chain. Spinner is
local-first and currently single-user. When the supervisor has vouched that no
models are live, bump the store format and convert the per-project SQLite stores
in place.

The cutover should remove name-keyed tag edges. Tag names are human-facing
handles; they are not durable identity.

### Tables

```sql
tags (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    family_id TEXT REFERENCES tag_families(id) ON DELETE SET NULL,
    status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

tag_families (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    mandatory INTEGER NOT NULL,
    status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

tag_name_history (
    name TEXT PRIMARY KEY NOT NULL,
    target_tag_id TEXT REFERENCES tags(id) ON DELETE SET NULL,
    disposition TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
);

hypothesis_tags (
    hypothesis_id TEXT NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
    tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (hypothesis_id, tag_id)
);

experiment_tags (
    experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (experiment_id, tag_id)
);

registry_locks (
    id TEXT PRIMARY KEY NOT NULL,
    registry TEXT NOT NULL,
    mode TEXT NOT NULL,
    scope_kind TEXT,
    scope_id TEXT,
    reason TEXT NOT NULL,
    revision INTEGER NOT NULL,
    locked_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (registry, mode, scope_kind, scope_id)
);
```

`tag_name_history` is the concurrency lubricant. If a model has stale context
after a rename, merge, or delete, it gets a guided policy error instead of an
opaque unknown-tag failure.

Required dispositions:

- `renamed`: old name now points to the same stable tag under a new name.
- `merged`: old tag was merged into a target tag.
- `deleted`: old tag was intentionally removed and has no target.

Required lock modes:

- `definition`: MCP cannot create registry entries.
- `assignment`: MCP cannot change assignments of existing entries.
- `family`: MCP cannot mutate the governed family surface.

Scope can be null for project-wide locks. Frontier-scoped locks are desirable
later but should not be in the first implementation unless the schema can carry
them without extra policy complexity.

## Tag Operations

### Rename

Rename changes `tags.name` while preserving `tags.id`.

Effects:

- Existing hypothesis and experiment edges remain untouched.
- A `tag_name_history` row records the old name as `renamed`.
- `tag.list` immediately shows only the new active name.
- MCP use of the old name fails with a policy message that names the replacement.

Porcelain:

```text
tag `local-search` was renamed to `search/local`; use `search/local`
```

### Merge

Merge folds a source tag into a target tag.

Effects:

- Copy source assignments to target with deduplication.
- Delete or retire the source tag.
- Record the source name in `tag_name_history` as `merged`.
- Preserve a clear event trail.

Porcelain:

```text
tag `ls` was merged into `search/local`; use `search/local`
```

### Delete

Delete removes the tag from the active registry.

Effects:

- Remove all hypothesis and experiment assignments by cascade or explicit edge
  deletion.
- Record the old name in `tag_name_history` as `deleted`.
- Require UI confirmation with usage counts before deletion.

Porcelain:

```text
tag `scratch` was deleted by the supervisor; choose an active tag from tag.list
```

### Family Assignment

Each tag belongs to at most one family. Family membership is explicit state, not
inferred from slashes in tag names. Slashes remain useful human convention, but
they must not be policy.

Family assignment is a supervisor operation. The first version should support:

- create family
- rename family
- assign or remove a tag's family
- mark family mandatory
- lock family edits

## Mandatory Families

A mandatory family means future MCP-created or MCP-updated tag sets must contain
at least one active tag from that family.

This rule is prospective.

- Existing hypotheses and experiments are not retroactively invalid.
- Reads never fail because of a newly mandatory family.
- Closing an existing open experiment should not fail if the close does not
  replace its tag set.
- Any MCP operation that creates a tag-bearing entity or explicitly replaces
  tags must satisfy every mandatory family.

Porcelain:

```text
mandatory tag family `phase` is missing; include at least one of: baseline, ablation, optimization
```

Mandatory checks should include enough tag names for the model to comply without
another lookup. If the family is huge, include a bounded sample plus tell the
model to call `tag.list`.

## Locks

Locks are general registry policy, not a tag-only hack. The first implementation
should wire tags, then leave the type surface ready for metrics and dimensions.

Recommended project-wide locks:

- `tags/definition`: blocks `tag.add` from MCP.
- `tags/assignment`: blocks MCP tag replacement on hypotheses and experiments.
- `tag-family/<name>/family`: blocks MCP-visible changes to family compliance if
  family-specific MCP paths ever exist.
- `metrics/definition`: future metric registry lock.
- `dimensions/definition`: future run-dimension registry lock.

Lock reads should appear in relevant MCP list surfaces. A model should be able
to discover the policy before hitting it.

`tag.list` should include:

- active tags
- families
- mandatory family rules
- lock state
- if requested in full detail, recent aliases/tombstones

Cleanup mutations remain UI-only. Do not add `tag.rename`, `tag.merge`, or
`tag.delete` to the model-facing MCP catalog.

## MCP Faults

Add a policy-specific fault kind.

```rust
FaultKind::PolicyViolation
```

It should be non-retryable. JSON-RPC can use a server-defined code such as
`-32001`. Tool output should carry the normal structured fault record with the
same plain text message.

Every policy fault should answer four questions:

1. What policy stopped the write?
2. What object or family is involved?
3. What must the model do instead?
4. Is there a replacement name if stale context caused the failure?

Good messages:

```text
tag registry is locked; new tags cannot be created from MCP; use an existing tag from tag.list or ask the supervisor
tag assignment is locked; experiment tag sets cannot be changed from MCP
tag `ls` was merged into `search/local`; use `search/local`
mandatory tag family `phase` is missing; include at least one of: baseline, ablation, optimization
```

Bad messages:

```text
invalid input
unknown tag
constraint failed
locked
```

`UnknownTag` should remain for truly unknown names. Names present in
`tag_name_history` should produce policy guidance, not unknown-tag ambiguity.

## Concurrency Semantics

Supervisor operations must be ordinary SQLite transactions.

Guarantees:

- Rename, merge, delete, family edits, and lock toggles are atomic.
- MCP reads and writes open fresh store state and see committed policy on the
  next operation.
- No service reload is required.
- Open experiments remain closable unless the close attempts a now-forbidden tag
  mutation.
- Revision guards protect UI edits from stale forms.

There should be no in-memory policy cache in the MCP host. If caching is ever
introduced, it must be invalidated by the existing refresh-token mechanism or by
SQLite revision state.

## Navigator Surface

Add a top-level project tab:

```text
Tags
```

The Tags page is a supervisor console, not a model-facing dashboard.

Top strip:

- tag definition lock state
- tag assignment lock state
- number of active tags
- number of families
- number of mandatory families
- number of orphaned tags

Family panel:

- family name
- mandatory status
- lock status
- member count
- missing-compliance count across active hypotheses and open experiments
- rename, lock, mandatory toggle, and delete-empty controls

Tag table:

- tag name
- family
- description
- hypothesis usage count
- experiment usage count
- last observed usage if available
- actions: rename, merge, delete, assign family

The table should make cleanup obvious:

- sort by low usage to find junk tags
- sort by family to find unclassified tags
- filter to orphan tags
- filter to duplicate-looking prefixes or aliases later

Dangerous actions should be one confirmation deep and should show impact:

```text
Merge `ls` into `search/local`.
This rewrites 4 hypothesis assignments and 11 experiment assignments.
The old name will remain as a model-readable alias.
```

## Events And Audit

Every supervisor cleanup mutation should record an event.

Minimum event facts:

- operation
- old name
- new name or target when relevant
- usage counts affected
- lock mode changed
- reason if supplied
- timestamp

The audit trail matters because tag cleanup changes the interpretation surface of
old experiments. It should be possible to answer why an old tag disappeared.

## Implementation Program

1. Add core domain types for stable tag IDs, tag family IDs, registry names,
   lock modes, lock scopes, tag dispositions, and mutation origin.
2. Cut schema to the new tag epoch and convert existing state to stable tag IDs.
3. Rewrite store tag reads and assignment edges around tag IDs.
4. Add supervisor store operations for rename, merge, delete, family edits, and
   lock toggles.
5. Add MCP policy enforcement for `tag.add`, hypothesis tag replacement,
   experiment tag replacement, and mandatory-family checks.
6. Add `FaultKind::PolicyViolation` and map policy store errors to that fault.
7. Expand `tag.list` projection to expose families, mandatory rules, locks, and
   full-detail name history.
8. Build the Tags navigator tab with usage counts and supervisor controls.
9. Add integration tests for stale model context during rename, merge, delete,
   mandatory family introduction, registry locks, and assignment locks.
10. Repeat the pattern for metric registry cleanup once tags are stable.

## Later Supervisory Controls

The same governance layer should eventually cover:

- metric rename and merge
- metric registry lock
- metric unit-family correction
- run-dimension allowed-value enumerations
- frontier freeze distinct from archive
- supervisor verdict override with audit
- stale hypothesis sweep
- provenance auto-tagging for model session identity
- naming pattern constraints for mature projects

The unifying rule is simple: models drive the search; supervisors govern the
language of the search once the shape is clear.
