# Frontier Membership Without Partitioning

## Status

Prospective design note only. Do not implement yet.

The current single-frontier posture is acceptable for now. This note captures a
clean future cut once overlapping frontier work becomes real enough to justify
the model change.

## Thesis

Frontiers should be scopes, not partitions.

`hypothesis` and `experiment` are the real scientific graph vertices.
`frontier` is a control object with a brief and a bounded grounding projection.
It should not own vertices exclusively.

The current model still treats frontier as an ownership partition:

- every hypothesis has one `frontier_id`
- every experiment has one `frontier_id`
- experiments inherit that frontier from their owning hypothesis
- influence edges may not cross frontier boundaries

That is stricter than the real ontology. In practice:

- one hypothesis may matter to multiple frontiers
- one experiment may be relevant to multiple frontiers
- later hypotheses may be directly informed by experiments from another frontier

The current shape is tolerable while there is effectively one live frontier. It
is not the right long-term model.

## Desired Ontology

### Graph vertices

The true graph vertices remain:

- `hypothesis`
- `experiment`

### Control objects

`frontier` is not a graph vertex. It is a scope object that owns:

- label
- objective
- status
- brief

### Sidecars

`artifact` remains an attachable reference-only sidecar.

## Relations

Two relations remain fundamental.

### 1. Ownership

Every experiment has exactly one owning hypothesis.

This is the canonical tree spine. It should remain mandatory and singular.

### 2. Influence

Hypotheses and experiments may both influence later hypotheses or experiments.

This is the sparse DAG laid over the ownership spine. It should no longer be
artificially constrained by frontier boundaries.

### 3. Membership

Frontier membership becomes a separate many-to-many relation.

A hypothesis or experiment may belong to zero, one, or many frontiers.

This relation is about scope and visibility, not causality.

## Recommended Data Model

### Remove vertex-owned frontier identity

Delete the idea that hypotheses or experiments intrinsically belong to exactly
one frontier.

Concretely, the long-term cut should remove:

- `HypothesisRecord.frontier_id`
- `ExperimentRecord.frontier_id`
- `CrossFrontierInfluence`

### Add explicit membership tables

Use explicit membership relations instead:

- `frontier_hypotheses(frontier_id, hypothesis_id, added_at)`
- `frontier_experiments(frontier_id, experiment_id, added_at)`

The split tables are preferable to one polymorphic membership table because the
types and invariants are simpler, and queries stay more direct.

### Preserve one hard invariant

An experiment may only be attached to a frontier if its owning hypothesis is
also attached to that frontier.

This prevents the frontier scope from containing orphan experiments whose
canonical spine is missing from the same view.

That still allows:

- one hypothesis in multiple frontiers
- one experiment in multiple frontiers
- one experiment in a subset of its hypothesis frontiers

but disallows:

- experiment in frontier `B` while owning hypothesis is absent from `B`

## Query Semantics

### `frontier.open`

`frontier.open` should derive its surface from membership, not ownership.

Its bounded output should still be:

- frontier brief
- active tags
- live metric keys
- active hypotheses with deduped current state
- open experiments

But all of those should be computed from frontier members.

### Active hypotheses

Active hypotheses should be derived from a bounded combination of:

- roadmap membership in the frontier brief
- hypotheses with open experiments in the frontier
- hypotheses with latest non-rejected closed experiments still relevant to the
  frontier

The exact rule can stay implementation-local as long as the result is bounded
and legible.

### Live metrics

The right default is not “all metrics touched by frontier members.”

The live metric set should be derived from:

- all open experiments in the frontier
- the immediate comparison context for those open experiments

A good default comparison context is:

- the union of metric keys on all open experiments
- plus the metric keys on immediate experiment ancestors of those open
  experiments

This keeps the hot path focused on the active A/B comparison set rather than
every historical metric ever observed in the scope.

## Surface Changes

When this cut happens, the public model should grow explicit membership
operations rather than smuggling scope through create-time ownership.

Likely surfaces:

- `frontier.member.add`
- `frontier.member.remove`
- `frontier.member.list`

Or, if we prefer type-specific verbs:

- `frontier.hypothesis.add`
- `frontier.hypothesis.remove`
- `frontier.experiment.add`
- `frontier.experiment.remove`

I prefer the type-specific form because it is clearer for agents and avoids a
generic weakly-typed membership tool.

Read/list filters should then interpret `frontier=` as membership selection.

## Brief and Roadmap Semantics

The frontier brief remains a singleton owned by the frontier.

Its roadmap should reference frontier-member hypotheses only.

That is a healthy constraint:

- the brief is a scoped grounding object
- roadmap entries should not point outside the scope they summarize

If a hypothesis becomes relevant to a frontier roadmap, attach it to that
frontier first.

## Migration Shape

This should be a red cut when it happens.

No backward-compatibility layer is needed if the project is still early enough
that re-seeding is acceptable.

The migration is straightforward:

1. Create frontier membership tables.
2. For every hypothesis, insert one membership row from its current
   `frontier_id`.
3. For every experiment, insert one membership row from its current
   `frontier_id`.
4. Drop `frontier_id` from hypotheses and experiments.
5. Delete `CrossFrontierInfluence`.
6. Rewrite frontier-scoped queries to use membership joins.

Because the current world is effectively single-frontier, this is mostly a
normalization cut rather than a semantic salvage operation.

## Why This Is Better

This model matches the clarified ontology:

- the scientific truth lives in hypotheses and experiments
- the frontier is a bounded lens over that truth
- scope should not distort causality

It also makes later comparative work cleaner:

- one hypothesis can be reused across multiple frontier narratives
- one experiment can inform more than one frontier without duplication
- influence edges can remain honest even when they cross scope boundaries

That is a better fit for a system whose real purpose is an austere experimental
record spine rather than a partitioned project tracker.
