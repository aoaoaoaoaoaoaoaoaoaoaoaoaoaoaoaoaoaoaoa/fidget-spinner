# Fidget Spinner Spec

## Navigator Design Language

The navigator is an austere local-first experiment ledger for a power user who
already knows the domain. It should read like a dense lab notebook: terse,
forensic, and structurally navigable. It is not a marketing surface, a general
dashboard, or a scrapbook.

The UI optimizes for answer density per viewport. A page should make it cheap to
answer, in order: what happened, why it happened, what changed, where the
evidence lives, and what should be inspected next. Space spent on scaffolding,
empty states, duplicated titles, or low-signal provenance is suspect until
proven necessary.

The information hierarchy is narrative first, evidence second, provenance last.
Outcome pages should surface the decision and its rationale before numerical
ledgers, command recipes, timestamps, backend names, or graph context. Metrics
are evidence for a claim; they are not the claim. Commit hashes, dimensions,
argv, env, and backend are provenance; they should remain available, but they
should not seize the first screen.

Experiment pages use this order:

1. Compact experiment header: title, owning frontier, owning hypothesis, status,
   verdict, tags, and a terse summary.
2. Outcome, when present. Within outcome: verdict, rationale, analysis, metric
   ledgers, then collapsed provenance.
3. Artifacts, when present.
4. Influence network, when present.

Use these component primitives consistently:

- Dense fact strips for short metadata runs such as verdict, primary metric,
  closed timestamp, backend, and commit.
- Narrative blocks for rationale, analysis summary/body, frontier situation, and
  hypothesis body.
- Metric ledgers for primary and supporting metrics.
- Provenance disclosures for command recipe, dimensions, backend, commit, and
  other recoverability details.
- Chip rows for tags, status, verdicts, and compact categorical labels.
- Link chips for traversing to hypotheses, experiments, and artifacts.
- Section cards only when the section carries real content. Empty sections should
  usually disappear instead of announcing absence.

Typography should stay compact. Monospace hierarchy comes from placement,
weight, and labels more than raw size. Large titles are a liability because they
consume scarce vertical context. Default grids should not allocate vast empty
columns around short values; short facts belong in fact strips, not sparse
four-column panels.

Every new element must pass a friction and grouping review before it lands:

- Does this control really need a separate confirmation click, or can the
  user's selection apply immediately with clear feedback?
- Does this element have a natural semantic sibling it should sit beside
  instead of below?
- Is this an edit affordance that belongs directly beside the value being
  edited instead of in a detached form field?
- Do we really need to put this on a new line?
- Does this string convey anything useful that is not already obvious from
  context?
- Is the layout spending a full row on something that is only a modifier,
  status, or secondary action?

Prefer reactive controls for low-risk, reversible supervisor actions. Prefer
same-line semantic clusters when an action qualifies or changes the status
beside it. New rows are reserved for new ideas, not for mechanically convenient
buttons. Avoid separate edit fields when a chip or label can become editable in
place. Prefer symbolic square chips over text buttons for common actions such
as edit, archive, delete, copy, and expand. Delete labels, subtitles, eyebrows,
and helper copy that merely describe the page the user is already on.

## Supervisory Registry Governance

Driving models should be allowed to explore quickly and leave imperfect
taxonomy behind. The product should not try to solve this solely by prompt
discipline. Mature frontiers need supervisor controls that clean, merge,
normalize, constrain, and eventually lock experimental registries while models
continue running.

Tags are the first governed registry. The durable direction is documented in
`docs/supervisory-tag-governance.md`.

The core pattern:

- registry cleanup is a supervisor/UI concern, not a normal MCP tool surface
- model-facing MCP writes obey current registry policy on every call
- policy lives in SQLite and takes effect without service reload
- locks distinguish definition edits from assignment edits
- tag identity is stable and internal; names are human-facing handles
- stale model context is answered by aliases or tombstones with clear porcelain
  errors
- mandatory families constrain future writes without invalidating history

This pattern should generalize to metrics, run dimensions, frontier freezing,
and other mature-frontier cleanup controls.
