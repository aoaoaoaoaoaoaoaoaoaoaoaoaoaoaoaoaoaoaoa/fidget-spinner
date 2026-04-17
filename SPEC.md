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
as add, edit, archive, delete, copy, and expand. Delete labels, subtitles, eyebrows,
and helper copy that merely describe the page the user is already on.

### State Bands Over Panels

Page-level counts, locks, toggles, filters, and other global modifiers belong in
a compact state band, not in section cards. A section card is reserved for a
content object, narrative block, table, plot, or list that merits its own
heading. A boolean policy switch almost never earns a card.

Before adding a panel, ask:

- Is this a distinct content region, or merely state/configuration for the page?
- Could this live beside the metric/fact it qualifies?
- Would this still deserve a heading if it had only one value?
- Is this prose explaining what a terse label plus hover help could explain?
- Is the UI repeating the same state in a chip, button, and heading?

Policy controls should be compact inline switches colocated with the facts they
govern. Use hover help for scope and consequences. Prefer labels that name the
user-visible effect over implementation categories: `new tags` beats
`definition`, and `registry edits` beats storage-level lock names. Supervisor
controls set MCP policy; they must not disable or veto the supervisor UI itself.

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
- locks distinguish new-entry creation from registry-structure edits
- tag identity is stable and internal; names are human-facing handles
- stale model context is answered by aliases or tombstones with clear porcelain
  errors
- mandatory families constrain future writes without invalidating history

This pattern should generalize to metrics, run dimensions, frontier freezing,
and other mature-frontier cleanup controls. Metrics should not grow generic
families or retain backend scoreboard pinning; the durable metric contract is
frontier-local KPIs with ordered metric alternatives, documented in
`docs/metric-kpi-governance.md`.

## Archive Semantics

`Archived` is a reserved term of art: only a frontier can be manually archived,
and only the supervisor surface can do it. Archiving is not a general lifecycle
state, not a synonym for closed, stale, superseded, deprecated, inactive, or
hidden, and not a per-entity cleanup knob.

All non-frontier hiding is derived visibility policy. A registry entity such as
a metric or tag is hidden by default when it appears on at least one frontier and
every frontier it appears on is archived. A never-recorded registry entity stays
default-visible so a supervisor can add vocabulary before models use it. Other
future hiding mechanisms must name their own cause precisely, such as
`superseded`, `deprecated`, `closed`, or `inactive`, instead of borrowing
archive vocabulary.

MCP is an active-world interface. It must not expose archive inspection or
archive manipulation controls, and it must not dump hidden-by-archive entities
with warning flags. To MCP-using models, archived frontiers and entities visible
only through archived frontiers behave as absent. Supervisor UI and CLI paths may
still inspect hidden records for cleanup, merge, rename, or deletion.
