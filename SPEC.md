# Fidget Spinner interfaces

The [glossary](GLOSSARY.md) owns terminology and [formal semantics](FORMAL_SEMANTICS.md) the domain rules. This
document owns addressing, operations, inspection, presentation, the navigator and persistence. MCP output follows
`../mcp_depot/TOKEN_POLICY.md`.

## Addressing

Every tool takes optional `project` (a path) and `frontier` (a slug). The default project is the server's working
directory. A path resolves to its project: the nearest ancestor holding `.git`, a linked worktree resolving to its
main checkout through `commondir`, or the canonical path itself outside Git. `frontier.patch` requires `frontier`. A
read may omit it only when the project has exactly one active frontier; otherwise the read is refused with the list
of the project's frontiers. Responses name the frontier, and the project only when it is not the default. Cursors
belong to their project and frontier. Failure to resolve the working directory does not prevent startup: `system.health`
reports the reason, and calls that need a project refuse only when `project` is omitted, asking the caller to pass it.

## Tools

| Tool | Purpose |
|---|---|
| `frontier.patch` | Apply a patch, or rehearse it with `dry_run`, and return the receipt. |
| `frontier.status` | The summary, list views, prior-art search, and the project's frontiers. |
| `hypothesis.read` | One hypothesis: claim, trials, disposition, inputs, outputs, reviews, docket and owner. |
| `review.read` | One review: charter, members, lesson and owner. |
| `frontier.history` | Patches newest first, or the operations of one patch. |
| `system.health` | Process and store health. |

## Operations

A patch is a JSON array of operations, each an object with `op` and its fields. Where a field list says
`hypothesis | baseline`, exactly one of those keys names the target. Trials are named `slug#n`.

```
frontier   {objective}
conclude   {lesson}
regime     {contract, metrics: [metric], reason?, bridge?: {slug, readings, note?}}
gauge      {contract?, metrics?: [metric], retire?: [key], restore?: [key]}
baseline   {slug, artifacts?: [text], readings?: {key: value}, adopts?: [slug], docket?: [slug], note?}
docket     {baseline, add?: [slug], drop?: [slug]}
hypothesis {slug, title?, mechanism?, claim?, after?: [slug], reviews?: [slug]}
edge       {from, to, carries?}
unedge     {from, to}
review     {slug, title?, charter?, members?: [slug], add?: [slug], drop?: [slug]}
trial      {hypothesis, note, readings?: {key: value}, against?: baseline, artifacts?: [text]}
accept     {hypothesis | baseline, rationale}
reject     {hypothesis | baseline, rationale}
shelve     {hypothesis, rationale}
reopen     {hypothesis | review}
void       {trial | baseline, reason}
unvoid     {trial | baseline}
remove     {hypothesis | review | baseline | trial}
lesson     {review, text}
waive      {finding, note}
assign     {hypothesis | review, owner, instructions?}
release    {hypothesis | review}
start      {hypothesis | review}
stop       {hypothesis | review}
rename     {hypothesis | review | baseline, to}

metric  = {key, unit, better?: "lower" | "higher", kpi?: bool, about?, refs?: {label: number}}
value   = number | "number unit"
```

- `hypothesis`, `review` and `baseline` create or update; creation requires `title` for a hypothesis, `title`,
  `charter` and members for a review. Omitted fields, and fields given as `null`, are unchanged.
- `hypothesis.after` adds edges from the listed hypotheses; `hypothesis.reviews` adds the hypothesis to the listed
  reviews.
- `review.members` and `baseline.docket` replace; `add` and `drop` edit. `baseline.adopts` and `artifacts` replace.
- `baseline.readings` and `bridge.readings` merge by key.
- `gauge.metrics` adds metrics or updates existing ones.
- `trial.against` defaults to the incumbent baseline.

## Receipts

A receipt reports the sequence number, or that the patch was rehearsed; the warnings of each operation by index; the
findings the patch raised or cleared; and only the hypothesis-state counts that changed, with their before and after
values. Its porcelain header is `frontier F · seq N`, or `frontier F · rehearsed, not applied` for a dry run; changes
share one line, such as `ready 0→1 · tested 0→1`. Warnings, raised findings and cleared findings appear only when present.
A refusal names the operation's index and name and the reason, and nothing is applied. Warnings name the subject, and, for an
unlisted reading, the closest metric key when one is close.

## Inspection

Selection, totals, paging, cursors, `render` (`porcelain` by default, or `json`) and `detail` (`concise` by default,
or `full`) follow the conventions of Vauban's SPEC, section Inspection: a scope sets denominators and filters select
rows; pages carry `items`, `total` and `next_cursor`; a cursor continues the same query at the same ledger sequence;
previews of long text are marked incomplete and name the call that reads them whole.

A removal after a cursor's snapshot invalidates non-history continuations; the refusal asks the caller to rerun the
query without the cursor. History continuations remain usable and apply current removal redaction.

Projections omit absent fields; an unsettled hypothesis has no disposition. Porcelain omits empty collections and
zero counts, and uses `—` for absent table cells. JSON may retain empty collections and zero counts. Tables omit
columns already fixed by their enclosing list: tested hypotheses omit state and disposition; ready hypotheses also
omit trial count. Readings use `key value unit`, with numbers printed by f64 Display. The summary joins KPI definitions,
reference lines and incumbent-baseline readings in one table: `key | unit | better | incumbent | refs`. Full history
operations preserve asserted empty arrays and nulls because they are part of the operation payload.

### Status

`frontier.status` without `view` returns the summary, under 2 KB for a typical frontier:

1. the objective;
2. the current regime: number and first line of the contract;
3. the incumbent baseline: slug, epoch and artifacts; one KPI table joins keys, units, directions, incumbent readings
   and reference lines;
4. docket progress of the incumbent baseline and of each candidate: settled members over members;
5. running hypotheses and reviews with owners;
6. tested hypotheses awaiting disposition;
7. the first ready hypotheses and ready reviews;
8. the latest dispositions with rationale previews;
9. the latest lessons;
10. unwaived findings with their identities.

Views:

| View | Rows |
|---|---|
| `hypotheses` | slug, state, disposition, owner, docket, trial count, title |
| `reviews` | slug, state, owner, members by state, title |
| `trials` | identity, time, baseline, KPI readings with ratios, note preview |
| `baselines` | slug, regime, epoch, disposition, artifacts, KPI readings, docket progress |
| `regimes` | number, reason, bridge, metrics |
| `findings` | identity, waiver, subject preview |
| `frontiers` | the project's frontiers: slug, active or concluded, counts by state, objective preview |

Scope selectors: `under` (a slug and those beneath it at a dot boundary), `review` (its members) and `docket` (a
baseline's docket). Filters: `state`, `disposition`, `owner`, `lane` and `text`. `text` is a case-insensitive
substring over titles, mechanisms, claims, trial notes, rationales, charters and lessons; with `scope: "project"` it
searches every frontier of the project, concluded ones included, and rows name their frontier.

### Packets

`hypothesis.read` sections: `claim` (title, mechanism, claim), `trials` (paged, newest first, each with its readings
against its baseline), `disposition`, `inputs` and `outputs` (each edge's other end with state, disposition, rationale
preview and what the edge carries), `reviews` (with lesson previews), `docket` and `owner`. `review.read` sections:
`charter`, `members` (state, disposition, rationale preview), `lesson` and `owner`.

### History

`frontier.history` lists patches newest first with sequence, time, operation count and targets. With `seq` and
`detail: "full"`, it returns that patch's operations. Operations on removed records show only the operation name and
the record kind.

## Navigator

`fidget-spinner ui` serves a loopback board. The index lists frontiers grouped by project, each project under a
heading that folds. A frontier's board uses the shared layout engine: hypotheses are cells grouped by lane, edges are
drawn, reviews are coloured sets, and each baseline holds its docket the way a Vauban wall holds its nodes. Epoch
boundaries are thin separators labelled with their baseline; a regime change is a thick separator marked bridged or
unbridged. Settled hypotheses beyond the horizon are hidden by default. Clicking a cell opens a foreground with its
trial table.

The header shows the objective and one chart per KPI: baseline readings as steps across epochs, trials as dots
coloured by disposition, reference lines dashed, the unit on the axis, every point a link to its record. The line
breaks at an unbridged regime change. Values show three significant figures.

## Store

One SQLite store at `$XDG_STATE_HOME/fidget-spinner/ledger.sqlite3` holds every project's frontiers as append-only
patch logs, each identified by project and slug. A frontier's state is the fold of its patches. Patches replay
forever: an operation's meaning never changes, and a new meaning requires a new operation or a new store format with
a migration. Tests use an isolated store and never the live one.
