# Fidget Spinner interfaces

The [glossary](GLOSSARY.md) owns terminology and [formal semantics](FORMAL_SEMANTICS.md) the domain rules. This
document owns addressing, operations, inspection, presentation, the navigator and persistence. MCP output follows
`../mcp_depot/TOKEN_POLICY.md`.

## Addressing

Every tool takes optional `project` (a path) and `frontier` (a slug). The default project is the server's working
directory. A path resolves to its project: the nearest ancestor holding `.git`, a linked worktree resolving to its
main checkout through `commondir`, or the canonical path itself outside Git. `frontier.patch` requires `frontier`. A
read may omit it only when the project has exactly one active frontier; otherwise the read is refused with the list
of the project's unarchived frontiers, each marked active or concluded. Responses name the frontier, and the project
only when it is not the default. Cursors belong to their project and frontier. Failure to resolve the working
directory does not prevent startup: `system.health` reports the reason, and calls that need a project refuse only
when `project` is omitted, asking the caller to pass it.

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
archive    {}
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
Receipts also list created trial identities (`slug#n`), baselines, regimes and opened epochs, named by baseline and acceptance time rather than by an ordinal
(for example, `opened epoch of baseline e5 · since 2026-10-14 20:43Z`). A refusal has the form
`operation i (op): path: problem`, and nothing is applied. Unknown operation names suggest nearby operations;
unknown fields suggest nearby fields and list the operation's fields. Warnings name the subject, and, for an
unlisted reading, the closest metric key when one is close.

## Inspection

Selection, totals, paging, cursors, `render` (`porcelain` by default, or `json`) and `detail` (`concise` by default,
or `full`) follow the conventions of Vauban's SPEC, section Inspection: a scope sets denominators and filters select
rows; pages carry `items`, `total` and `next_cursor`; a cursor continues the same query at the same ledger sequence;
previews of long text are marked incomplete and name the call that reads them whole. Cursors are compact opaque
base64url tokens. Query, collection and address are checked before sequence; a mismatch says
`cursor belongs to another query; rerun without it`.

A removal after a cursor's snapshot invalidates non-history continuations; the refusal asks the caller to rerun the
query without the cursor. History continuations remain usable and apply current removal redaction.

Projections omit absent fields; an unsettled hypothesis has no disposition. Porcelain omits empty collections and
zero counts, and uses `—` for absent table cells. JSON may retain empty collections and zero counts. Tables omit
columns already fixed by their enclosing list: tested hypotheses omit state and disposition; ready hypotheses also
omit trial count. Readings use `key value unit`, with numbers printed by f64 Display. The summary joins KPI definitions,
reference lines and incumbent-baseline readings in one table: `key | unit | better | incumbent | refs`. Full history
operations preserve asserted empty arrays and nulls because they are part of the operation payload. Porcelain table
cells never contain JSON objects or arrays. Routes are labelled `more:`; a route template such as
`hypothesis=<slug>` substitutes the named column of its table. Concise timestamps have minute precision
(`2026-10-15 00:25Z`); full detail retains exact timestamps. A primary result without rows reports `matches 0/N`.
Unknown argument names suggest their nearest valid paths, including nested paths such as `filter.text`. Unknown
frontier, hypothesis, review, baseline, trial, finding and tool names suggest at most three names within edit distance
two, or a unique prefix completion.

### Status

`frontier.status` without `view` returns the summary, under 2 KB for a typical frontier:

1. the objective, and the lesson of a concluded frontier, marked when archived;
2. the current regime: number and the first line of the contract, at most 160 characters, with a full-read route;
3. the incumbent baseline: slug, acceptance time (`since`) and artifacts; one KPI table joins keys, units, directions, incumbent readings
   and reference lines;
4. docket progress of the incumbent baseline and of each candidate: settled members over members;
5. running hypotheses and reviews with owners;
6. tested hypotheses awaiting disposition;
7. the first ready hypotheses and ready reviews;
8. the latest dispositions with rationale previews;
9. the latest lessons;
10. unwaived findings as `identity | statement | waiver`, with statements derived from finding identities rather
    than copied from record text.

Summary collections show their totals and a bounded prefix: up to eight KPIs, three findings and two rows in other
collections, with a full-list route when more exist. Disposition rows are `hypothesis | disposition | rationale`,
with rationale previews of at most 120 characters. Tested rows whose non-void trials all precede the current regime
annotate the count with the latest trial regime, for example `2 (regime 3)`.

Views:

| View | Rows |
|---|---|
| `hypotheses` | slug, state, disposition, owner, docket, trial count, title |
| `reviews` | slug, state, owner, members by state, title |
| `trials` | identity, time, baseline, KPI readings with ratios, note preview |
| `baselines` | slug, regime, acceptance time (`since`), disposition, artifacts, KPI readings, docket progress |
| `regimes` | number, reason, bridge, contract, metrics; newest first |
| `findings` | identity, generated statement, waiver |
| `frontiers` | the project's frontiers: slug, standing (active, concluded or archived), counts by state, objective preview |

Scope selectors: `under` (a slug and those beneath it at a dot boundary), `review` (its members) and `docket` (a
baseline's docket). Filters: `state`, `disposition`, `owner`, `lane` and `text`. `text` is a case-insensitive
substring over titles, mechanisms, claims, trial notes, rationales, charters and lessons; with `scope: "project"` it
searches every frontier of the project, concluded ones included. Prior-art rows are
`frontier | record | field | excerpt`: one matching field per record, with trials identified as `slug#n`, and at most
120 characters around its first match. The denominator counts searchable hypotheses, trials, reviews and baselines.
Read a trial through its hypothesis's trials section; read other slugs through `hypothesis.read`, `review.read`, or
the baselines view. The tool description gives these routes once rather than repeating them in every row.

An inapplicable filter or scope is ignored with a warning naming it. Regimes ignore all filters and scopes; frontiers
ignore non-project scopes and filters other than text; reviews ignore disposition; baselines ignore state and owner.
Other views retain their applicable selectors.

### Packets

`hypothesis.read` sections: `claim` (title, mechanism, claim), `trials` (paged, newest first, each with its readings
against its baseline), `disposition`, `inputs` and `outputs` (each edge's other end with state, disposition, rationale
preview and what the edge carries), `reviews` (with lesson previews), `docket` and `owner`. `review.read` sections:
`charter`, `members` (`slug | state | disposition | rationale`, with rationale previews at most 120 characters),
`lesson` and `owner`. Trial comparison cells use `elapsed 0.8× better` notation.

### History

`frontier.history` lists patches newest first with sequence, time, operation count and targets. With `seq` and
`detail: "full"`, it returns that patch's operations. Operations on removed records show only the operation name and
the record kind.

## Navigator

`fidget-spinner ui serve --bind 127.0.0.1:8913` serves a loopback board. The index lists active frontiers before
concluded ones, newest patch first within each: each frontier over its project, a bar of its settled hypotheses
(accepted, rejected and shelved, with running ones hatched), its hypotheses counted by decision or state, its latest
patch and whether it is concluded, with an Archive button when it is. It omits archived frontiers, whose boards keep
their addresses. A frontier's name shows its objective on hover. An archive POST requires a matching Origin and
appends an `archive` patch through the transactional writer.

A frontier's board uses the shared layout engine: hypotheses are cells titled by their titles, grouped by lane and
showing their trial counts; edges are drawn, reviews are coloured sets, and each baseline holds its docket the way a
Vauban wall holds its nodes. Epoch boundaries are thin separators labelled with their baseline and acceptance time.
Once a frontier has changed gauge, each regime begins at a thick separator marked bridged or unbridged. A settled
hypothesis belongs to the epoch of its disposition, rather than its creation. The Settled control keeps dispositions
since the latest epoch or the last 2, 3 or 5 epochs began, none or all; by default the last 2. Unsettled hypotheses
always remain visible.

Above the board, the frontier's counts, regime and incumbent baseline lead its objective, its conclusion, one tile per
KPI and its reviews in their board colours. A tile names the KPI, its better direction, its unit and the incumbent
baseline's value. From two readings on, it charts baseline readings as steps across epochs, trials as dots coloured by decision
or state and reference lines dashed, every point a link to its record; otherwise it lists the reference lines. The
line breaks at an unbridged regime change. Epochs are named by baseline, never by an ordinal: a chart labels epoch
boundaries with their baselines where they fit and titles them with their acceptance times. Values show three
significant figures.

Hovering a cell shows its state, claim, mechanism, rationale, latest trial, reviews and neighbours. Clicking it opens a
foreground with the whole record, its neighbourhood and its trial table, whose readings show their change against the
trial's baseline. Trials, baselines and reviews open the same way from charts, tables and the review list: a trial
marks its row, a baseline draws its docket and a review its members.

## Store

One SQLite store at `$XDG_STATE_HOME/fidget-spinner/ledger.sqlite3` holds every project's frontiers as append-only
patch logs, each identified by project and slug. A frontier's state is the fold of its patches. Patches replay
forever: an operation's meaning never changes, and a new meaning requires a new operation or a new store format with
a migration. Tests use an isolated store and never the live one.
