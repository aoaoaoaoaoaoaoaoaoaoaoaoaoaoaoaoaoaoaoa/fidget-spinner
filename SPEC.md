# Fidget Spinner

Fidget Spinner keeps an inquiry's record: open questions, attempts and their outcomes, judgments over evidence,
supervision, and optional measurement. One append-only ledger holds every frontier; an MCP server, a CLI and a
loopback navigator read and write it. This document is the product's only specification, and behaviour is never to
be inferred from the code. MCP output also follows `../mcp_depot/TOKEN_POLICY.md`.

## 1. Doctrine

The more compactions, handoffs and sessions a procedure spans, the more durable records and mechanical checks it
needs; work that lives and dies within one model context may stay informal. Checks preserve continuity and expose
drift; they are not process gates. Model judgment governs method, and principals own contextual exceptions. Process
flexibility never waives authorization or mathematical obligations.

- FS is an inquiry's memory across discontinuities: what is open, who pursues it, what was judged over which
  evidence, and why.
- Refusals protect representation (§14), plus one administrative state, archive (§13). Everything else is a receipt
  warning or a finding (§12), and neither blocks.
- Admissibility of evidence is project policy. FS records chronology (planned, started, reported, judged) and never
  labels evidence supportive, preregistered or confirmatory.
- Inquiry is empirical discovery: an iset poses a question whose answer may land either way, and a trial is an
  attempt whose outcome may land either way. Neither direction is a rejection, and FS presumes neither.
- Every record resolves at most once and is voided at most once. Nothing is reopened, corrected or unvoided. A later
  answer is a new record, and whoever voids a result sweeps what rests on it.

## 2. Ledger

A **project** is a canonical filesystem root: the nearest ancestor holding `.git`, with a linked worktree resolved to its
main checkout, or the canonical path itself outside Git. A **frontier** belongs to one project and is addressed by a
slug unique within it.

The store holds each frontier as an append-only log of **patches**. A patch is a nonempty ordered list of operations.
Its operations apply in order to a copy of the state, and the patch commits only if every one succeeds; a refused
patch changes nothing. A committed patch takes the next global sequence number, its **seq**, and the server's time. An
operation's **position** is its patch's seq with its zero-based index in the patch. A frontier's state is the fold of
its patches, and operations replay forever under the meaning they were written with. A **rehearsal** (`dry_run`)
validates a patch against the current state without committing it.

Every read states the frontier seq it reflects. A record's **revision** is the position of its last write, rendered
`seq.index` and otherwise opaque. Numeric inputs are integers in [−(2⁵³−1), 2⁵³−1]; null never stands for zero, absence
or an unbounded value.

## 3. Records, names and removal

The **records** are trials and isets, together called **work** (§6), and baselines (§8). Schemas, regimes and metrics
are addressed by id, number and key. Each record is an immutable incarnation beneath a renamable **name**. Work shares
one namespace; baselines have another.

A name is a slug: at most 96 bytes of lowercase dot-separated segments of `[a-z0-9_-]`, each beginning alphanumeric;
input is lowercased. A trial may instead be named `s#n`. A trial created without a name must join an iset; it takes the
slug of the first iset it joins and the next number of that iset's counter, which never reuses a number and survives
renames. The **lane** of a name is its first segment. A metric key consists of lowercase letters, digits and
underscores and begins with a letter.

Operations name records by current name. Each name resolves to an incarnation when its operation applies, and reads
render current names. `rename` gives a record a free name. `remove` erases a record from every model-facing view and
frees its name; a record later created under that name is a new incarnation and inherits nothing. Removing work drops it
from the isets holding it and from the inputs of work waiting on it, and removing an iset leaves its members. Any other
reference to a removed record, whether consumption, control, treatment, adoption or a data reference, renders as removed
and is never retargeted, and a resolution that consumed it carries the mark `source removed`. The store keeps removed
records' operations; history redacts them (§17).

`annotate` appends a dated **annotation** of at most 100 words to a record, whether open, resolved or void.
Annotations accumulate and are never edited; they change no data, revision, consumption, freeze, finding or guard.

## 4. Data and schemas

**Data** is a JSON value: null, boolean, number, text, list, object, or a **reference**
`{"$fs":"ref","kind":K,"id":N}`, where K is `trial`, `iset`, `baseline` or `metric` and N is a current name, or
`key@k` for a metric of regime k. `$fs` is otherwise forbidden as an object key. A reference must resolve when written; it
follows renames and removal and creates no membership, input or consumption. Text resembling a name stays text.
Data is stored exactly as written; references are traversed whether or not the data conforms.

A frontier's **registry** maps schema ids, which are slugs, to sequences of immutable **definitions**, each
identified by the position that defined it. `define` appends a definition; a redefinition names the id's current
definition revision (§10) and warns when a record is bound to the id. A definition outside this grammar is refused:

```
definition = {carrier: "iset", data: fields, answer: fields, title?, description?}
           | {carrier: "trial", plan: fields, report: fields, title?, description?}
fields     = {name: field, …}
field      = {type, presence: "required" | "optional", title?, description?,
              show?: "headline" | "badge" | "card" | "section" | "hidden"}
type       = "bool" | "text" | "int" | "float" | {enum: [text, …]} | {list: type} | {map: type}
           | {record: fields} | {variant: {tag: fields, …}} | {ref: K}
```

`int` admits integral numbers in [−(2⁵³−1), 2⁵³−1] however spelled; `float` admits any number and is approximate;
`enum` admits one of its nonempty, distinct texts; `list` is ordered and admits duplicates; `map` has text keys;
`record` and `fields` are closed; `variant` admits exactly `{"tag", "value"}`, the value conforming to that tag's fields;
`ref` admits a reference of kind K. There are no imports, cross-schema references, regular expressions, constraints,
formulas, defaults, coercions or untyped fields. `show` is a display hint, never an access control.

Work names a schema when created or edited: `id` binds the id's current definition and `id@rev` binds that definition.
A name resolving to no definition stays a requested, **unvalidated** binding; a later definition never resolves it.
Work without a binding is **untyped**: neither conforming nor nonconforming. Data violating its binding (a wrong type,
an extra key, a missing required field, or the other carrier's definition) is stored exactly, with a warning and a
nonconforming finding. Where the bound definition is not the id's current one, reads also show conformance to the
current one; rebinding is explicit.

## 5. Measurement

Measurement is optional; a frontier may have no regime.

Regime k ≥ 1 has a **gauge**: contract text and metrics keyed by name. The current regime is the latest. A regime
change installs regime k + 1 with a complete gauge and requires a reason after the first. A **metric** has a unit, an
optional direction (lower or higher is better), a KPI flag, an optional description, reference lines and a retired
flag; a KPI requires a direction. Within a regime a metric may be added, retired or restored, and its KPI flag,
description and reference lines may change; its unit and direction may change only while no record of the regime has
a reading for it; the contract text may change with a warning. Any other change of meaning requires a regime change.
A gauge with more than eight unretired metrics draws a warning.

Units belong to quantity kinds: time (ns, µs, ms, s, min, h), memory (B, kB, MB, GB, TB, KiB, MiB, GiB, TiB),
dimensionless (1, %), and count, in which every other lowercase word is its own kind. Units are stored canonically:
sec, secs, second and seconds mean s; msec and millisecond(s) ms; us and microsecond(s) µs; nanosecond(s) ns;
minute(s) min; hr and hour(s) h; byte(s) B. Conversion is defined within a kind, with 1 = 100 %, and never across
kinds.

A **reading** assigns a key a finite value: a number in the metric's unit, or text holding a number and a unit. A
trial's readings are checked against its regime and a baseline's against the regime it was created in. For an
unretired metric of that gauge, a value of the metric's kind converts to the metric's unit and a unit of another kind
is refused. Any other key, and every reading without a regime, is stored as given as an **unlisted** reading, with a
warning naming the closest metric key.

A report is compared, per metric with both readings in one regime, between its **treatment** observation, the trial's
own readings, and its **control** observation: the report's control readings when supplied, otherwise its control
baseline's readings as consumed (§9). A comparison states both sources and their identities, the unit, the ratio
treatment/control and whether it is better under the metric's direction. Effects, uncertainties, coverage and claims
belong in report data under a schema and are shown apart from the computed ratio.

## 6. Work

Work is a trial or an iset. **Membership** places work in isets; work may belong to several isets, and an iset may be
empty. **Inputs** order work: work waits for its inputs, and an input constrains only its target, never the target's
members. Membership and inputs together stay acyclic, and a refusal names the cycle. Membership is not dependency.

An iset's **title** names its question in at most 40 characters, and a trial's **synopsis** tells its attempt in at most
120; each is one line, and the navigator's labels and cards show them. Creating work requires its display text: a
written patch that leaves an iset it created untitled, or a trial it created without a synopsis, is refused, while
history written before FS 4.2 replays without them. `title` and `synopsis` replace it in any state, even on a concluded
frontier, and like annotations change no data, revision, consumption, freeze, finding or guard.

Work is **open** until it **resolves**, a trial when reported and an iset when judged, or is voided. Work is **done**
when resolved and not void. Open work's data, plan, binding, inputs and assignment, and an open iset's membership, may
change; resolution and void freeze them. Finished work may still join open isets.

### Trials

A **trial** is an attempt. `plan` creates one with its first plan revision: data, binding, regime (the current one, if
any), control and treatment. The **control** is a baseline: the given one, or, under a regime, the incumbent baseline
at planning, recorded as defaulted. The **treatment** is optional: a baseline, or an opaque identity such as an
artifact revision or an external program. An `edit` of a trial is an **amendment**: it appends a plan revision, and
every revision stays readable.

`report` resolves a trial once with readings, optional control readings measured alongside, artifacts, data, and the
plan revision executed: the current one, unless the report names an earlier one or `unknown`. A later revision never
becomes the executed one, and a trial without a plan reports `unknown`. Reporting a name that names no trial creates a
trial without a plan and reports it at once, a **retrospective** report, whose control defaults as in planning.

A trial's **state** is `reported`; otherwise `running` while execution is asserted; otherwise `waiting` while an input is not
done; otherwise `ready`.

### Isets

An **iset** (integration set) poses a question; its data holds the question and its working candidate. `iset` creates
one with data, binding, members and inputs; `edit` changes its data or binding, and `member` its membership. `judge`
resolves it once with an **answer**, checked against the binding's answer fields. FS reads no verdict from an answer;
a negative or inconclusive answer settles like any other. Membership is dynamic until judgment, and an early judgment
binds: work reported afterwards is not its evidence.

An iset's state is the first of these that applies:

1. `settled`: it is judged;
2. `running`: execution is asserted on it, or on an unreported trial among its members at any depth;
3. `waiting`: a member is open;
4. `tested`: a reported, non-void trial is among its members at any depth;
5. `waiting`: an input is not done;
6. `ready`.

States inform views and never prohibit early execution, reporting or judgment.

### Supervision

`assign` names work's owner, with optional instructions, and binds the assignment to the work's
**specification**: an iset's data, a trial's plan, and either's binding and inputs. A later change to the specification
makes the assignment `stale` (§11) until the work is reassigned; membership, execution assertions and annotations never
do. `release` clears it. `start` asserts execution and `stop` retracts the assertion. A trial has one assignment however many
isets share it. Owners are free-form names, not accounts.

## 7. Void

`void` marks a trial, iset or baseline invalid, once, with a reason; it is never undone. A void record stays visible and
is excluded from charts, comparisons, incumbency and evidence. It reads as **withdrawn** when nothing had resolved or
consumed it, and **invalidated** otherwise. Voiding an iset never voids its members. Void never satisfies an input:
work waiting on a void input stays waiting until its inputs are changed by hand. Views show void in place of a state.

## 8. Baselines and elections

A **baseline** is a comparison state: its **identity**, the artifacts that name what it is (commits, configurations,
evidence paths); readings; a note; and the regime current at its creation. A **candidate** is a baseline never elected.
A baseline's identity freezes at its first election or first use as a report's control or treatment, and its readings
freeze when an election, or a report's control without supplied control readings, consumes them. Frozen parts never
change; the note never freezes.

`elect` names a baseline, a rationale and optional adopted isets. It requires a non-void baseline of the current regime
and opens an **epoch**; electing a baseline again opens another. The **incumbent baseline** is the baseline of the
latest election of the current regime whose baseline is neither void nor removed; voiding it reinstates the previous
one without an epoch. An election records each adopted iset's judgment, or that it had none; adoption conveys no
verdict.

A regime change may name a **bridge**: a new baseline of the new regime with the outgoing incumbent baseline's
artifacts and the given readings, elected with the regime change's reason. A bridge requires an outgoing incumbent.

## 9. Consumption

A resolution consumes evidence when written, and what it consumed never changes:

- a judgment consumes the done work reached through its iset's membership, passing through open isets and stopping at
  resolved or void ones; void and open members stay visible but are not evidence;
- a report consumes its control's identity, its control's readings unless control readings were supplied, and a
  baseline treatment's identity;
- an election consumes its baseline and every adopted iset that is judged or void;
- a conclusion consumes every done work record and every elected, non-void baseline of its frontier.

A record whose consumption includes a void record **rests on void**, from birth if the source was already void. The
mark is a fact shown beside the source and its reason: never a finding, never cleared. It reaches one hop, since a mark
is bookkeeping and not transitive validity, and the sweep voids what falls by hand. Reads show permanently what each
resolution consumed and what it passed over (open, void or unjudged members and adoptees). A **dependents** read
returns any record's consumers, the work waiting on it and the isets containing it, at any time.

A later answer is a new iset. New understanding takes an iset containing the old answer and the new evidence, and the
old answer stands: supersession is not void. Void evidence takes a successor over the survivors, which may contain
its void predecessor as a member that is not evidence. A marked answer that still holds stays unvoided: a
reassessment iset containing it says why, and the mark stays. A replacement for a misrecorded report is a new trial
whose lineage lives in its data; its report names the plan revision actually executed, possibly its predecessor's by
reference in data, or `unknown`. FS counts recorded trials and infers no multiplicity.

## 10. Guards

An `edit` of work that existed before its patch names the work's revision (`rev`); a redefinition names the id's current
definition revision. Guards are checked against the state before the patch: a patch's own operations never
invalidate its guards, differing guards for one record are refused, and a record created earlier in the patch needs
none. A conflict refuses the whole patch, reports the actual revision and never rebases.

`judge` may carry `seen: S`, a frontier seq the caller has read, which the ledger keeps. The judgment is refused when a
patch after S, other than its own, changed anything it consumes, validates against or resolves: the iset's data,
binding or membership; the membership traversed; any resolution or void reached; the incarnation a name resolves to;
and what the patch's own earlier operations resolved implicitly, such as a defaulted control resolving to an
incumbent baseline elected after S. A change counts even if later reverted. Reads show such a judgment as `guarded at S`,
a passed concurrency check rather than proof of review, and any other as `read not recorded`.

`guard {head: S}` refuses its patch when the frontier has a patch after S.

## 11. Findings

A finding is a fact computed from the current state that may need attention. It is identified by rule and subject,
never blocks, and clears when its condition ends. Receipts list the findings a patch raised and cleared.

| Finding | Present when |
|---|---|
| `unsettled/<iset>` | A judged iset has an open member or an input that is not done. |
| `unjudged/<baseline>/<iset>` | The incumbent baseline's election adopted an iset that is neither judged nor void. |
| `nonconforming/<work>` | Data, a plan, a report or an answer violates its binding. |
| `unvalidated/<work>` | Work's binding requests a definition that does not exist. |
| `stale/<work>` | Open work's specification changed after its assignment was bound (§6). |
| `superseded/<trial>` | An open trial's defaulted control is no longer the incumbent baseline. |
| `unbridged/<k>` | Regime k ≥ 2 has no bridge. |
| `unlisted/<k>/<key>` | Three or more non-void trials of regime k carry an unlisted reading for the key. |
| `unread/<baseline>/<key>` | The incumbent baseline lacks a reading for an unretired KPI of the current gauge. |

## 12. Warnings

A receipt warning names its operation and subject. Warnings accompany nonconforming or unvalidated data, unlisted
readings (with the closest metric key), contract edits, gauges over eight metrics, redefinition of a bound schema id,
plans, reports and elections naming a void source, a control of another regime, a judgment of an iset with an open
member or an input that is not done, and an election adopting an unjudged iset.

## 13. Conclusion and archive

`conclude` records a frontier's lesson once and consumes its done work. A concluded frontier refuses every operation but
`void`, `remove`, `rename` and `annotate`; a later void marks the conclusion. Continuing means a new frontier. A conclusion's
view shows what it consumed and the open work left at conclusion.

**Archive** is the owner's administrative end state, taken only in the navigator and only on a concluded frontier.
An archived frontier refuses every operation, naming the archive, until he unarchives it, and its reads stay
complete. A patch carrying `archive` or `unarchive` is refused.

## 14. Refusals

A patch is refused, naming the operation index and field, for:

- malformed operations, unknown fields, invalid names, keys, units, values or paths, empty required text, and an
  annotation over 100 words;
- references that do not resolve, name collisions, and membership or input cycles;
- a unit of the wrong kind, and a change of a metric's meaning after its first reading;
- definitions outside the grammar;
- failed guards;
- writes to resolved or void records, a second report or judgment, a second void, and changes to frozen parts of a
  baseline;
- an election of a void baseline or one outside the current regime, and a bridge without an incumbent baseline;
- operations other than `void`, `remove`, `rename` and `annotate` on a concluded frontier, and every operation on an
  archived one;
- `archive`, `unarchive` and the migration's operations (§19).

A patch to an unknown frontier creates it when its first operation is `frontier`, and is refused otherwise.

## 15. Operations

A patch is a JSON array of operations, each an object with `op` and its fields. `work` names a trial or an iset; where a
field list says `work | baseline`, exactly one of those keys names the target.

```
frontier {objective}
conclude {lesson}
guard    {head}
regime   {contract, metrics: [metric], reason?, bridge?: {slug, readings, note?}}
gauge    {contract?, metrics?: [metric], retire?: [key], restore?: [key]}
define   {schema, definition, rev?}
iset     {slug, title, schema?, data?, members?: [work], into?: [iset], after?: [work]}
plan     {trial?, synopsis, schema?, data?, control?, treatment?, into?: [iset], after?: [work]}
report   {trial?, plan?, readings?, control_readings?, artifacts?: [text], data?,
          synopsis?, schema?, control?, treatment?, into?: [iset], after?: [work]}
edit     {work, rev?, data?, set?: [{path, value}], delete?: [path], schema?, unbind?, control?, treatment?}
member   {iset, add?: [work], drop?: [work]}
after    {work, add?: [work], drop?: [work]}
judge    {iset, answer, seen?}
baseline {slug, artifacts?: [text], readings?, note?}
elect    {baseline, rationale, adopts?: [iset]}
void     {work | baseline, reason}
remove   {work | baseline}
rename   {work | baseline, to}
annotate {work | baseline, text}
title    {iset, text}
synopsis {trial, text}
assign   {work, owner, instructions?}
release  {work}
start    {work}
stop     {work}

metric    = {key, unit, better?: "lower" | "higher", kpi?: bool, about?, refs?: {label: number}}
readings  = {key: number | "number unit"}
treatment = {baseline: slug} | {identity: text}
path      = [key, …]
```

- `data` and `answer` are data (§4); `data` defaults to `{}`. `schema` is `id` or `id@rev`.
- `iset.members` places existing work in the new iset; `into` places the new record in existing isets; `after` gives its
  inputs.
- `plan.trial` names the trial; without it, `into` is required and supplies the name (§3).
- `report` names an existing open trial, or creates a retrospective one; `synopsis`, `schema`, `control`, `treatment`,
  `into` and `after` apply only to creation, which requires `synopsis`. `plan` is a revision of the trial's plan or `"unknown"`.
- `edit` replaces the data object whole with `data`, or edits it by `set` and `delete`; `schema` rebinds and `unbind: true`
  removes the binding; `control` and `treatment` apply to trials. Paths are literal arrays of object keys relative to
  the data object: the empty path, array indexes and implicit parent creation are refused, and parents must exist and
  be objects. An edit's changes are simultaneous against the value before it; duplicate or overlapping paths refuse.
  `set` replaces one whole value and lists are replaced whole; `delete` removes one present member. The resulting data
  is then checked against the resulting binding.
- `baseline` creates or updates: `artifacts` replace, `readings` merge by key, `note` replaces.
- `annotate` appends `text`, at most 100 words, to the record's annotations (§3); it applies to resolved and void
  records and on a concluded frontier.
- `member` and `after` add and drop. `gauge.metrics` adds metrics or updates existing ones.
- `judge.seen` and `guard.head` are seqs (§10).

## 16. Receipts

A receipt reports the seq, or that the patch was rehearsed; each operation's warnings by index; the records it created,
with defaulted controls and assigned trial names; opened epochs, named by baseline and election time; the findings it
raised and cleared; and the work states whose counts changed, with before and after values. A judgment's receipt lists
the work it made ready. A void's receipt lists the resolutions that now rest on it and the work waiting on it. A
refusal reads `operation i (op): path: problem`, and nothing applies. Unknown operation and field names suggest near ones and
list the operation's fields.

## 17. Reads

### Addressing

Every tool takes optional `project` (a path) and `frontier` (a slug). The default project is the server's working
directory. `frontier.patch` requires `frontier`. A read may omit it only when the project has exactly one unconcluded
frontier; otherwise it is refused with the project's unarchived frontiers, each marked active or concluded. Responses
name the frontier, and the project only when it is not the default. When the working directory cannot be resolved,
the server still starts; `system.health` reports why, and calls that need a project refuse only when `project` is omitted.

### Tools

| Tool | Purpose |
|---|---|
| `frontier.patch` | Apply a patch, or rehearse it with `dry_run`, and return the receipt. |
| `frontier.status` | The summary, list views, search, and the project's frontiers. |
| `record.read` | One trial, iset or baseline, by section. |
| `schema.read` | One schema id's definitions and bindings, or the definition grammar. |
| `frontier.history` | Patches newest first, or one patch's operations. |
| `system.health` | Process and store health. |

Every read follows TOKEN_POLICY: `render` is `porcelain` (default) or `json` and selects representation only; `detail` is
`concise` (default) or `full`. Collections carry `items`, `total` and `next_cursor`; a cursor continues the same query at the
same seq and is refused, asking for a rerun, after a removal or a mismatch. Porcelain renders data as compact JSON
with references as `@kind:name` (`@kind:-` when removed), uniform rows as tables with a header once, and never
indented JSON; previews of long text are marked and name the read that returns them whole. Porcelain and JSON carry
the same information. A selector or section that does not apply to its view or record is ignored with a warning naming
it; a read none of whose sections applies is refused, naming the record's sections.

### Status

`frontier.status` without `view` returns the summary: the objective, and the conclusion and archive of a concluded frontier;
the current regime's number and contract line; the incumbent baseline with its election time, artifacts and a KPI
table of keys, units, directions, readings and reference lines; counts of isets and trials by state, void counts, and
counts of untyped, unvalidated and nonconforming work; running work with owners; tested isets awaiting judgment; ready
work; the latest judgments with answer previews; and findings. Each collection shows its total and a bounded prefix
with a route to the full list.

| View | Rows |
|---|---|
| `isets` | name, title, state, owner, members by state, schema, headline |
| `trials` | name, synopsis, state, regime, control, KPI readings with ratios, schema, headline |
| `baselines` | slug, regime, elections, frozen parts, artifacts, KPI readings |
| `regimes` | number, reason, bridge, contract, metrics; newest first |
| `schemas` | id, carrier, definitions, current revision, bound, unvalidated and nonconforming counts |
| `findings` | identity and generated statement |
| `frontiers` | the project's frontiers: slug, standing, counts by state, objective preview |

Selectors: `under` (an iset's members at any depth), `lane`, `state` (a state or `void`), `owner`, `schema` and `text`. `text`
is a case-insensitive substring over all names, data, answers, plans, reports, notes, annotations, rationales and lessons, and
over data field paths; with `scope: "project"` it searches every frontier of the project, and its rows are `frontier | record
| field | excerpt`.

### Packets

`record.read` names `work` or `baseline` and optional `sections`:

- trial: `head` (name, state or void, regime, binding, validity and conformance to the current definition, marks, revision,
  annotation count), `plans` (every revision, with the executed one marked), `report` (readings, control readings,
  comparisons, artifacts, data, consumed sources, or `none yet`), `isets`, `inputs`, `dependents`, `owner` (with the bound
  specification revision and, when stale, the revision that changed it), `annotations` (newest first);
- iset: `head` (as a trial's, without regime), `data`, `members` (with states), `judgment` (answer, guard, consumed and
  passed over, or `none yet`; FS 3 judgments it replaced follow as `prior_judgments`, §19), `isets`, `inputs`,
  `dependents`, `owner`, `annotations`;
- baseline: `head` (regime, artifacts, readings, note, frozen parts, void, annotation count), `elections` (with
  adoptions), `dependents`, `annotations`.

Void records show withdrawn or invalidated and the reason; records resting on void name the void source and its
reason.

### Schemas

`schema.read` with `schema` returns the id's definitions, newest first, each with its revision; `writing`, one line per
variant or ref field of the newest definition showing a value to write, with placeholders; and the work bound to it.
Without `schema` it returns the definition grammar with an example of each carrier.

### History

`frontier.history` lists patches newest first with seq, time, operation count and targets. With `seq` and
`detail: "full"` it returns that patch's operations as written. Operations on removed records show only the operation
name and the record kind, and references to removed records inside data render as removed.

## 18. Navigator

`fidget-spinner ui serve --bind 127.0.0.1:8913` serves a loopback navigator. The index lists active frontiers before
concluded ones, newest patch first within each: each frontier over its project, a bar of its settled isets with
running ones hatched, its work counted by state, its latest patch and whether it is concluded, with an Archive button
when it is. Archived frontiers fold under a closed Archived disclosure, each with an Unarchive button, and their boards
keep their addresses. An archive or unarchive POST requires a matching Origin and appends that operation through the
transactional writer.

A frontier's board draws work through the shared layout engine. Every trial is a cell showing its synopsis, else its
headline or name, then its badge fields, coloured by its state or void, with rests-on-void and validity marks. An iset is
a wall around the cells below it at any depth, packed together: its line takes the iset's state colour and the line style
of its schema's title, the commonest solid, and a tab on its top edge shows its title, else its headline. An iset that holds isets grounds its wall in a faint
texture, a different one for each such wall on the board, cycling through six. An iset with
no cell below it is a hollow cell of its own, its trial count in its strip. Inputs are drawn as edges, an iset's from its
latest cell and to its earliest. Elections draw thin separators labelled with their baseline and time; regime changes
draw thick separators marked bridged or unbridged. Cells joined by walls sit in the epoch of their latest record. A
trial shows while an innermost iset holding it is open or was judged within the Settled horizon, a trial in no iset by
its own resolution; the control keeps judgments since the latest epoch or the last 2, 3 or 5 epochs began, none or all,
by default the last 2, and open work always remains visible. `?under=<iset>` scopes the board to an iset and its
records at any depth.

Above the board, the frontier's counts, regime and incumbent baseline lead, with the line styles of the iset kinds drawn at
the right when there are several; then its objective, conclusion and one tile per KPI. A tile names the KPI, its direction,
its unit and the incumbent baseline's value; from two
readings on it charts baseline readings as steps across epochs and reported trials as dots coloured by state, reference lines
dashed, every point a link to its record. A regime change draws a vertical rule that no step
crosses; on a bridged change's rule an arrow runs from the outgoing level, a hollow point, to the bridge's reading. A dot describes its trial, never a containing iset's answer. Epochs are named by
baseline and time, never by ordinal. Hovering a cell or a wall's tab shows its state, headline and badge fields, answer
preview, latest report and neighbours; clicking opens a foreground with the whole record, its annotations included.

## 19. Store and migration

One SQLite store at `$XDG_STATE_HOME/fidget-spinner/ledger.sqlite3` holds every project's frontiers in store format 24. A
format-23 store (FS 4.1) lacks only titles and synopses, and a format-22 store (FS 4.0) also `annotate`; each is promoted
unchanged when a writer first opens it. A format-21 store (FS 3) is migrated then, in one transaction: each operation is translated in place, preserving seqs, times, patch boundaries and
the order of operations within each patch.

| FS 3 | Format 24 |
|---|---|
| `frontier {objective}` | `frontier {objective, legacy_through}`, then `define` of `fs3.hypothesis`, `fs3.review` and `fs3.trial` |
| `hypothesis {slug, title, mechanism?, claim?}` | `iset {slug, schema: "fs3.hypothesis", data: {title, mechanism?, claim?, carries: {}}}` |
| `edge {from, to, carries?}` | `after {work: to, add: [from]}`, then with carries `edit {work: to, set: [{path: ["carries", from], value}]}` |
| `review {slug, title, charter, members}` | `iset {slug, schema: "fs3.review", data: {title, charter}, members}`; an update becomes `edit` and `member` |
| `trial {hypothesis, note, readings?, against?, artifacts?}` | `report {trial: "<hypothesis>#n", into: [hypothesis], schema: "fs3.trial", control: against?, readings, artifacts, data: {note}}` |
| `accept`, `reject`, `shelve {hypothesis, rationale}` | `judge {iset, answer: {disposition, rationale}}` |
| `lesson {review, text}` | `judge {iset: review, answer: {lesson}}` |
| `baseline {slug, artifacts?, readings?, adopts?, note?}` | `baseline {slug, artifacts, readings, note}`; adoptions move to its election |
| `accept {baseline, rationale}` | `elect {baseline, rationale, adopts}` |
| `reopen {hypothesis | review}` | `reopen {iset}` |
| `void {trial, reason}`, `assign`, `start`, `stop` | the same on work |
| `regime`, `conclude`, `archive` | unchanged |
| `docket` | dropped |

The three `fs3` schemas define a hypothesis (data: title, mechanism, claim, and what each input carries; answer:
disposition and rationale), a review (data: title and charter; answer: lesson) and a trial (report: note). FS 3
operations absent from the store refuse the migration. A translated legacy trial has no plan and no execution
assertion; a legacy disposition or lesson is a judgment with `read not recorded`. Patches through `legacy_through` replay
under FS 3 law: writes to resolved or void records are accepted, a judgment of a judged iset replaces its current
judgment, and `reopen` withdraws it; a replaced or withdrawn judgment stays readable as a prior judgment.
Later patches follow this specification. `legacy_through` and `reopen` exist only in migrated history.

Tests use isolated stores, never the live one. The CLI mirrors the tools: `fidget-spinner mcp serve`, `patch`, `status`,
`read`, `schema`, `history`, `health`, `import` (timestamped `{at, ops}` JSONL into a new frontier) and `ui serve`.
