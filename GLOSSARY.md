# Glossary

[Formal semantics](FORMAL_SEMANTICS.md) defines the rules; [SPEC](SPEC.md) defines the tools, presentation and
persistence. Each term has one meaning in Fidget Spinner.

## Measurement

| Term | Meaning |
|---|---|
| Project | The canonical filesystem root that groups frontiers: the nearest ancestor holding `.git`, with a linked worktree resolved to its main checkout, or the directory itself outside Git. It has no record and no operations. |
| Frontier | An objective pursued under one gauge at a time, identified by project and slug; active or concluded. |
| Objective | What the frontier pursues and under which constraints. |
| Gauge | The measurement contract of a regime: contract text naming cohorts, envelope (machine or pool, threads, limits) and protocol, plus the metrics. |
| Metric | A key declared in the gauge, with a unit, an optional direction, a KPI flag, an optional description and reference lines. |
| KPI | A metric marked as a headline. It has a direction and is charted. |
| Diagnostic | A metric that is not a KPI. |
| Direction | Which values of a metric are better: lower or higher. |
| Reference line | A labelled constant on a KPI's chart, such as a rival's value or a target. |
| Unit | A time or memory unit, `1`, `%`, or a count label. |
| Quantity kind | A class of units that convert into each other: time, memory, dimensionless (`1` and `%`), or a single count label. |
| Count label | A unit naming what is counted, such as `cases`. It converts only to itself. |
| Reading | A value recorded for a key on a trial or baseline. |
| Unlisted reading | A reading whose key is not an unretired metric of the current gauge. It stays on its trial and is never charted or compared. |
| Baseline | A comparison state: artifacts, readings, adopted hypotheses and a docket. |
| Candidate | A baseline without a disposition. |
| Incumbent baseline | The accepted baseline of the current regime with the latest epoch that is neither void nor rejected. Trials are compared against it by default. Always written as two words; bare "incumbent" keeps its MIP meaning in solver projects. |
| Epoch | The tenure of one incumbent baseline. Each baseline acceptance opens the next epoch, numbered from 1 across the frontier. |
| Regime | The span of epochs under one gauge, numbered from 1. |
| Regime change | The installation of a new gauge. |
| Bridge | A regime's first baseline when it remeasures the outgoing incumbent baseline's artifacts under the new gauge. |
| Docket | The hypotheses due against a baseline. |
| Artifact | A commit, profile or evidence path recorded on a trial or baseline. |

## Inquiry

| Term | Meaning |
|---|---|
| Hypothesis | A claim under test: slug, title, and optionally a mechanism and a claim. |
| Mechanism | Why the hypothesis is expected to hold. |
| Claim | What must hold for the hypothesis to be accepted: a prediction or a bar. |
| Trial | One measurement or attempt under a hypothesis: a note, readings, the baseline it is against, and artifacts. Its identity is `slug#n`. |
| Edge | A precedence between hypotheses: the target waits until the source is settled. It may state what it carries forward. |
| Lane | The first segment of a dot-separated hypothesis slug. |
| Review | A set of hypotheses with a charter, completed by a lesson. A program is a review with an owner. |
| Charter | The question a review answers. |
| Lesson | What a review taught beyond its members' dispositions, or what a concluded frontier taught. |

## Outcomes and work

| Term | Meaning |
|---|---|
| Disposition | Accepted, rejected or, for hypotheses only, shelved; always with a rationale. |
| Rationale | The reason recorded with a disposition. |
| Settled | Having a disposition. |
| State | The derived condition of a hypothesis: waiting, ready, running, tested or settled. |
| Void | Marked invalid: still shown, but excluded from charts, comparisons, incumbency and findings. |
| Removed | Erased from every model-facing view and recoverable only from the store. |
| Finding | A fact computed from the current state that may need attention. It never blocks an operation. |
| Waiver | A note that dismisses one finding. |
| Owner | The agent named as pursuing a hypothesis or review. |
| Concluded | The state of a frontier ended by a lesson. |
| Patch | An ordered list of operations applied atomically. |
| Receipt | The response to a patch: its sequence number, warnings, and the findings it raised or cleared. |
