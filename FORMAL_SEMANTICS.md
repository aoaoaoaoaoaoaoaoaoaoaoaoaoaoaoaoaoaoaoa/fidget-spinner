# Formal semantics

This document defines Fidget Spinner's domain rules. [GLOSSARY](GLOSSARY.md) names the objects; [SPEC](SPEC.md)
defines the tools, presentation and persistence. The ledger records assertions about an inquiry; it neither runs
experiments nor verifies the evidence it cites.

## 1. Objects and identity

A frontier belongs to one project and has a slug unique within it. It holds an objective, a sequence of regimes, and
finite sets of baselines B, hypotheses H, reviews R, edges E ⊆ H × H and waivers. Hypotheses, reviews and baselines
have separate slug namespaces. Slugs follow the shared engine's rule: at most 96 bytes of lowercase dot-separated
segments of `[a-z0-9_-]`, each starting alphanumeric, with parsing lowercasing. Metric keys are lowercase letters,
digits and underscores, starting with a letter.

A trial belongs to exactly one hypothesis and is identified by `slug#n`. Its n is one more than the largest n ever
used under that hypothesis and never changes; renaming the hypothesis renames its trials' identities.

## 2. Regimes, gauges and metrics

Regime k ≥ 1 has a gauge: contract text and a set of metrics keyed by name. The current regime is the latest. A
regime change installs regime k + 1 with a complete gauge; the first gauge is installed the same way. Every regime
change after the first requires a reason.

A metric has a unit, an optional direction, a KPI flag, an optional description, reference lines and a retired flag.
A KPI requires a direction. Within a regime:

- a metric may be added, retired or restored, and its KPI flag, description and reference lines may change;
- its unit and direction may change only while no baseline or trial of the regime, void or not, has a reading for it;
- the contract text may be edited, with a warning, because only the caller can judge whether the edit affects
  comparability.

Any other change of meaning requires a regime change. A gauge with more than eight unretired metrics draws a warning.

Units belong to quantity kinds:

| Kind | Units |
|---|---|
| time | ns, µs, ms, s, min, h |
| memory | B, kB, MB, GB, TB, KiB, MiB, GiB, TiB |
| dimensionless | 1, % |
| count | any other lowercase word; each word is its own kind |

Units are stored canonically. Aliases normalize as follows and cannot denote count kinds:

| Canonical unit | Aliases |
|---|---|
| s | sec, secs, second, seconds |
| ms | msec, millisecond, milliseconds |
| µs | us, microsecond, microseconds |
| ns | nanosecond, nanoseconds |
| min | minute, minutes |
| h | hr, hour, hours |
| B | byte, bytes |

Conversion is defined within a kind, where 1 = 100 %, and never across kinds. A conversion refusal names the
reading key, the supplied unit and kind, and the metric's unit and kind.

## 3. Readings

A reading assigns a key a finite value: a number in the metric's unit, or text of a number followed by a unit. A
reading is checked against the gauge of the regime its record belongs to: for a trial, the regime current when it is
recorded; for a baseline, the regime current when the baseline was created. For a key that is an unretired metric of
that gauge, a value whose unit has the metric's kind is converted to the metric's unit; a unit of another kind is
refused. Any other key yields an unlisted reading, stored as given, with a warning that names the closest metric key
when one is close.

## 4. Baselines, epochs and incumbency

A baseline belongs to the regime that is current when it is created. It holds artifacts, readings, a note, the set of
hypotheses it adopts, and a disposition: none (candidate), accepted or rejected, with a rationale. Readings may be
added or changed at any time; changing a reading of an accepted baseline draws a warning.

Accepting a baseline requires that it belong to the current regime and not be void. Each acceptance opens the next
epoch. The incumbent baseline is the accepted, non-void baseline of the current regime with the latest epoch; there
may be none. Rejecting, voiding or removing the incumbent baseline reinstates the previous qualifying baseline of the
regime without opening an epoch.

A regime change may name a bridge: a new baseline of the new regime that copies the outgoing incumbent baseline's
artifacts, takes the given readings, and is accepted with the regime change's reason as its rationale. A bridge
requires an outgoing incumbent baseline.

## 5. Hypotheses, edges and dockets

A hypothesis has a title and optionally a mechanism and a claim. Edges form an acyclic graph without self-edges; an
edge that would close a cycle is refused and the cycle reported.

Each hypothesis is on at most one docket; adding it to a docket removes it from any other. When accepting a baseline
makes it the incumbent baseline, every unsettled hypothesis on the previous incumbent baseline's docket moves to the
new one's docket, and the receipt names them. No other change of incumbency moves docket members: rejecting, voiding
or removing a baseline, and unvoiding one, move nothing, and a receipt for rejecting or voiding names the unsettled
docket members.

The state of a hypothesis is the first of these that applies:

1. settled: it has a disposition;
2. running: an execution start is asserted;
3. tested: it has a non-void trial;
4. waiting: an edge source is unsettled, or it is on the docket of a baseline that is not accepted;
5. ready.

Trials, dispositions and execution ahead of unsettled inputs are permitted. A tested hypothesis remains tested
when all its non-void trials precede the current regime; inspection identifies the latest such trial regime.

## 6. Trials

A trial has a nonempty note, readings, artifacts, a time, and the baseline it is against: the given one, or by
default the incumbent baseline when the trial is recorded, or none. Trials are never edited; they can be voided,
unvoided or removed. A trial on a settled hypothesis draws a warning, and so does a trial against a baseline of
another regime. A trial is compared with its baseline only when both belong to the same regime, on each metric for
which both have readings: the ratio of the trial's value to the baseline's, and whether it is better under the
metric's direction.

## 7. Dispositions, void and removal

Accept and reject apply to hypotheses and baselines; shelve applies to hypotheses. Each requires a nonempty
rationale. A disposition set over an existing one replaces it with a warning; reopening a hypothesis clears it.
Accepting a hypothesis records the incumbent baseline of that moment.

Void marks a trial or baseline invalid with a nonempty reason, and unvoid clears the mark. A void record stays in
every view and is excluded from charts, comparisons, incumbency and findings.

Remove erases a hypothesis, review, baseline or trial and every reference to it, as if the reference had never been
set. Removing a hypothesis also removes its trials, edges, docket entry, baseline adoptions and review memberships,
and is refused when it is a review's last member. Removing a baseline clears its docket, trials' comparison baseline,
acceptances' recorded incumbent baseline and any regime bridge naming it, and erases its epochs without reusing their
numbers. An acceptance without a recorded incumbent baseline is backed by any non-void trial; a regime without its
bridge is unbridged. Removal drops waivers whose finding subject was removed. Rename rewrites every reference,
including waiver identities. History shows operations on removed records redacted; the store keeps them, and the
slug becomes free.

## 8. Reviews

A review has a title, a nonempty charter, a nonempty set of member hypotheses, and an optional lesson. Membership may
overlap between reviews and creates no precedence. An open review is running when an execution start is asserted;
otherwise it is ready when every member is tested or settled, and waiting when not. A lesson completes the review;
reopening the review clears the lesson.

## 9. Ownership

A hypothesis or review has at most one owner, with instructions, and at most one execution start. Assign replaces the
owner, release clears it, start asserts execution, and stop clears execution. Owners are names without a registry.

## 10. Findings and waivers

Findings are recomputed from each state and identified by kind and subject:

| Identity | Present when |
|---|---|
| `unbacked/<hypothesis>` | The hypothesis is accepted without a non-void trial against the incumbent baseline of the moment of acceptance, or, when there was none, without any non-void trial. |
| `unbridged/<k>` | Regime k ≥ 2 has no bridge. |
| `unlisted/<k>/<key>` | Three or more non-void trials of regime k carry an unlisted reading for the key. |
| `unread/<baseline>/<key>` | The incumbent baseline has no reading for an unretired KPI of the current gauge. |
| `unaccepted/<baseline>/<hypothesis>` | An accepted, non-void baseline adopts a hypothesis that is not accepted. |

A waiver attaches a note to the identity of a present finding and applies whenever that identity recurs.

## 11. Patches

A patch applies its operations in order to a copy of the state and commits only if every operation succeeds. Each
reference must resolve when its operation applies. Refusals protect representation: malformed operations; invalid
slugs, keys, units or values; empty required text; unknown references; cycles and self-edges; rename collisions;
a unit of the wrong kind; a change of meaning after a metric's first reading; acceptance of a void baseline or one
outside the current regime; a bridge without an incumbent baseline; an empty review; removal of a review's last
member; and a waiver of an absent finding. Every other operation is accepted, with the warnings sections 2 to 7 name.

A patch to an unknown frontier creates it when its first operation is `frontier` with an objective, and is refused
otherwise. `conclude` records the frontier's lesson; a later patch with any other operation reopens the frontier,
with a warning.
