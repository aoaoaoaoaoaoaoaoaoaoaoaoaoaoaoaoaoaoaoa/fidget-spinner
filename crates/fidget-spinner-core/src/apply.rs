use crate::{cycle::closes_cycle, *};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use time::OffsetDateTime;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Warning {
    GaugeEdited {
        regime: RegimeNumber,
    },
    ManyMetrics {
        regime: RegimeNumber,
        count: usize,
    },
    UnlistedReading {
        subject: String,
        key: MetricKey,
        closest: Option<MetricKey>,
    },
    AcceptedBaselineReading {
        baseline: Slug,
        key: MetricKey,
    },
    TrialOnSettled {
        hypothesis: Slug,
    },
    ReplacedDisposition {
        subject: String,
    },
    DocketMoved {
        from: Slug,
        to: Slug,
        hypotheses: Vec<Slug>,
    },
    DocketStranded {
        baseline: Slug,
        hypotheses: Vec<Slug>,
    },
    CrossRegimeTrial {
        trial: TrialId,
        baseline: Slug,
    },
    ReopenedFrontier,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RecordKind {
    Hypothesis,
    Review,
    Baseline,
    Trial,
    Metric,
}
#[derive(Clone, Debug, PartialEq, thiserror::Error, Serialize)]
pub enum OpRefusal {
    #[error("objective: frontier creation requires a first frontier operation")]
    MissingObjective,
    #[error("frontier: only a concluded frontier can be archived")]
    PrematureArchive,
    #[error("{field}: must be nonempty")]
    Empty { field: String },
    #[error("{field}: required for creation")]
    MissingField { field: String },
    #[error("{kind:?}: unknown {identity}{suggestions}")]
    Unknown {
        kind: RecordKind,
        identity: String,
        suggestions: String,
    },
    #[error("gauge: no gauge is installed")]
    NoGauge,
    #[error("reason: required after the first regime")]
    MissingRegimeReason,
    #[error("bridge: requires an outgoing incumbent baseline")]
    NoIncumbentBridge,
    #[error("metrics.{key}.better: KPI requires a direction")]
    KpiDirection { key: MetricKey },
    #[error(
        "metrics.{key}: already has readings in regime {regime}; its unit and direction cannot change"
    )]
    MeaningChanged {
        regime: RegimeNumber,
        key: MetricKey,
    },
    #[error("metrics.{key}: duplicate metric")]
    DuplicateMetric { key: MetricKey },
    #[error("readings: invalid reading {value:?}")]
    InvalidValue { value: Value },
    #[error("readings: {0}")]
    Unit(#[from] UnitError),
    #[error("refs: reference lines must have finite values")]
    NonfiniteReference,
    #[error("baseline: {baseline} is void")]
    VoidBaseline { baseline: Slug },
    #[error("baseline: {baseline} belongs to a previous regime")]
    OldBaseline { baseline: Slug },
    #[error("members: review {review} must have at least one member")]
    EmptyReview { review: Slug },
    #[error("hypothesis: cannot remove {hypothesis}, the last member of review {review}")]
    LastMember { hypothesis: Slug, review: Slug },
    #[error("to: {kind:?} {to} already exists")]
    RenameCollision { kind: RecordKind, to: Slug },
    #[error("finding: {finding} is not present{suggestions}")]
    AbsentFinding {
        finding: FindingId,
        suggestions: String,
    },
    #[error("edge: closes a cycle: {path:?}")]
    Cycle { path: Vec<Slug> },
    #[error("identity: counter exhausted")]
    Exhausted,
    #[error("readings.{key}: {from} is {from_kind}; metric {key} is {to_kind} ({to})")]
    ReadingUnit {
        key: MetricKey,
        from: Unit,
        from_kind: QuantityKind,
        to: Unit,
        to_kind: QuantityKind,
    },
}

#[derive(Clone, Debug, PartialEq, thiserror::Error, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Refusal {
    #[error("operation {index} ({op}): {cause}")]
    Operation {
        index: usize,
        op: String,
        cause: Box<OpRefusal>,
    },
    #[error("operation {index} ({op}): edge: closes a cycle: {path:?}")]
    Cycle {
        index: usize,
        op: String,
        path: Vec<Slug>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Created {
    Trial(TrialId),
    Baseline(Slug),
    Regime(RegimeNumber),
    Epoch {
        baseline: Slug,
        #[serde(with = "time::serde::rfc3339")]
        at: OffsetDateTime,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Patched {
    pub frontier: Frontier,
    pub warnings: BTreeMap<usize, Vec<Warning>>,
    pub raised: BTreeMap<FindingId, Option<String>>,
    pub cleared: BTreeMap<FindingId, Option<String>>,
    pub before: Counts,
    pub after: Counts,
    pub created: Vec<Created>,
}

fn required(raw: &str, field: &str) -> Result<String, OpRefusal> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        Err(OpRefusal::Empty {
            field: field.into(),
        })
    } else {
        Ok(trimmed.into())
    }
}
fn missing(field: &str) -> OpRefusal {
    OpRefusal::MissingField {
        field: field.into(),
    }
}
fn unknown(kind: RecordKind, identity: &impl ToString) -> OpRefusal {
    OpRefusal::Unknown {
        kind,
        identity: identity.to_string(),
        suggestions: String::new(),
    }
}
fn empty_ownership() -> Ownership {
    Ownership {
        owner: None,
        started: None,
    }
}

impl Frontier {
    pub fn patched(&self, ops: &[Op], at: OffsetDateTime) -> Result<Patched, Refusal> {
        if self.objective.is_none() && !matches!(ops.first(), Some(Op::Frontier { .. })) {
            return Err(Refusal::Operation {
                index: 0,
                op: ops.first().map_or_else(|| "patch".into(), Op::name),
                cause: Box::new(OpRefusal::MissingObjective),
            });
        }
        let before = Assay::new(self);
        let mut frontier = self.clone();
        let mut warnings = BTreeMap::new();
        let mut reopening = self.conclusion.is_some();
        let mut created = Vec::new();
        for (index, op) in ops.iter().enumerate() {
            let mut log = Vec::new();
            let previous_epoch = frontier.last_epoch;
            let new_baseline = match op {
                Op::Baseline { slug, .. } => !frontier.baselines.contains_key(slug),
                _ => false,
            };
            if reopening && !matches!(op, Op::Conclude { .. } | Op::Archive {}) {
                frontier.conclusion = None;
                reopening = false;
                log.push(Warning::ReopenedFrontier);
            }
            if let Err(mut cause) = frontier.apply(op, at, &mut log) {
                let candidates: Vec<String> = match &cause {
                    OpRefusal::Unknown { kind, .. } => match kind {
                        RecordKind::Hypothesis => frontier
                            .hypotheses
                            .keys()
                            .map(ToString::to_string)
                            .collect(),
                        RecordKind::Review => {
                            frontier.reviews.keys().map(ToString::to_string).collect()
                        }
                        RecordKind::Baseline => {
                            frontier.baselines.keys().map(ToString::to_string).collect()
                        }
                        RecordKind::Trial => frontier
                            .hypotheses
                            .iter()
                            .flat_map(|(slug, h)| {
                                h.trials.keys().map(move |n| format!("{slug}#{n}"))
                            })
                            .collect(),
                        RecordKind::Metric => frontier
                            .current_regime()
                            .map(|r| r.gauge.metrics.keys().map(ToString::to_string).collect())
                            .unwrap_or_default(),
                    },
                    OpRefusal::AbsentFinding { .. } => Assay::new(&frontier)
                        .findings
                        .keys()
                        .map(ToString::to_string)
                        .collect(),
                    _ => Vec::new(),
                };
                if let OpRefusal::Unknown {
                    identity,
                    suggestions,
                    ..
                } = &mut cause
                {
                    let names = slug::nearest(identity, candidates.iter().map(String::as_str));
                    if !names.is_empty() {
                        *suggestions = format!("; nearest: {}", names.join(", "));
                    }
                } else if let OpRefusal::AbsentFinding {
                    finding,
                    suggestions,
                } = &mut cause
                {
                    let names =
                        slug::nearest(&finding.to_string(), candidates.iter().map(String::as_str));
                    if !names.is_empty() {
                        *suggestions = format!("; nearest: {}", names.join(", "));
                    }
                }
                let op = op.name();
                return Err(match cause {
                    OpRefusal::Cycle { path } => Refusal::Cycle { index, op, path },
                    cause => Refusal::Operation {
                        index,
                        op,
                        cause: Box::new(cause),
                    },
                });
            }
            match op {
                Op::Trial { hypothesis, .. } => created.push(Created::Trial(TrialId {
                    hypothesis: hypothesis.clone(),
                    number: frontier.hypotheses[hypothesis].last_trial,
                })),
                Op::Baseline { slug, .. } if new_baseline => {
                    created.push(Created::Baseline(slug.clone()));
                }
                Op::Regime { bridge, .. } => {
                    created.push(Created::Regime(
                        frontier.regimes[frontier.regimes.len() - 1].number,
                    ));
                    if let Some(bridge) = bridge {
                        created.push(Created::Baseline(bridge.slug.clone()));
                    }
                }
                _ => {}
            }
            if frontier.last_epoch != previous_epoch {
                let epoch = frontier
                    .epochs
                    .last()
                    .unwrap_or_else(|| unreachable!("acceptance opens an epoch"));
                created.push(Created::Epoch {
                    baseline: epoch.baseline.clone(),
                    at: epoch.at,
                });
            }
            if !log.is_empty() {
                warnings.insert(index, log);
            }
        }
        let after = Assay::new(&frontier);
        let raised = after
            .findings
            .iter()
            .filter(|(id, _)| !before.findings.contains_key(*id))
            .map(|(id, f)| (id.clone(), f.clone()))
            .collect();
        let cleared = before
            .findings
            .into_iter()
            .filter(|(id, _)| !after.findings.contains_key(id))
            .collect();
        Ok(Patched {
            frontier,
            warnings,
            raised,
            cleared,
            created,
            before: before.counts,
            after: after.counts,
        })
    }

    pub fn replay(&mut self, ops: &[Op], at: OffsetDateTime) -> Result<(), Refusal> {
        *self = self.patched(ops, at)?.frontier;
        Ok(())
    }

    fn apply(
        &mut self,
        op: &Op,
        at: OffsetDateTime,
        log: &mut Vec<Warning>,
    ) -> Result<(), OpRefusal> {
        match op {
            Op::Frontier { objective } => self.objective = Some(required(objective, "objective")?),
            Op::Conclude { lesson } => {
                self.conclusion = Some(Conclusion {
                    lesson: Lesson {
                        text: required(lesson, "lesson")?,
                        at,
                    },
                    archived: self.conclusion.as_ref().is_some_and(|c| c.archived),
                });
            }
            Op::Archive {} => {
                self.conclusion
                    .as_mut()
                    .ok_or(OpRefusal::PrematureArchive)?
                    .archived = true;
            }
            Op::Regime {
                contract,
                metrics,
                reason,
                bridge,
            } => {
                let contract = required(contract, "contract")?;
                let reason = reason
                    .as_deref()
                    .map(|r| required(r, "reason"))
                    .transpose()?;
                if !self.regimes.is_empty() && reason.is_none() {
                    return Err(OpRefusal::MissingRegimeReason);
                }
                let outgoing = self.incumbent_baseline().cloned();
                if bridge.is_some() && outgoing.is_none() {
                    return Err(OpRefusal::NoIncumbentBridge);
                }
                if let Some(bridge) = bridge
                    && self.baselines.contains_key(&bridge.slug)
                {
                    return Err(OpRefusal::RenameCollision {
                        kind: RecordKind::Baseline,
                        to: bridge.slug.clone(),
                    });
                }
                let mut gauge = Gauge {
                    contract,
                    metrics: BTreeMap::new(),
                };
                for spec in metrics {
                    if gauge.metrics.contains_key(&spec.key) {
                        return Err(OpRefusal::DuplicateMetric {
                            key: spec.key.clone(),
                        });
                    }
                    let metric = Self::metric(spec, None)?;
                    gauge.metrics.insert(spec.key.clone(), metric);
                }
                let number = self
                    .current_regime()
                    .map_or(Some(RegimeNumber::FIRST), |r| r.number.next())
                    .ok_or(OpRefusal::Exhausted)?;
                self.regimes.push(Regime {
                    number,
                    gauge,
                    reason: reason.clone(),
                    bridge: bridge.as_ref().map(|b| b.slug.clone()),
                    at,
                });
                self.gauge_size(log);
                if let Some(bridge) = bridge {
                    let outgoing = outgoing.ok_or(OpRefusal::NoIncumbentBridge)?;
                    let artifacts = self.baselines[&outgoing].artifacts.clone();
                    let readings =
                        self.readings(&bridge.readings, number, &bridge.slug.to_string(), log)?;
                    self.baselines.insert(
                        bridge.slug.clone(),
                        Baseline {
                            regime: number,
                            epoch: None,
                            artifacts,
                            readings,
                            note: bridge.note.clone(),
                            adopts: BTreeSet::new(),

                            disposition: None,
                            void: None,
                            at,
                        },
                    );
                    self.accept_baseline(
                        &bridge.slug,
                        reason.as_deref().ok_or(OpRefusal::MissingRegimeReason)?,
                        at,
                        outgoing.into(),
                        log,
                    )?;
                }
            }
            Op::Gauge {
                contract,
                metrics,
                retire,
                restore,
            } => {
                let regime = self.current_regime().ok_or(OpRefusal::NoGauge)?.number;
                if let Some(contract) = contract {
                    let contract = required(contract, "contract")?;
                    if self.regimes[regime.index()].gauge.contract != contract {
                        log.push(Warning::GaugeEdited { regime });
                    }
                    self.regimes[regime.index()].gauge.contract = contract;
                }
                for spec in metrics.iter().flatten() {
                    let previous = self.regimes[regime.index()].gauge.metrics.get(&spec.key);
                    let metric = Self::metric(spec, previous)?;
                    if previous.is_some_and(|old| {
                        (old.unit != metric.unit || old.better != metric.better)
                            && self.has_reading(regime, &spec.key)
                    }) {
                        return Err(OpRefusal::MeaningChanged {
                            regime,
                            key: spec.key.clone(),
                        });
                    }
                    self.regimes[regime.index()]
                        .gauge
                        .metrics
                        .insert(spec.key.clone(), metric);
                }
                for (keys, retired) in [(retire, true), (restore, false)] {
                    for key in keys.iter().flatten() {
                        self.regimes[regime.index()]
                            .gauge
                            .metrics
                            .get_mut(key)
                            .ok_or_else(|| unknown(RecordKind::Metric, key))?
                            .retired = retired;
                    }
                }
                self.gauge_size(log);
            }
            Op::Baseline {
                slug,
                artifacts,
                readings,
                adopts,
                docket,
                note,
            } => {
                let regime = self
                    .baselines
                    .get(slug)
                    .map(|b| b.regime)
                    .or_else(|| self.current_regime().map(|r| r.number))
                    .ok_or(OpRefusal::NoGauge)?;
                for h in adopts.iter().flatten().chain(docket.iter().flatten()) {
                    self.ensure_hypothesis(h)?;
                }
                let readings = readings
                    .as_ref()
                    .map(|r| self.readings(r, regime, &slug.to_string(), log))
                    .transpose()?;
                let baseline = self
                    .baselines
                    .entry(slug.clone())
                    .or_insert_with(|| Baseline {
                        regime,
                        epoch: None,
                        artifacts: Vec::new(),
                        readings: BTreeMap::new(),
                        note: None,
                        adopts: BTreeSet::new(),

                        disposition: None,
                        void: None,
                        at,
                    });
                if let Some(artifacts) = artifacts {
                    baseline.artifacts.clone_from(artifacts);
                }
                if let Some(note) = note {
                    baseline.note = Some(note.clone());
                }
                if let Some(adopts) = adopts {
                    baseline.adopts = adopts.iter().cloned().collect();
                }
                for (key, reading) in readings.into_iter().flatten() {
                    if baseline
                        .disposition
                        .as_ref()
                        .is_some_and(|d| d.decision == BaselineDecision::Accepted)
                        && baseline
                            .readings
                            .get(&key)
                            .is_some_and(|old| old != &reading)
                    {
                        log.push(Warning::AcceptedBaselineReading {
                            baseline: slug.clone(),
                            key: key.clone(),
                        });
                    }
                    baseline.readings.insert(key, reading);
                }
                if let Some(docket) = docket {
                    self.clear_docket(slug);
                    self.add_to_docket(slug, docket)?;
                }
            }
            Op::Docket {
                baseline,
                add,
                drop,
            } => {
                self.ensure_baseline(baseline)?;
                for h in add.iter().flatten().chain(drop.iter().flatten()) {
                    self.ensure_hypothesis(h)?;
                }
                if let Some(add) = add {
                    self.add_to_docket(baseline, add)?;
                }
                for h in drop.iter().flatten() {
                    let h = self
                        .hypotheses
                        .get_mut(h)
                        .ok_or_else(|| unknown(RecordKind::Hypothesis, h))?;
                    if h.docket.as_ref() == Some(baseline) {
                        h.docket = None;
                    }
                }
            }
            Op::Hypothesis {
                slug,
                title,
                mechanism,
                claim,
                after,
                reviews,
            } => {
                if !self.hypotheses.contains_key(slug) {
                    let title =
                        required(title.as_deref().ok_or_else(|| missing("title"))?, "title")?;
                    self.hypotheses.insert(
                        slug.clone(),
                        Hypothesis {
                            title,
                            mechanism: None,
                            claim: None,
                            docket: None,
                            trials: BTreeMap::new(),
                            last_trial: 0,
                            disposition: None,
                            ownership: empty_ownership(),
                            at,
                        },
                    );
                }
                let h = self
                    .hypotheses
                    .get_mut(slug)
                    .ok_or_else(|| unknown(RecordKind::Hypothesis, slug))?;
                if let Some(title) = title {
                    h.title = required(title, "title")?;
                }
                if let Some(mechanism) = mechanism {
                    h.mechanism = Some(mechanism.clone());
                }
                if let Some(claim) = claim {
                    h.claim = Some(claim.clone());
                }
                for from in after.iter().flatten() {
                    self.add_edge(from, slug, None)?;
                }
                for review in reviews.iter().flatten() {
                    self.reviews
                        .get_mut(review)
                        .ok_or_else(|| unknown(RecordKind::Review, review))?
                        .members
                        .insert(slug.clone());
                }
            }
            Op::Edge { from, to, carries } => self.add_edge(from, to, carries.as_ref())?,
            Op::Unedge { from, to } => {
                self.ensure_hypothesis(from)?;
                self.ensure_hypothesis(to)?;
                self.edges.remove(&(from.clone(), to.clone()));
            }
            Op::Review {
                slug,
                title,
                charter,
                members,
                add,
                drop,
            } => {
                for h in members
                    .iter()
                    .flatten()
                    .chain(add.iter().flatten())
                    .chain(drop.iter().flatten())
                {
                    self.ensure_hypothesis(h)?;
                }
                if !self.reviews.contains_key(slug) {
                    let title =
                        required(title.as_deref().ok_or_else(|| missing("title"))?, "title")?;
                    let charter = required(
                        charter.as_deref().ok_or_else(|| missing("charter"))?,
                        "charter",
                    )?;
                    if members.is_none() {
                        return Err(missing("members"));
                    }
                    self.reviews.insert(
                        slug.clone(),
                        Review {
                            title,
                            charter,
                            members: BTreeSet::new(),
                            lesson: None,
                            ownership: empty_ownership(),
                            at,
                        },
                    );
                }
                let r = self
                    .reviews
                    .get_mut(slug)
                    .ok_or_else(|| unknown(RecordKind::Review, slug))?;
                if let Some(title) = title {
                    r.title = required(title, "title")?;
                }
                if let Some(charter) = charter {
                    r.charter = required(charter, "charter")?;
                }
                if let Some(members) = members {
                    r.members = members.iter().cloned().collect();
                }
                r.members.extend(add.iter().flatten().cloned());
                for h in drop.iter().flatten() {
                    r.members.remove(h);
                }
                if r.members.is_empty() {
                    return Err(OpRefusal::EmptyReview {
                        review: slug.clone(),
                    });
                }
            }
            Op::Trial {
                hypothesis,
                note,
                readings,
                against,
                artifacts,
            } => {
                self.ensure_hypothesis(hypothesis)?;
                let regime = self.current_regime().ok_or(OpRefusal::NoGauge)?.number;
                let note = required(note, "note")?;
                let against = against
                    .clone()
                    .or_else(|| self.incumbent_baseline().cloned());
                if let Some(b) = &against {
                    self.ensure_baseline(b)?;
                }
                let readings = self.readings(
                    readings.as_ref().unwrap_or(&BTreeMap::new()),
                    regime,
                    hypothesis.as_str(),
                    log,
                )?;
                let h = self
                    .hypotheses
                    .get_mut(hypothesis)
                    .ok_or_else(|| unknown(RecordKind::Hypothesis, hypothesis))?;
                if h.disposition.is_some() {
                    log.push(Warning::TrialOnSettled {
                        hypothesis: hypothesis.clone(),
                    });
                }
                h.last_trial = h.last_trial.checked_add(1).ok_or(OpRefusal::Exhausted)?;
                let number = h.last_trial;
                for warning in log.iter_mut() {
                    if let Warning::UnlistedReading { subject, .. } = warning {
                        *subject = format!("{hypothesis}#{number}");
                    }
                }
                if let Some(b) = &against
                    && self.baselines[b].regime != regime
                {
                    log.push(Warning::CrossRegimeTrial {
                        trial: TrialId {
                            hypothesis: hypothesis.clone(),
                            number,
                        },
                        baseline: b.clone(),
                    });
                }
                h.trials.insert(
                    number,
                    Trial {
                        regime,
                        note,
                        readings,
                        against,
                        artifacts: artifacts.clone().unwrap_or_default(),
                        at,
                        void: None,
                    },
                );
            }
            Op::Accept { target, rationale } | Op::Reject { target, rationale } => {
                let rationale = required(rationale, "rationale")?;
                let accept = matches!(op, Op::Accept { .. });
                match target {
                    DecisionTarget::Hypothesis(hypothesis) => {
                        let against = self.incumbent_baseline().cloned();
                        let h = self
                            .hypotheses
                            .get_mut(hypothesis)
                            .ok_or_else(|| unknown(RecordKind::Hypothesis, hypothesis))?;
                        if h.disposition.is_some() {
                            log.push(Warning::ReplacedDisposition {
                                subject: hypothesis.to_string(),
                            });
                        }
                        h.disposition = Some(Disposition {
                            decision: if accept {
                                HypothesisDecision::Accepted { against }
                            } else {
                                HypothesisDecision::Rejected
                            },
                            rationale,
                            at,
                        });
                    }
                    DecisionTarget::Baseline(baseline) if accept => {
                        let outgoing = self.incumbent_baseline().cloned();
                        self.accept_baseline(baseline, &rationale, at, outgoing, log)?;
                    }
                    DecisionTarget::Baseline(baseline) => {
                        self.stranded(baseline, log)?;
                        let b = self
                            .baselines
                            .get_mut(baseline)
                            .ok_or_else(|| unknown(RecordKind::Baseline, baseline))?;
                        if b.disposition.is_some() {
                            log.push(Warning::ReplacedDisposition {
                                subject: baseline.to_string(),
                            });
                        }
                        b.disposition = Some(Disposition {
                            decision: BaselineDecision::Rejected,
                            rationale,
                            at,
                        });
                    }
                }
            }
            Op::Shelve {
                hypothesis,
                rationale,
            } => {
                let rationale = required(rationale, "rationale")?;
                let h = self
                    .hypotheses
                    .get_mut(hypothesis)
                    .ok_or_else(|| unknown(RecordKind::Hypothesis, hypothesis))?;
                if h.disposition.is_some() {
                    log.push(Warning::ReplacedDisposition {
                        subject: hypothesis.to_string(),
                    });
                }
                h.disposition = Some(Disposition {
                    decision: HypothesisDecision::Shelved,
                    rationale,
                    at,
                });
            }
            Op::Reopen { target } => match target {
                WorkTarget::Hypothesis(h) => {
                    self.hypotheses
                        .get_mut(h)
                        .ok_or_else(|| unknown(RecordKind::Hypothesis, h))?
                        .disposition = None;
                }
                WorkTarget::Review(r) => {
                    self.reviews
                        .get_mut(r)
                        .ok_or_else(|| unknown(RecordKind::Review, r))?
                        .lesson = None;
                }
            },
            Op::Void { target, reason } => {
                let reason = required(reason, "reason")?;
                if let VoidTarget::Baseline(b) = target {
                    self.stranded(b, log)?;
                }
                *self.void_mut(target)? = Some(reason);
            }
            Op::Unvoid { target } => *self.void_mut(target)? = None,
            Op::Remove { target } => self.remove(target)?,
            Op::Lesson { review, text } => {
                let lesson = Lesson {
                    text: required(text, "text")?,
                    at,
                };
                self.reviews
                    .get_mut(review)
                    .ok_or_else(|| unknown(RecordKind::Review, review))?
                    .lesson = Some(lesson);
            }
            Op::Waive { finding, note } => {
                let note = required(note, "note")?;
                if !Assay::new(self).findings.contains_key(finding) {
                    return Err(OpRefusal::AbsentFinding {
                        finding: finding.clone(),
                        suggestions: String::new(),
                    });
                }
                self.waivers.insert(finding.clone(), note);
            }
            Op::Assign {
                target,
                owner,
                instructions,
            } => {
                let assignment = Assignment {
                    name: required(owner, "owner")?,
                    instructions: instructions.clone(),
                    at,
                };
                self.ownership_mut(target)?.owner = Some(assignment);
            }
            Op::Release { target } => self.ownership_mut(target)?.owner = None,
            Op::Start { target } => self.ownership_mut(target)?.started = Some(at),
            Op::Stop { target } => self.ownership_mut(target)?.started = None,
            Op::Rename { target, to } => self.rename(target, to)?,
        }
        Ok(())
    }

    fn metric(spec: &MetricSpec, previous: Option<&Metric>) -> Result<Metric, OpRefusal> {
        let metric = Metric {
            unit: spec.unit.clone(),
            better: spec.better.or_else(|| previous.and_then(|m| m.better)),
            kpi: spec.kpi.unwrap_or_else(|| previous.is_some_and(|m| m.kpi)),
            about: spec
                .about
                .clone()
                .or_else(|| previous.and_then(|m| m.about.clone())),
            refs: spec
                .refs
                .clone()
                .unwrap_or_else(|| previous.map_or_else(BTreeMap::new, |m| m.refs.clone())),
            retired: previous.is_some_and(|m| m.retired),
        };
        if metric.kpi && metric.better.is_none() {
            return Err(OpRefusal::KpiDirection {
                key: spec.key.clone(),
            });
        }
        if metric.refs.values().any(|v| !v.is_finite()) {
            return Err(OpRefusal::NonfiniteReference);
        }
        Ok(metric)
    }

    fn gauge_size(&self, log: &mut Vec<Warning>) {
        if let Some(regime) = self.regimes.last() {
            let count = regime.gauge.metrics.values().filter(|m| !m.retired).count();
            if count > 8 {
                log.push(Warning::ManyMetrics {
                    regime: regime.number,
                    count,
                });
            }
        }
    }

    fn has_reading(&self, regime: RegimeNumber, key: &MetricKey) -> bool {
        self.baselines
            .values()
            .any(|b| b.regime == regime && b.readings.contains_key(key))
            || self
                .hypotheses
                .values()
                .flat_map(|h| h.trials.values())
                .any(|t| t.regime == regime && t.readings.contains_key(key))
    }

    fn readings(
        &self,
        input: &Readings,
        regime: RegimeNumber,
        subject: &str,
        log: &mut Vec<Warning>,
    ) -> Result<RecordedReadings, OpRefusal> {
        let gauge = &self
            .regimes
            .get(regime.index())
            .ok_or(OpRefusal::NoGauge)?
            .gauge;
        input
            .iter()
            .map(|(key, value)| {
                let (number, unit) = match value {
                    Value::Number(n) if n.is_finite() => (*n, None),
                    Value::Text(text) => {
                        let parts: Vec<_> = text.split_whitespace().collect();
                        let [number, unit] = parts.as_slice() else {
                            return Err(OpRefusal::InvalidValue {
                                value: value.clone(),
                            });
                        };
                        let number =
                            number.parse::<f64>().map_err(|_| OpRefusal::InvalidValue {
                                value: value.clone(),
                            })?;
                        if !number.is_finite() {
                            return Err(OpRefusal::InvalidValue {
                                value: value.clone(),
                            });
                        }
                        (number, Some(unit.parse::<Unit>()?))
                    }
                    Value::Number(_) => {
                        return Err(OpRefusal::InvalidValue {
                            value: value.clone(),
                        });
                    }
                };
                let reading = if let Some(metric) = gauge.metrics.get(key).filter(|m| !m.retired) {
                    Reading::Listed {
                        value: unit.as_ref().map_or(Ok(number), |u| {
                            u.convert(number, &metric.unit)
                                .map_err(|error| match error {
                                    UnitError::Incompatible { from, to } => {
                                        OpRefusal::ReadingUnit {
                                            key: key.clone(),
                                            from_kind: from.kind(),
                                            to_kind: to.kind(),
                                            from,
                                            to,
                                        }
                                    }
                                    error => OpRefusal::Unit(error),
                                })
                        })?,
                    }
                } else {
                    let closest =
                        slug::nearest(key.as_str(), gauge.metrics.keys().map(MetricKey::as_str))
                            .first()
                            .and_then(|closest| {
                                gauge.metrics.keys().find(|key| key.as_str() == closest)
                            })
                            .cloned();
                    log.push(Warning::UnlistedReading {
                        subject: subject.into(),
                        key: key.clone(),
                        closest,
                    });
                    Reading::Unlisted {
                        value: value.clone(),
                    }
                };
                Ok((key.clone(), reading))
            })
            .collect()
    }

    fn ensure_hypothesis(&self, slug: &Slug) -> Result<(), OpRefusal> {
        if self.hypotheses.contains_key(slug) {
            Ok(())
        } else {
            Err(unknown(RecordKind::Hypothesis, slug))
        }
    }
    fn ensure_baseline(&self, slug: &Slug) -> Result<(), OpRefusal> {
        if self.baselines.contains_key(slug) {
            Ok(())
        } else {
            Err(unknown(RecordKind::Baseline, slug))
        }
    }
    fn add_edge(
        &mut self,
        from: &Slug,
        to: &Slug,
        carries: Option<&String>,
    ) -> Result<(), OpRefusal> {
        self.ensure_hypothesis(from)?;
        self.ensure_hypothesis(to)?;
        if let Some(path) = closes_cycle(&self.edges, from, to) {
            return Err(OpRefusal::Cycle { path });
        }
        let edge = self
            .edges
            .entry((from.clone(), to.clone()))
            .or_insert(Edge { carries: None });
        if let Some(carries) = carries {
            edge.carries = Some(carries.clone());
        }
        Ok(())
    }
    fn add_to_docket(&mut self, baseline: &Slug, hypotheses: &[Slug]) -> Result<(), OpRefusal> {
        self.ensure_baseline(baseline)?;
        for h in hypotheses {
            self.ensure_hypothesis(h)?;
        }
        for h in hypotheses {
            self.hypotheses
                .get_mut(h)
                .ok_or_else(|| unknown(RecordKind::Hypothesis, h))?
                .docket = Some(baseline.clone());
        }
        Ok(())
    }
    fn clear_docket(&mut self, baseline: &Slug) {
        for h in self.hypotheses.values_mut() {
            if h.docket.as_ref() == Some(baseline) {
                h.docket = None;
            }
        }
    }
    fn unsettled_docket(&self, baseline: &Slug) -> Vec<Slug> {
        self.hypotheses
            .iter()
            .filter(|(_, h)| h.docket.as_ref() == Some(baseline) && h.disposition.is_none())
            .map(|(slug, _)| slug.clone())
            .collect()
    }
    fn accept_baseline(
        &mut self,
        baseline: &Slug,
        rationale: &str,
        at: OffsetDateTime,
        outgoing: Option<Slug>,
        log: &mut Vec<Warning>,
    ) -> Result<(), OpRefusal> {
        let current = self.current_regime().map(|r| r.number);
        let b = self
            .baselines
            .get_mut(baseline)
            .ok_or_else(|| unknown(RecordKind::Baseline, baseline))?;
        if b.void.is_some() {
            return Err(OpRefusal::VoidBaseline {
                baseline: baseline.clone(),
            });
        }
        if Some(b.regime) != current {
            return Err(OpRefusal::OldBaseline {
                baseline: baseline.clone(),
            });
        }
        if b.disposition.is_some() {
            log.push(Warning::ReplacedDisposition {
                subject: baseline.to_string(),
            });
        }
        let epoch = self
            .last_epoch
            .map_or(Some(EpochNumber::FIRST), EpochNumber::next)
            .ok_or(OpRefusal::Exhausted)?;
        self.last_epoch = Some(epoch);
        b.epoch = Some(epoch);
        b.disposition = Some(Disposition {
            decision: BaselineDecision::Accepted,
            rationale: rationale.into(),
            at,
        });
        self.epochs.push(Epoch {
            number: epoch,
            regime: b.regime,
            baseline: baseline.clone(),
            at,
        });
        if let Some(from) = outgoing.filter(|b| b != baseline) {
            let hypotheses = self.unsettled_docket(&from);
            if !hypotheses.is_empty() {
                self.add_to_docket(baseline, &hypotheses)?;
                log.push(Warning::DocketMoved {
                    from,
                    to: baseline.clone(),
                    hypotheses,
                });
            }
        }
        Ok(())
    }
    fn stranded(&self, baseline: &Slug, log: &mut Vec<Warning>) -> Result<(), OpRefusal> {
        self.ensure_baseline(baseline)?;
        let hypotheses = self.unsettled_docket(baseline);
        if !hypotheses.is_empty() {
            log.push(Warning::DocketStranded {
                baseline: baseline.clone(),
                hypotheses,
            });
        }
        Ok(())
    }
    fn void_mut(&mut self, target: &VoidTarget) -> Result<&mut Option<String>, OpRefusal> {
        match target {
            VoidTarget::Baseline(b) => Ok(&mut self
                .baselines
                .get_mut(b)
                .ok_or_else(|| unknown(RecordKind::Baseline, b))?
                .void),
            VoidTarget::Trial(id) => Ok(&mut self
                .hypotheses
                .get_mut(&id.hypothesis)
                .and_then(|h| h.trials.get_mut(&id.number))
                .ok_or_else(|| unknown(RecordKind::Trial, id))?
                .void),
        }
    }
    fn ownership_mut(&mut self, target: &WorkTarget) -> Result<&mut Ownership, OpRefusal> {
        match target {
            WorkTarget::Hypothesis(h) => Ok(&mut self
                .hypotheses
                .get_mut(h)
                .ok_or_else(|| unknown(RecordKind::Hypothesis, h))?
                .ownership),
            WorkTarget::Review(r) => Ok(&mut self
                .reviews
                .get_mut(r)
                .ok_or_else(|| unknown(RecordKind::Review, r))?
                .ownership),
        }
    }

    fn remove(&mut self, target: &RemoveTarget) -> Result<(), OpRefusal> {
        match target {
            RemoveTarget::Hypothesis(h) => {
                self.ensure_hypothesis(h)?;
                for (slug, r) in &self.reviews {
                    if r.members.len() == 1 && r.members.contains(h) {
                        return Err(OpRefusal::LastMember {
                            hypothesis: h.clone(),
                            review: slug.clone(),
                        });
                    }
                }
                self.hypotheses.remove(h);
                self.edges.retain(|(from, to), _| from != h && to != h);
                for b in self.baselines.values_mut() {
                    b.adopts.remove(h);
                }
                for r in self.reviews.values_mut() {
                    r.members.remove(h);
                }
            }
            RemoveTarget::Review(r) => {
                self.reviews
                    .remove(r)
                    .ok_or_else(|| unknown(RecordKind::Review, r))?;
            }
            RemoveTarget::Baseline(b) => {
                self.baselines
                    .remove(b)
                    .ok_or_else(|| unknown(RecordKind::Baseline, b))?;
                self.clear_docket(b);
                self.epochs.retain(|e| &e.baseline != b);
                for regime in &mut self.regimes {
                    if regime.bridge.as_ref() == Some(b) {
                        regime.bridge = None;
                    }
                }
                for h in self.hypotheses.values_mut() {
                    if let Some(Disposition {
                        decision: HypothesisDecision::Accepted { against },
                        ..
                    }) = &mut h.disposition
                        && against.as_ref() == Some(b)
                    {
                        *against = None;
                    }
                    for t in h.trials.values_mut() {
                        if t.against.as_ref() == Some(b) {
                            t.against = None;
                        }
                    }
                }
            }
            RemoveTarget::Trial(id) => {
                self.hypotheses
                    .get_mut(&id.hypothesis)
                    .and_then(|h| h.trials.remove(&id.number))
                    .ok_or_else(|| unknown(RecordKind::Trial, id))?;
            }
        }
        self.waivers
            .retain(|finding, _| !finding.has_subject(target));
        Ok(())
    }

    fn rename(&mut self, target: &RenameTarget, to: &Slug) -> Result<(), OpRefusal> {
        match target {
            RenameTarget::Hypothesis(from) => {
                self.ensure_hypothesis(from)?;
                if from == to {
                    return Ok(());
                }
                if self.hypotheses.contains_key(to) {
                    return Err(OpRefusal::RenameCollision {
                        kind: RecordKind::Hypothesis,
                        to: to.clone(),
                    });
                }
                let h = self
                    .hypotheses
                    .remove(from)
                    .ok_or_else(|| unknown(RecordKind::Hypothesis, from))?;
                self.hypotheses.insert(to.clone(), h);
                let rename = |slug: Slug| if &slug == from { to.clone() } else { slug };
                self.edges = std::mem::take(&mut self.edges)
                    .into_iter()
                    .map(|((a, b), e)| ((rename(a), rename(b)), e))
                    .collect();
                for b in self.baselines.values_mut() {
                    rename_member(&mut b.adopts, from, to);
                }
                for r in self.reviews.values_mut() {
                    rename_member(&mut r.members, from, to);
                }
            }
            RenameTarget::Review(from) => {
                if !self.reviews.contains_key(from) {
                    return Err(unknown(RecordKind::Review, from));
                }
                if from == to {
                    return Ok(());
                }
                if self.reviews.contains_key(to) {
                    return Err(OpRefusal::RenameCollision {
                        kind: RecordKind::Review,
                        to: to.clone(),
                    });
                }
                let r = self
                    .reviews
                    .remove(from)
                    .ok_or_else(|| unknown(RecordKind::Review, from))?;
                self.reviews.insert(to.clone(), r);
            }
            RenameTarget::Baseline(from) => {
                self.ensure_baseline(from)?;
                if from == to {
                    return Ok(());
                }
                if self.baselines.contains_key(to) {
                    return Err(OpRefusal::RenameCollision {
                        kind: RecordKind::Baseline,
                        to: to.clone(),
                    });
                }
                let b = self
                    .baselines
                    .remove(from)
                    .ok_or_else(|| unknown(RecordKind::Baseline, from))?;
                self.baselines.insert(to.clone(), b);
                for e in &mut self.epochs {
                    if &e.baseline == from {
                        e.baseline = to.clone();
                    }
                }
                for r in &mut self.regimes {
                    if r.bridge.as_ref() == Some(from) {
                        r.bridge = Some(to.clone());
                    }
                }
                for h in self.hypotheses.values_mut() {
                    if h.docket.as_ref() == Some(from) {
                        h.docket = Some(to.clone());
                    }
                    for t in h.trials.values_mut() {
                        if t.against.as_ref() == Some(from) {
                            t.against = Some(to.clone());
                        }
                    }
                    if let Some(Disposition {
                        decision: HypothesisDecision::Accepted { against },
                        ..
                    }) = &mut h.disposition
                        && against.as_ref() == Some(from)
                    {
                        *against = Some(to.clone());
                    }
                }
            }
        }
        self.waivers = std::mem::take(&mut self.waivers)
            .into_iter()
            .map(|(finding, note)| (finding.renamed(target, to), note))
            .collect();
        Ok(())
    }
}

fn rename_member(set: &mut BTreeSet<Slug>, from: &Slug, to: &Slug) {
    if set.remove(from) {
        set.insert(to.clone());
    }
}
