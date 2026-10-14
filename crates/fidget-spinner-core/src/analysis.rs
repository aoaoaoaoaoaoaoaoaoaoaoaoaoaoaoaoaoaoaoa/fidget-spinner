use crate::{
    BaselineDecision, Direction, Epoch, Frontier, HypothesisDecision, MetricKey, Reading, Slug,
    TrialId,
};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Waiting,
    Ready,
    Running,
    Tested,
    Settled,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReviewState {
    Waiting,
    Ready,
    Running,
    Completed,
}
pub type Counts = BTreeMap<State, usize>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DocketProgress {
    pub members: usize,
    pub settled: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Movement {
    Better,
    Equal,
    Worse,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Comparison {
    pub trial: f64,
    pub baseline: f64,
    pub ratio: Option<f64>,
    pub movement: Option<Movement>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum FindingKind {
    Unbacked { hypothesis: Slug },
    Unbridged { regime: usize },
    Unlisted { regime: usize, key: MetricKey },
    Unread { baseline: Slug, key: MetricKey },
    Unaccepted { baseline: Slug, hypothesis: Slug },
}
impl FindingKind {
    #[must_use]
    pub fn identity(&self) -> String {
        match self {
            Self::Unbacked { hypothesis } => format!("unbacked/{hypothesis}"),
            Self::Unbridged { regime } => format!("unbridged/{regime}"),
            Self::Unlisted { regime, key } => format!("unlisted/{regime}/{key}"),
            Self::Unread { baseline, key } => format!("unread/{baseline}/{key}"),
            Self::Unaccepted {
                baseline,
                hypothesis,
            } => format!("unaccepted/{baseline}/{hypothesis}"),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Finding {
    pub kind: FindingKind,
    pub waiver: Option<String>,
}
impl Finding {
    #[must_use]
    pub fn identity(&self) -> String {
        self.kind.identity()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Assay {
    pub hypotheses: BTreeMap<Slug, State>,
    pub reviews: BTreeMap<Slug, ReviewState>,
    pub incumbent_baseline: Option<Slug>,
    pub epochs: Vec<Epoch>,
    pub docket_membership: BTreeMap<Slug, Slug>,
    pub dockets: BTreeMap<Slug, DocketProgress>,
    pub comparisons: BTreeMap<TrialId, BTreeMap<MetricKey, Comparison>>,
    pub findings: BTreeMap<String, Finding>,
    pub counts: Counts,
}

impl Assay {
    #[must_use]
    pub fn new(frontier: &Frontier) -> Self {
        let docket_membership: BTreeMap<_, _> = frontier
            .baselines
            .iter()
            .flat_map(|(slug, b)| b.docket.iter().map(move |h| (h.clone(), slug.clone())))
            .collect();
        let hypotheses: BTreeMap<_, _> = frontier
            .hypotheses
            .iter()
            .map(|(slug, h)| {
                let state =
                    if h.disposition.is_some() {
                        State::Settled
                    } else if h.ownership.started.is_some() {
                        State::Running
                    } else if h.trials.values().any(|t| t.void.is_none()) {
                        State::Tested
                    } else if frontier.edges.keys().any(|(from, to)| {
                        to == slug && frontier.hypotheses[from].disposition.is_none()
                    }) || docket_membership.get(slug).is_some_and(|b| {
                        frontier.baselines[b]
                            .disposition
                            .as_ref()
                            .is_none_or(|d| d.decision != BaselineDecision::Accepted)
                    }) {
                        State::Waiting
                    } else {
                        State::Ready
                    };
                (slug.clone(), state)
            })
            .collect();
        let reviews = frontier
            .reviews
            .iter()
            .map(|(slug, r)| {
                let state = if r.lesson.is_some() {
                    ReviewState::Completed
                } else if r.ownership.started.is_some() {
                    ReviewState::Running
                } else if r
                    .members
                    .iter()
                    .all(|h| matches!(hypotheses[h], State::Tested | State::Settled))
                {
                    ReviewState::Ready
                } else {
                    ReviewState::Waiting
                };
                (slug.clone(), state)
            })
            .collect();
        let dockets = frontier
            .baselines
            .iter()
            .map(|(slug, b)| {
                (
                    slug.clone(),
                    DocketProgress {
                        members: b.docket.len(),
                        settled: b
                            .docket
                            .iter()
                            .filter(|h| hypotheses[*h] == State::Settled)
                            .count(),
                    },
                )
            })
            .collect();
        let mut counts: Counts = [
            State::Waiting,
            State::Ready,
            State::Running,
            State::Tested,
            State::Settled,
        ]
        .into_iter()
        .map(|state| (state, 0))
        .collect();
        for state in hypotheses.values() {
            *counts.entry(*state).or_default() += 1;
        }
        let mut assay = Self {
            hypotheses,
            reviews,
            counts,
            dockets,
            docket_membership,
            incumbent_baseline: frontier.incumbent_baseline().cloned(),
            epochs: frontier.epochs.clone(),
            comparisons: BTreeMap::new(),
            findings: BTreeMap::new(),
        };
        assay.measure(frontier);
        assay.find(frontier);
        assay
    }

    fn measure(&mut self, frontier: &Frontier) {
        for (slug, h) in &frontier.hypotheses {
            for (number, trial) in &h.trials {
                if trial.void.is_some() {
                    continue;
                }
                let Some(b) = trial
                    .against
                    .as_ref()
                    .and_then(|b| frontier.baselines.get(b))
                else {
                    continue;
                };
                if b.void.is_some() || b.regime != trial.regime {
                    continue;
                }
                let gauge = &frontier.regimes[trial.regime - 1].gauge;
                let mut comparisons = BTreeMap::new();
                for (key, reading) in &trial.readings {
                    let Some(metric) = gauge.metrics.get(key).filter(|m| !m.retired) else {
                        continue;
                    };
                    let (
                        Reading::Listed { value: t, unit: tu },
                        Some(Reading::Listed { value: b, unit: bu }),
                    ) = (reading, b.readings.get(key))
                    else {
                        continue;
                    };
                    let (Ok(t), Ok(b)) =
                        (tu.convert(*t, &metric.unit), bu.convert(*b, &metric.unit))
                    else {
                        continue;
                    };
                    let ratio = t / b;
                    let movement = metric.better.map(|direction| {
                        if matches!(t.partial_cmp(&b), Some(std::cmp::Ordering::Equal)) {
                            Movement::Equal
                        } else if (t < b) == (direction == Direction::Lower) {
                            Movement::Better
                        } else {
                            Movement::Worse
                        }
                    });
                    let _ = comparisons.insert(
                        key.clone(),
                        Comparison {
                            trial: t,
                            baseline: b,
                            ratio: ratio.is_finite().then_some(ratio),
                            movement,
                        },
                    );
                }
                let _ = self.comparisons.insert(
                    TrialId {
                        hypothesis: slug.clone(),
                        number: *number,
                    },
                    comparisons,
                );
            }
        }
    }

    fn finding(&mut self, frontier: &Frontier, kind: FindingKind) {
        let identity = kind.identity();
        let waiver = frontier.waivers.get(&identity).cloned();
        let _ = self.findings.insert(identity, Finding { kind, waiver });
    }

    fn find(&mut self, frontier: &Frontier) {
        for (slug, h) in &frontier.hypotheses {
            if let Some(crate::Disposition {
                decision: HypothesisDecision::Accepted { against },
                ..
            }) = &h.disposition
                && !h.trials.values().any(|t| {
                    t.void.is_none()
                        && against
                            .as_ref()
                            .is_none_or(|b| t.against.as_ref() == Some(b))
                })
            {
                self.finding(
                    frontier,
                    FindingKind::Unbacked {
                        hypothesis: slug.clone(),
                    },
                );
            }
        }
        for regime in &frontier.regimes {
            if regime.number >= 2 && regime.bridge.is_none() {
                self.finding(
                    frontier,
                    FindingKind::Unbridged {
                        regime: regime.number,
                    },
                );
            }
        }
        let mut unlisted: BTreeMap<(usize, MetricKey), usize> = BTreeMap::new();
        for trial in frontier
            .hypotheses
            .values()
            .flat_map(|h| h.trials.values())
            .filter(|t| t.void.is_none())
        {
            for (key, reading) in &trial.readings {
                if matches!(reading, Reading::Unlisted { .. }) {
                    *unlisted.entry((trial.regime, key.clone())).or_default() += 1;
                }
            }
        }
        for ((regime, key), count) in unlisted {
            if count >= 3 {
                self.finding(frontier, FindingKind::Unlisted { regime, key });
            }
        }
        if let Some(slug) = frontier.incumbent_baseline() {
            let baseline = &frontier.baselines[slug];
            for (key, metric) in &frontier.regimes[baseline.regime - 1].gauge.metrics {
                if metric.kpi
                    && !metric.retired
                    && !matches!(baseline.readings.get(key), Some(Reading::Listed { .. }))
                {
                    self.finding(
                        frontier,
                        FindingKind::Unread {
                            baseline: slug.clone(),
                            key: key.clone(),
                        },
                    );
                }
            }
        }
        for (slug, b) in &frontier.baselines {
            if b.void.is_none()
                && b.disposition
                    .as_ref()
                    .is_some_and(|d| d.decision == BaselineDecision::Accepted)
            {
                for h in &b.adopts {
                    if frontier.hypotheses[h]
                        .disposition
                        .as_ref()
                        .is_none_or(|d| !d.decision.accepted())
                    {
                        self.finding(
                            frontier,
                            FindingKind::Unaccepted {
                                baseline: slug.clone(),
                                hypothesis: h.clone(),
                            },
                        );
                    }
                }
            }
        }
    }
}

impl From<&Frontier> for Assay {
    fn from(frontier: &Frontier) -> Self {
        Self::new(frontier)
    }
}
