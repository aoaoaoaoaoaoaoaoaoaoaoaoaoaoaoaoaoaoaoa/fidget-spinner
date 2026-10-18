use crate::{
    BaselineDecision, Direction, Epoch, FindingId, Frontier, HypothesisDecision, MetricKey,
    Reading, RegimeNumber, Slug, TrialId,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, serde::Deserialize)]
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
    pub members: BTreeSet<Slug>,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Assay {
    pub hypotheses: BTreeMap<Slug, State>,
    pub reviews: BTreeMap<Slug, ReviewState>,
    pub incumbent_baseline: Option<Slug>,
    pub epochs: Vec<Epoch>,
    pub dockets: BTreeMap<Slug, DocketProgress>,
    pub comparisons: BTreeMap<TrialId, BTreeMap<MetricKey, Comparison>>,
    pub findings: BTreeMap<FindingId, Option<String>>,
    pub counts: Counts,
}

impl Assay {
    pub fn new(frontier: &Frontier) -> Self {
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
                    }) || h.docket.as_ref().is_some_and(|b| {
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
        let mut dockets: BTreeMap<_, _> = frontier
            .baselines
            .keys()
            .map(|slug| {
                (
                    slug.clone(),
                    DocketProgress {
                        members: BTreeSet::new(),
                        settled: 0,
                    },
                )
            })
            .collect();
        for (slug, h) in &frontier.hypotheses {
            if let Some(baseline) = &h.docket {
                let docket = dockets.get_mut(baseline).unwrap_or_else(|| {
                    unreachable!("hypothesis docket references an existing baseline")
                });
                docket.members.insert(slug.clone());
                docket.settled += usize::from(hypotheses[slug] == State::Settled);
            }
        }
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
                let gauge = &frontier.regimes[trial.regime.index()].gauge;
                let mut comparisons = BTreeMap::new();
                for (key, reading) in &trial.readings {
                    let Some(metric) = gauge.metrics.get(key).filter(|m| !m.retired) else {
                        continue;
                    };
                    let (Reading::Listed { value: t }, Some(Reading::Listed { value: b })) =
                        (reading, b.readings.get(key))
                    else {
                        continue;
                    };
                    let (t, b) = (*t, *b);
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
                    comparisons.insert(
                        key.clone(),
                        Comparison {
                            trial: t,
                            baseline: b,
                            ratio: ratio.is_finite().then_some(ratio),
                            movement,
                        },
                    );
                }
                self.comparisons.insert(
                    TrialId {
                        hypothesis: slug.clone(),
                        number: *number,
                    },
                    comparisons,
                );
            }
        }
    }

    fn finding(&mut self, frontier: &Frontier, identity: FindingId) {
        let waiver = frontier.waivers.get(&identity).cloned();
        self.findings.insert(identity, waiver);
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
                self.finding(frontier, FindingId::Unbacked(slug.clone()));
            }
        }
        for regime in &frontier.regimes {
            if regime.number != RegimeNumber::FIRST && regime.bridge.is_none() {
                self.finding(frontier, FindingId::Unbridged(regime.number));
            }
        }
        let mut unlisted: BTreeMap<(RegimeNumber, MetricKey), usize> = BTreeMap::new();
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
                self.finding(frontier, FindingId::Unlisted(regime, key));
            }
        }
        if let Some(slug) = frontier.incumbent_baseline() {
            let baseline = &frontier.baselines[slug];
            for (key, metric) in &frontier.regimes[baseline.regime.index()].gauge.metrics {
                if metric.kpi
                    && !metric.retired
                    && !matches!(baseline.readings.get(key), Some(Reading::Listed { .. }))
                {
                    self.finding(frontier, FindingId::Unread(slug.clone(), key.clone()));
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
                        self.finding(frontier, FindingId::Unaccepted(slug.clone(), h.clone()));
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
