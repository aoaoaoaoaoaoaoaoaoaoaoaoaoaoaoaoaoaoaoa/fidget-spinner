use crate::{
    Direction, EpochNumber, FindingId, MetricKey, Ownership, RegimeNumber, Slug, Unit, Value,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use time::OffsetDateTime;

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Frontier {
    pub(crate) objective: Option<String>,
    pub(crate) regimes: Vec<Regime>,
    pub(crate) baselines: BTreeMap<Slug, Baseline>,
    pub(crate) hypotheses: BTreeMap<Slug, Hypothesis>,
    pub(crate) reviews: BTreeMap<Slug, Review>,
    #[serde(serialize_with = "serialize_edges")]
    pub(crate) edges: BTreeMap<(Slug, Slug), Edge>,
    pub(crate) waivers: BTreeMap<FindingId, String>,
    pub(crate) epochs: Vec<Epoch>,
    pub(crate) last_epoch: Option<EpochNumber>,
    pub(crate) conclusion: Option<Conclusion>,
}

impl Frontier {
    pub fn objective(&self) -> Option<&str> {
        self.objective.as_deref()
    }
    pub fn regimes(&self) -> &[Regime] {
        &self.regimes
    }
    pub fn current_regime(&self) -> Option<&Regime> {
        self.regimes.last()
    }
    pub fn baselines(&self) -> &BTreeMap<Slug, Baseline> {
        &self.baselines
    }
    pub fn hypotheses(&self) -> &BTreeMap<Slug, Hypothesis> {
        &self.hypotheses
    }
    pub fn reviews(&self) -> &BTreeMap<Slug, Review> {
        &self.reviews
    }
    pub fn edges(&self) -> &BTreeMap<(Slug, Slug), Edge> {
        &self.edges
    }
    pub fn waivers(&self) -> &BTreeMap<FindingId, String> {
        &self.waivers
    }
    pub fn epochs(&self) -> &[Epoch] {
        &self.epochs
    }
    pub fn conclusion(&self) -> Option<&Conclusion> {
        self.conclusion.as_ref()
    }
    pub fn incumbent_baseline(&self) -> Option<&Slug> {
        self.baselines
            .iter()
            .filter(|(_, b)| {
                self.current_regime()
                    .is_some_and(|regime| b.regime == regime.number)
                    && b.void.is_none()
                    && b.disposition
                        .as_ref()
                        .is_some_and(|d| d.decision == BaselineDecision::Accepted)
            })
            .max_by_key(|(_, b)| b.epoch)
            .map(|(slug, _)| slug)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Regime {
    pub number: RegimeNumber,
    pub gauge: Gauge,
    pub reason: Option<String>,
    pub bridge: Option<Slug>,
    pub at: OffsetDateTime,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Gauge {
    pub contract: String,
    pub metrics: BTreeMap<MetricKey, Metric>,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Metric {
    pub unit: Unit,
    pub better: Option<Direction>,
    pub kpi: bool,
    pub about: Option<String>,
    pub refs: BTreeMap<String, f64>,
    pub retired: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Reading {
    Listed { value: f64 },
    Unlisted { value: Value },
}
pub type RecordedReadings = BTreeMap<MetricKey, Reading>;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Baseline {
    pub regime: RegimeNumber,
    pub epoch: Option<EpochNumber>,
    pub artifacts: Vec<String>,
    pub readings: RecordedReadings,
    pub note: Option<String>,
    pub adopts: BTreeSet<Slug>,
    pub disposition: Option<Disposition<BaselineDecision>>,
    pub void: Option<String>,
    pub at: OffsetDateTime,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Hypothesis {
    pub title: String,
    pub mechanism: Option<String>,
    pub claim: Option<String>,
    pub docket: Option<Slug>,
    pub trials: BTreeMap<u64, Trial>,
    pub(crate) last_trial: u64,
    pub disposition: Option<Disposition<HypothesisDecision>>,
    pub ownership: Ownership,
    pub at: OffsetDateTime,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Trial {
    pub regime: RegimeNumber,
    pub note: String,
    pub readings: RecordedReadings,
    pub against: Option<Slug>,
    pub artifacts: Vec<String>,
    pub at: OffsetDateTime,
    pub void: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Review {
    pub title: String,
    pub charter: String,
    pub members: BTreeSet<Slug>,
    pub lesson: Option<Lesson>,
    pub ownership: Ownership,
    pub at: OffsetDateTime,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Lesson {
    pub text: String,
    pub at: OffsetDateTime,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Conclusion {
    pub lesson: Lesson,
    pub archived: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Disposition<D> {
    pub decision: D,
    pub rationale: String,
    pub at: OffsetDateTime,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BaselineDecision {
    Accepted,
    Rejected,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum HypothesisDecision {
    Accepted { against: Option<Slug> },
    Rejected,
    Shelved,
}
impl HypothesisDecision {
    pub const fn accepted(&self) -> bool {
        matches!(self, Self::Accepted { .. })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Edge {
    pub carries: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Epoch {
    pub number: EpochNumber,
    pub regime: RegimeNumber,
    pub baseline: Slug,
    pub at: OffsetDateTime,
}

fn serialize_edges<S: serde::Serializer>(
    edges: &BTreeMap<(Slug, Slug), Edge>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    #[derive(Serialize)]
    struct Row<'a> {
        from: &'a Slug,
        to: &'a Slug,
        #[serde(flatten)]
        edge: &'a Edge,
    }
    edges
        .iter()
        .map(|((from, to), edge)| Row { from, to, edge })
        .collect::<Vec<_>>()
        .serialize(serializer)
}
