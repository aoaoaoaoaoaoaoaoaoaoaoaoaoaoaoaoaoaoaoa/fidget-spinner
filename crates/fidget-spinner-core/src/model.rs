use crate::{
    Direction, Id, MetricKey, Moment, Name, Pos, ReadingValue, RegimeNumber, Seq, Slug, Unit,
    data::{Data, Ref},
    schema::{Carrier, Schema},
};
use std::collections::{BTreeMap, BTreeSet};

/// A frontier's state: the fold of its patches.
#[derive(Clone, Debug, Default)]
pub struct Frontier {
    pub(crate) objective: Option<String>,
    pub(crate) head: Seq,
    pub(crate) legacy_through: Option<Seq>,
    pub(crate) next: u32,
    pub(crate) regimes: Vec<Regime>,
    pub(crate) schemas: BTreeMap<Slug, Vec<Definition>>,
    pub(crate) work: BTreeMap<Id, Work>,
    pub(crate) baselines: BTreeMap<Id, Baseline>,
    pub(crate) names: BTreeMap<Name, Bound>,
    pub(crate) baseline_names: BTreeMap<Slug, Bound>,
    pub(crate) counters: BTreeMap<Id, u64>,
    pub(crate) elections: Vec<Election>,
    pub(crate) incumbency: Option<Pos>,
    pub(crate) conclusion: Option<Conclusion>,
    pub(crate) archived: bool,
    pub(crate) removed: BTreeMap<Id, Removed>,
}

/// A name's current incarnation and the position that bound it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bound {
    pub id: Id,
    pub since: Pos,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Regime {
    pub number: RegimeNumber,
    pub gauge: Gauge,
    pub reason: Option<String>,
    pub bridge: Option<Id>,
    pub at: Moment,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Gauge {
    pub contract: String,
    pub metrics: BTreeMap<MetricKey, Metric>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Metric {
    pub unit: Unit,
    pub better: Option<Direction>,
    pub kpi: bool,
    pub about: Option<String>,
    pub refs: BTreeMap<String, f64>,
    pub retired: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Reading {
    Listed(f64),
    Unlisted(ReadingValue),
}
pub type RecordedReadings = BTreeMap<MetricKey, Reading>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Definition {
    pub at: Moment,
    pub schema: Schema,
}

/// A work record's schema binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Binding {
    Exact { id: Slug, at: Pos },
    Requested { id: Slug, rev: Option<Pos> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Work {
    pub name: Name,
    pub created: Moment,
    pub rev: Pos,
    /// The last change to what the work asks: its data or plan, binding or inputs.
    pub specified: Pos,
    pub binding: Option<Binding>,
    pub inputs: BTreeSet<Id>,
    pub owner: Option<Owner>,
    pub running: Option<Moment>,
    pub void: Option<Void>,
    pub annotations: Vec<Annotation>,
    pub body: Body,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Body {
    Trial(Trial),
    Iset(Iset),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Trial {
    pub regime: Option<RegimeNumber>,
    pub plans: Vec<Plan>,
    pub report: Option<Report>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub at: Moment,
    pub data: Data,
    pub control: Option<Control>,
    pub treatment: Option<Treatment>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Control {
    pub baseline: Id,
    pub defaulted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Treatment {
    Baseline(Id),
    Identity(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Report {
    pub at: Moment,
    pub plan: Option<usize>,
    pub control: Option<Control>,
    pub treatment: Option<Treatment>,
    pub readings: RecordedReadings,
    pub control_readings: Option<RecordedReadings>,
    pub artifacts: Vec<String>,
    pub data: Data,
    pub consumes: BTreeSet<Id>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Iset {
    pub data: Data,
    pub members: BTreeSet<Id>,
    pub judgment: Option<Judgment>,
    /// FS 3 judgments replaced by a later judgment or withdrawn by reopen.
    pub prior: Vec<Judgment>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Judgment {
    pub at: Moment,
    pub answer: Data,
    pub seen: Option<Seq>,
    pub consumes: BTreeSet<Id>,
    pub passed: BTreeSet<Id>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Owner {
    pub name: String,
    pub instructions: Option<String>,
    /// The work's specification revision when it was assigned.
    pub bound: Pos,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Void {
    pub at: Moment,
    pub reason: String,
}

/// A dated remark appended by annotate; it changes nothing the record says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Annotation {
    pub at: Moment,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Baseline {
    pub slug: Slug,
    pub created: Moment,
    pub rev: Pos,
    pub regime: Option<RegimeNumber>,
    pub artifacts: Vec<String>,
    pub readings: RecordedReadings,
    pub note: Option<String>,
    pub identity_frozen: Option<Pos>,
    pub readings_frozen: Option<Pos>,
    pub void: Option<Void>,
    pub annotations: Vec<Annotation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Election {
    pub at: Moment,
    pub baseline: Id,
    pub regime: Option<RegimeNumber>,
    pub rationale: String,
    pub adopts: BTreeMap<Id, Option<Pos>>,
    pub consumes: BTreeSet<Id>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conclusion {
    pub at: Moment,
    pub lesson: String,
    pub consumes: BTreeSet<Id>,
    pub open: BTreeSet<Id>,
}

/// The kind of a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    Trial,
    Iset,
    Baseline,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Removed {
    pub kind: Kind,
    pub at: Pos,
}

impl Kind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trial => "trial",
            Self::Iset => "iset",
            Self::Baseline => "baseline",
        }
    }
}

impl Work {
    pub const fn carrier(&self) -> Carrier {
        match self.body {
            Body::Trial(_) => Carrier::Trial,
            Body::Iset(_) => Carrier::Iset,
        }
    }
    pub const fn kind(&self) -> Kind {
        match self.body {
            Body::Trial(_) => Kind::Trial,
            Body::Iset(_) => Kind::Iset,
        }
    }
    pub const fn trial(&self) -> Option<&Trial> {
        match &self.body {
            Body::Trial(trial) => Some(trial),
            Body::Iset(_) => None,
        }
    }
    pub const fn iset(&self) -> Option<&Iset> {
        match &self.body {
            Body::Iset(iset) => Some(iset),
            Body::Trial(_) => None,
        }
    }
    /// Whether the record resolved: a trial reported or an iset judged.
    pub const fn resolved(&self) -> bool {
        match &self.body {
            Body::Trial(trial) => trial.report.is_some(),
            Body::Iset(iset) => iset.judgment.is_some(),
        }
    }
    /// Open, non-void work whose specification changed after its assignment was bound.
    pub fn stale(&self) -> bool {
        self.void.is_none()
            && !self.resolved()
            && self
                .owner
                .as_ref()
                .is_some_and(|owner| owner.bound < self.specified)
    }
    /// The data a view headlines: an iset's data or a trial's current plan.
    pub fn data(&self) -> Option<&Data> {
        match &self.body {
            Body::Iset(iset) => Some(&iset.data),
            Body::Trial(trial) => trial.plans.last().map(|plan| &plan.data),
        }
    }
}

impl Trial {
    pub fn plan(&self) -> Option<&Plan> {
        self.plans.last()
    }
}

impl Frontier {
    pub fn objective(&self) -> Option<&str> {
        self.objective.as_deref()
    }
    pub const fn head(&self) -> Seq {
        self.head
    }
    pub const fn legacy_through(&self) -> Option<Seq> {
        self.legacy_through
    }
    pub fn regimes(&self) -> &[Regime] {
        &self.regimes
    }
    pub fn current_regime(&self) -> Option<&Regime> {
        self.regimes.last()
    }
    pub fn regime(&self, number: RegimeNumber) -> Option<&Regime> {
        self.regimes.get(number.index())
    }
    pub const fn schemas(&self) -> &BTreeMap<Slug, Vec<Definition>> {
        &self.schemas
    }
    pub const fn work(&self) -> &BTreeMap<Id, Work> {
        &self.work
    }
    pub const fn baselines(&self) -> &BTreeMap<Id, Baseline> {
        &self.baselines
    }
    pub fn elections(&self) -> &[Election] {
        &self.elections
    }
    pub const fn conclusion(&self) -> Option<&Conclusion> {
        self.conclusion.as_ref()
    }
    pub const fn archived(&self) -> bool {
        self.archived
    }
    pub const fn removed(&self) -> &BTreeMap<Id, Removed> {
        &self.removed
    }
    pub const fn names(&self) -> &BTreeMap<Name, Bound> {
        &self.names
    }
    pub const fn baseline_names(&self) -> &BTreeMap<Slug, Bound> {
        &self.baseline_names
    }

    pub fn named(&self, name: &Name) -> Option<(Id, &Work)> {
        let id = self.names.get(name)?.id;
        Some((id, self.work.get(&id)?))
    }
    pub fn baseline_named(&self, slug: &Slug) -> Option<(Id, &Baseline)> {
        let id = self.baseline_names.get(slug)?.id;
        Some((id, self.baselines.get(&id)?))
    }

    /// A record's current name; None once removed.
    pub fn name_of(&self, id: Id) -> Option<String> {
        self.work
            .get(&id)
            .map(|w| w.name.to_string())
            .or_else(|| self.baselines.get(&id).map(|b| b.slug.to_string()))
    }

    /// The kind of a live or removed record.
    pub fn kind_of(&self, id: Id) -> Option<Kind> {
        self.work
            .get(&id)
            .map(Work::kind)
            .or_else(|| self.baselines.contains_key(&id).then_some(Kind::Baseline))
            .or_else(|| self.removed.get(&id).map(|r| r.kind))
    }

    /// Render a reference by current name; None once removed.
    pub fn ref_name(&self, r: &Ref) -> Option<String> {
        match r {
            Ref::Trial(id) | Ref::Iset(id) | Ref::Baseline(id) => self.name_of(*id),
            Ref::Metric(regime, key) => Some(format!("{key}@{regime}")),
        }
    }

    pub fn definition(&self, binding: &Binding) -> Option<&Definition> {
        match binding {
            Binding::Exact { id, at } => self.schemas.get(id)?.iter().find(|d| d.at.pos == *at),
            Binding::Requested { .. } => None,
        }
    }
}
