use crate::{
    Body, Finding, Frontier, Id, Reading,
    schema::{Carrier, Slot},
};
use std::collections::{BTreeMap, BTreeSet};

/// A lifecycle state derived from the ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum State {
    Waiting,
    Ready,
    Running,
    Tested,
    Reported,
    Settled,
}

/// What views show for work: its state, or void in its place.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Shown {
    State(State),
    Void,
}

/// A resolution that consumed evidence when written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Consumer {
    Report(Id),
    Judgment(Id),
    Election(usize),
    Conclusion,
}

pub type Counts = BTreeMap<(Carrier, Shown), usize>;

impl State {
    pub const ALL: [Self; 6] = [
        Self::Waiting,
        Self::Ready,
        Self::Running,
        Self::Tested,
        Self::Reported,
        Self::Settled,
    ];
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Tested => "tested",
            Self::Reported => "reported",
            Self::Settled => "settled",
        }
    }
}

impl Shown {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::State(state) => state.as_str(),
            Self::Void => "void",
        }
    }
    pub fn parse(raw: &str) -> Option<Self> {
        if raw == "void" {
            return Some(Self::Void);
        }
        State::ALL
            .into_iter()
            .find(|state| state.as_str() == raw)
            .map(Self::State)
    }
}

impl Frontier {
    /// Resolved and not void.
    pub fn done(&self, id: Id) -> bool {
        self.work
            .get(&id)
            .is_some_and(|w| w.void.is_none() && w.resolved())
    }

    /// Neither resolved nor void.
    pub fn is_open(&self, id: Id) -> bool {
        self.work
            .get(&id)
            .is_some_and(|w| w.void.is_none() && !w.resolved())
    }

    fn inputs_done(&self, id: Id) -> bool {
        self.work
            .get(&id)
            .is_some_and(|w| w.inputs.iter().all(|input| self.done(*input)))
    }

    /// Members at any depth.
    pub fn below(&self, id: Id) -> BTreeSet<Id> {
        let mut out = BTreeSet::new();
        let mut stack = vec![id];
        while let Some(next) = stack.pop() {
            for member in self.members(next) {
                if out.insert(*member) {
                    stack.push(*member);
                }
            }
        }
        out
    }

    pub fn members(&self, id: Id) -> impl Iterator<Item = &Id> {
        self.work
            .get(&id)
            .and_then(crate::Work::iset)
            .into_iter()
            .flat_map(|iset| iset.members.iter())
    }

    /// What a judgment of the iset would reach: its members, descending through open isets.
    pub fn reach(&self, id: Id) -> BTreeSet<Id> {
        let mut out = BTreeSet::new();
        let mut stack = vec![id];
        while let Some(next) = stack.pop() {
            for member in self.members(next) {
                if out.insert(*member)
                    && self.is_open(*member)
                    && self.work.get(member).is_some_and(|w| w.iset().is_some())
                {
                    stack.push(*member);
                }
            }
        }
        out
    }

    pub fn state(&self, id: Id) -> Option<State> {
        let work = self.work.get(&id)?;
        Some(match &work.body {
            Body::Trial(trial) => {
                if trial.report.is_some() {
                    State::Reported
                } else if work.running.is_some() {
                    State::Running
                } else if self.inputs_done(id) {
                    State::Ready
                } else {
                    State::Waiting
                }
            }
            Body::Iset(iset) => {
                let below = self.below(id);
                let trial = |id: &Id, report: bool| {
                    self.work.get(id).is_some_and(|w| {
                        w.void.is_none()
                            && w.trial().is_some_and(|t| t.report.is_some() == report)
                            && (report || w.running.is_some())
                    })
                };
                if iset.judgment.is_some() {
                    State::Settled
                } else if work.running.is_some() || below.iter().any(|m| trial(m, false)) {
                    State::Running
                } else if iset.members.iter().any(|m| self.is_open(*m)) {
                    State::Waiting
                } else if below.iter().any(|m| trial(m, true)) {
                    State::Tested
                } else if self.inputs_done(id) {
                    State::Ready
                } else {
                    State::Waiting
                }
            }
        })
    }

    pub fn shown(&self, id: Id) -> Option<Shown> {
        let work = self.work.get(&id)?;
        if work.void.is_some() {
            Some(Shown::Void)
        } else {
            self.state(id).map(Shown::State)
        }
    }

    /// The latest election of the current regime whose baseline is neither void nor removed.
    pub fn incumbent(&self) -> Option<(usize, Id)> {
        let current = self.current_regime().map(|r| r.number);
        self.elections
            .iter()
            .enumerate()
            .rev()
            .find(|(_, e)| {
                e.regime == current
                    && self
                        .baselines
                        .get(&e.baseline)
                        .is_some_and(|b| b.void.is_none())
            })
            .map(|(index, e)| (index, e.baseline))
    }

    pub fn consumes(&self, consumer: Consumer) -> Option<&BTreeSet<Id>> {
        match consumer {
            Consumer::Report(id) => self
                .work
                .get(&id)?
                .trial()?
                .report
                .as_ref()
                .map(|r| &r.consumes),
            Consumer::Judgment(id) => self
                .work
                .get(&id)?
                .iset()?
                .judgment
                .as_ref()
                .map(|j| &j.consumes),
            Consumer::Election(index) => self.elections.get(index).map(|e| &e.consumes),
            Consumer::Conclusion => self.conclusion.as_ref().map(|c| &c.consumes),
        }
    }

    /// Every resolution whose consumption includes the record.
    pub fn consumers(&self, id: Id) -> Vec<Consumer> {
        let mut out: Vec<Consumer> = self
            .work
            .iter()
            .flat_map(|(w, work)| {
                let report = work
                    .trial()
                    .and_then(|t| t.report.as_ref())
                    .filter(|r| r.consumes.contains(&id))
                    .map(|_| Consumer::Report(*w));
                let judgment = work
                    .iset()
                    .and_then(|i| i.judgment.as_ref())
                    .filter(|j| j.consumes.contains(&id))
                    .map(|_| Consumer::Judgment(*w));
                report.into_iter().chain(judgment)
            })
            .collect();
        out.extend(
            self.elections
                .iter()
                .enumerate()
                .filter(|(_, e)| e.consumes.contains(&id))
                .map(|(index, _)| Consumer::Election(index)),
        );
        if self
            .conclusion
            .as_ref()
            .is_some_and(|c| c.consumes.contains(&id))
        {
            out.push(Consumer::Conclusion);
        }
        out
    }

    /// Work waiting on the record.
    pub fn waiting_on(&self, id: Id) -> Vec<Id> {
        self.work
            .iter()
            .filter(|(_, w)| w.inputs.contains(&id))
            .map(|(w, _)| *w)
            .collect()
    }

    /// Isets containing the record.
    pub fn containers(&self, id: Id) -> Vec<Id> {
        self.work
            .iter()
            .filter(|(_, w)| w.iset().is_some_and(|i| i.members.contains(&id)))
            .map(|(w, _)| *w)
            .collect()
    }

    pub fn is_void(&self, id: Id) -> bool {
        self.work
            .get(&id)
            .map(|w| w.void.is_some())
            .or_else(|| self.baselines.get(&id).map(|b| b.void.is_some()))
            .unwrap_or(false)
    }

    /// Consumed records now void: what a resolution rests on.
    pub fn void_sources(&self, consumes: &BTreeSet<Id>) -> Vec<Id> {
        consumes
            .iter()
            .copied()
            .filter(|id| self.is_void(*id))
            .collect()
    }

    /// Whether a consumed record has since been removed.
    pub fn removed_source(&self, consumes: &BTreeSet<Id>) -> bool {
        consumes.iter().any(|id| self.removed.contains_key(id))
    }

    /// A void record reads as invalidated when it had resolved, been elected or been consumed.
    pub fn invalidated(&self, id: Id) -> bool {
        let Some(void) = self
            .work
            .get(&id)
            .and_then(|w| w.void.as_ref())
            .or_else(|| self.baselines.get(&id).and_then(|b| b.void.as_ref()))
        else {
            return false;
        };
        let resolved = self.work.get(&id).is_some_and(crate::Work::resolved)
            || self
                .baselines
                .get(&id)
                .is_some_and(|b| b.identity_frozen.is_some_and(|at| at < void.at.pos));
        resolved
            || self.consumers(id).into_iter().any(|consumer| {
                self.consumer_pos(consumer)
                    .is_some_and(|pos| pos < void.at.pos)
            })
    }

    pub fn consumer_pos(&self, consumer: Consumer) -> Option<crate::Pos> {
        match consumer {
            Consumer::Report(id) => Some(self.work.get(&id)?.trial()?.report.as_ref()?.at.pos),
            Consumer::Judgment(id) => Some(self.work.get(&id)?.iset()?.judgment.as_ref()?.at.pos),
            Consumer::Election(index) => Some(self.elections.get(index)?.at.pos),
            Consumer::Conclusion => Some(self.conclusion.as_ref()?.at.pos),
        }
    }

    /// Violations of a work record's binding, each `path: problem`.
    pub fn violations(&self, id: Id) -> Vec<String> {
        let Some(work) = self.work.get(&id) else {
            return Vec::new();
        };
        let Some(definition) = work.binding.as_ref().and_then(|b| self.definition(b)) else {
            return Vec::new();
        };
        let schema = &definition.schema;
        match &work.body {
            Body::Iset(iset) => {
                let mut out = schema.check(Slot::Data, &iset.data);
                if let Some(judgment) = &iset.judgment {
                    out.extend(schema.check(Slot::Answer, &judgment.answer));
                }
                out
            }
            Body::Trial(trial) => {
                let mut out = trial
                    .plan()
                    .map(|plan| schema.check(Slot::Plan, &plan.data))
                    .unwrap_or_default();
                if let Some(report) = &trial.report {
                    out.extend(schema.check(Slot::Report, &report.data));
                }
                out
            }
        }
    }

    pub fn findings(&self) -> BTreeSet<Finding> {
        let mut out = BTreeSet::new();
        let incumbent = self.incumbent();
        for (&id, work) in &self.work {
            if work.void.is_some() {
                continue;
            }
            match &work.binding {
                Some(crate::Binding::Requested { .. }) => {
                    out.insert(Finding::Unvalidated(id));
                }
                Some(_) if !self.violations(id).is_empty() => {
                    out.insert(Finding::Nonconforming(id));
                }
                _ => {}
            }
            if let Some(iset) = work.iset()
                && iset.judgment.is_some()
                && (iset.members.iter().any(|m| self.is_open(*m)) || !self.inputs_done(id))
            {
                out.insert(Finding::Unsettled(id));
            }
            if !work.resolved() {
                if work
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.bound < work.rev)
                {
                    out.insert(Finding::Stale(id));
                }
                if let Some(control) = work.trial().and_then(|t| t.plan()).and_then(|p| p.control)
                    && control.defaulted
                    && incumbent.map(|(_, b)| b) != Some(control.baseline)
                {
                    out.insert(Finding::Superseded(id));
                }
            }
        }
        if let Some((index, baseline)) = incumbent {
            for iset in self.elections[index].adopts.keys() {
                if self
                    .work
                    .get(iset)
                    .is_some_and(|w| w.void.is_none() && !w.resolved())
                {
                    out.insert(Finding::Unjudged(baseline, *iset));
                }
            }
            if let (Some(regime), Some(b)) = (self.current_regime(), self.baselines.get(&baseline))
            {
                for (key, metric) in &regime.gauge.metrics {
                    if metric.kpi && !metric.retired && !b.readings.contains_key(key) {
                        out.insert(Finding::Unread(baseline, key.clone()));
                    }
                }
            }
        }
        for regime in self.regimes.iter().skip(1) {
            if regime.bridge.is_none() {
                out.insert(Finding::Unbridged(regime.number));
            }
        }
        let mut unlisted: BTreeMap<_, usize> = BTreeMap::new();
        for work in self.work.values().filter(|w| w.void.is_none()) {
            let Some(trial) = work.trial() else { continue };
            let (Some(regime), Some(report)) = (trial.regime, &trial.report) else {
                continue;
            };
            for (key, reading) in &report.readings {
                if matches!(reading, Reading::Unlisted(_)) {
                    *unlisted.entry((regime, key.clone())).or_default() += 1;
                }
            }
        }
        for ((regime, key), count) in unlisted {
            if count >= 3 {
                out.insert(Finding::Unlisted(regime, key));
            }
        }
        out
    }

    pub fn counts(&self) -> Counts {
        let mut out = Counts::new();
        for (&id, work) in &self.work {
            if let Some(shown) = self.shown(id) {
                *out.entry((work.carrier(), shown)).or_default() += 1;
            }
        }
        out
    }
}
