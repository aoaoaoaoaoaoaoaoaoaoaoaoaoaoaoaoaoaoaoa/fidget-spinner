use crate::{
    data::{Data, DataError, Path, Ref, RefKind, join},
    schema::Schema,
    *,
};
use serde_json::Value as Json;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Why an operation was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    Malformed,
    Unknown,
    Collision,
    Cycle,
    Unit,
    Meaning,
    Grammar,
    Guard,
    Resolved,
    Void,
    Frozen,
    Concluded,
    Archived,
    Election,
    Bridge,
    Reserved,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{path}: {message}")]
pub struct OpRefusal {
    pub fault: Fault,
    pub path: String,
    pub message: String,
}

/// A refused patch: nothing applied.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("operation {index} ({op}): {refusal}")]
pub struct Refusal {
    pub index: usize,
    pub op: &'static str,
    pub refusal: OpRefusal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Warning {
    pub subject: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Created {
    Work(Id),
    Baseline(Id),
    Regime(RegimeNumber),
    Election(usize),
    Definition(Slug),
}

/// A record voided by a patch, with what rests on it and what waits on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Voided {
    pub id: Id,
    pub consumers: Vec<Consumer>,
    pub waiting: Vec<Id>,
}

/// The records one operation created, the names it resolved and the data
/// references it resolved, for history redaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trace {
    pub created: Vec<Id>,
    pub named: Vec<(String, Id)>,
    pub refs: Vec<(String, Id)>,
}

/// An accepted patch's candidate state and receipt facts.
pub struct Patched {
    pub frontier: Frontier,
    pub warnings: BTreeMap<usize, Vec<Warning>>,
    pub created: Vec<Created>,
    pub raised: Vec<(String, String)>,
    pub cleared: Vec<String>,
    pub before: Counts,
    pub after: Counts,
    pub unblocked: Vec<Id>,
    pub voided: Vec<Voided>,
}

/// Refuse operations a caller may not write.
pub fn bar_reserved(ops: &[Op]) -> Result<(), Refusal> {
    for (index, op) in ops.iter().enumerate() {
        if let Some(reason) = op.operation().reserved() {
            return Err(Refusal {
                index,
                op: op.name(),
                refusal: refuse(Fault::Reserved, "op", reason),
            });
        }
    }
    Ok(())
}

fn refuse(fault: Fault, path: impl Into<String>, message: impl Into<String>) -> OpRefusal {
    OpRefusal {
        fault,
        path: path.into(),
        message: message.into(),
    }
}

fn required(text: &str, field: &str) -> Result<String, OpRefusal> {
    let text = text.trim();
    if text.is_empty() {
        Err(refuse(Fault::Malformed, field, "must not be empty"))
    } else {
        Ok(text.to_owned())
    }
}

fn data_error(path: &str, error: DataError) -> OpRefusal {
    let path = if error.path.is_empty() {
        path.to_owned()
    } else {
        format!("{path}.{}", join(&error.path))
    };
    refuse(Fault::Malformed, path, error.message)
}

#[derive(Default)]
struct Log {
    warnings: BTreeMap<usize, Vec<Warning>>,
    created: Vec<Created>,
    judged: Vec<Id>,
    voided: Vec<Id>,
    traces: Vec<Trace>,
}

/// Per-patch bookkeeping.
struct Ctx {
    stamp: Stamp,
    pos: Pos,
    legacy: bool,
    dep: Seq,
    pre_rev: BTreeMap<Id, Pos>,
    pre_def: BTreeMap<Slug, Pos>,
    implicit: BTreeMap<Id, Seq>,
    guards: BTreeMap<Id, Pos>,
    fresh: BTreeSet<Id>,
    warnings: Vec<Warning>,
    trace: Trace,
    log: Log,
}

impl Ctx {
    const fn moment(&self) -> Moment {
        Moment {
            pos: self.pos,
            time: self.stamp.time,
        }
    }
    fn warn(&mut self, subject: impl Into<String>, message: impl Into<String>) {
        self.warnings.push(Warning {
            subject: subject.into(),
            message: message.into(),
        });
    }
    fn depend(&mut self, seq: Seq) {
        self.dep = self.dep.max(seq);
    }
    fn named(&mut self, name: &impl ToString, id: Id) {
        self.trace.named.push((name.to_string(), id));
    }
}

impl Frontier {
    /// Apply a patch to a copy of the state, with receipt diagnostics.
    pub fn patched(&self, ops: &[Op], stamp: Stamp) -> Result<Patched, Refusal> {
        let mut next = self.clone();
        let log = next.run(ops, stamp)?;
        Ok(self.diagnose(next, log))
    }

    /// Replay a committed patch.
    pub fn replay(&mut self, ops: &[Op], stamp: Stamp) -> Result<(), Refusal> {
        self.run(ops, stamp).map(drop)
    }

    /// Replay a committed patch, returning what each operation created and named.
    pub fn trace(&mut self, ops: &[Op], stamp: Stamp) -> Result<Vec<Trace>, Refusal> {
        self.run(ops, stamp).map(|log| log.traces)
    }

    const fn legacy_at(&self, seq: Seq) -> bool {
        match self.legacy_through {
            Some(through) => seq <= through,
            None => false,
        }
    }

    fn run(&mut self, ops: &[Op], stamp: Stamp) -> Result<Log, Refusal> {
        let at = |index: usize, op: &'static str, refusal| Refusal { index, op, refusal };
        if self.objective.is_none()
            && !matches!(
                ops.first().map(Op::operation),
                Some(Operation::Frontier { .. })
            )
        {
            return Err(at(
                0,
                ops.first().map_or("patch", Op::name),
                refuse(
                    Fault::Unknown,
                    "frontier",
                    "a new frontier's first operation is frontier {objective}",
                ),
            ));
        }
        if ops.is_empty() && !self.legacy_at(stamp.seq) {
            return Err(at(
                0,
                "patch",
                refuse(Fault::Malformed, "ops", "a patch needs an operation"),
            ));
        }
        let mut ctx = Ctx {
            stamp,
            pos: Pos {
                seq: stamp.seq,
                index: 0,
            },
            legacy: false,
            dep: 0,
            pre_rev: BTreeMap::new(),
            pre_def: BTreeMap::new(),
            implicit: BTreeMap::new(),
            guards: BTreeMap::new(),
            fresh: BTreeSet::new(),
            warnings: Vec::new(),
            trace: Trace::default(),
            log: Log::default(),
        };
        for (index, op) in ops.iter().enumerate() {
            let fail = |refusal| at(index, op.name(), refusal);
            ctx.pos.index = u32::try_from(index)
                .map_err(|_| fail(refuse(Fault::Malformed, "ops", "too many operations")))?;
            ctx.dep = 0;
            ctx.legacy = self.legacy_at(stamp.seq);
            if self.archived && !matches!(op.operation(), Operation::Unarchive {}) {
                return Err(fail(refuse(
                    Fault::Archived,
                    "frontier",
                    "the frontier is archived; its owner unarchives it in the navigator",
                )));
            }
            self.apply(op.operation(), &mut ctx).map_err(fail)?;
            if !ctx.warnings.is_empty() {
                ctx.log
                    .warnings
                    .insert(index, std::mem::take(&mut ctx.warnings));
            }
            let trace = std::mem::take(&mut ctx.trace);
            ctx.log.traces.push(trace);
        }
        self.head = stamp.seq;
        Ok(ctx.log)
    }

    fn diagnose(&self, after: Self, log: Log) -> Patched {
        let before_findings = self.findings();
        let after_findings = after.findings();
        let raised = after_findings
            .difference(&before_findings)
            .map(|f| (f.identity(&after), f.statement(&after)))
            .collect();
        let cleared = before_findings
            .difference(&after_findings)
            .map(|f| f.identity(self))
            .collect();
        let mut unblocked = Vec::new();
        for judged in &log.judged {
            for waiting in after.waiting_on(*judged) {
                if after.state(waiting) == Some(State::Ready)
                    && self.state(waiting) != Some(State::Ready)
                    && !unblocked.contains(&waiting)
                {
                    unblocked.push(waiting);
                }
            }
        }
        let voided = log
            .voided
            .iter()
            .map(|id| Voided {
                id: *id,
                consumers: after.consumers(*id),
                waiting: after.waiting_on(*id),
            })
            .collect();
        Patched {
            before: self.counts(),
            after: after.counts(),
            frontier: after,
            warnings: log.warnings,
            created: log.created,
            raised,
            cleared,
            unblocked,
            voided,
        }
    }

    fn apply(&mut self, op: &Operation, ctx: &mut Ctx) -> Result<(), OpRefusal> {
        match op {
            Operation::Frontier {
                objective,
                legacy_through,
            } => {
                if self.objective.is_some() {
                    return Err(refuse(Fault::Collision, "op", "the frontier exists"));
                }
                self.objective = Some(required(objective, "objective")?);
                self.legacy_through = legacy_through.map(|s| s.0);
            }
            Operation::Conclude { lesson } => {
                self.live()?;
                let lesson = required(lesson, "lesson")?;
                let mut consumes: BTreeSet<Id> = self
                    .work
                    .keys()
                    .copied()
                    .filter(|id| self.done(*id))
                    .collect();
                consumes.extend(
                    self.elections
                        .iter()
                        .map(|e| e.baseline)
                        .filter(|b| self.baselines.get(b).is_some_and(|b| b.void.is_none())),
                );
                let open = self
                    .work
                    .keys()
                    .copied()
                    .filter(|id| self.is_open(*id))
                    .collect();
                self.conclusion = Some(Conclusion {
                    at: ctx.moment(),
                    lesson,
                    consumes,
                    open,
                });
            }
            Operation::Archive {} => {
                if self.conclusion.is_none() {
                    return Err(refuse(
                        Fault::Malformed,
                        "op",
                        "only a concluded frontier is archived",
                    ));
                }
                self.archived = true;
            }
            Operation::Unarchive {} => {
                if !self.archived {
                    return Err(refuse(
                        Fault::Malformed,
                        "op",
                        "the frontier is not archived",
                    ));
                }
                self.archived = false;
            }
            Operation::Guard { head } => {
                if self.head > head.0 {
                    return Err(refuse(
                        Fault::Guard,
                        "head",
                        format!(
                            "the frontier has patches after seq {}; its head is seq {}",
                            head.0, self.head
                        ),
                    ));
                }
            }
            Operation::Regime {
                contract,
                metrics,
                reason,
                bridge,
            } => self.regime_change(contract, metrics, reason.as_deref(), bridge.as_ref(), ctx)?,
            Operation::Gauge {
                contract,
                metrics,
                retire,
                restore,
            } => self.gauge(
                contract.as_deref(),
                metrics.as_deref(),
                retire.as_deref(),
                restore.as_deref(),
                ctx,
            )?,
            Operation::Define {
                schema,
                definition,
                rev,
            } => self.define(schema, definition, *rev, ctx)?,
            Operation::Iset {
                slug,
                schema,
                data,
                members,
                into,
                after,
            } => {
                self.live()?;
                let name = Name::Slug(slug.clone());
                self.free(&name, "slug")?;
                let binding = self.bind(schema.as_ref(), slug.as_str(), ctx);
                let data = self.data(data.as_ref(), "data", ctx)?;
                let id = self.create(
                    name,
                    binding,
                    Body::Iset(Iset {
                        data,
                        members: BTreeSet::new(),
                        judgment: None,
                        superseded: Vec::new(),
                    }),
                    ctx,
                )?;
                for (i, member) in members.iter().flatten().enumerate() {
                    let path = format!("members[{i}]");
                    let member = self.resolve(member, &path, ctx)?;
                    self.link(member, id, &path, ctx)?;
                }
                self.place(id, into.as_deref(), after.as_deref(), ctx)?;
                self.conform(id, ctx);
            }
            Operation::Plan {
                trial,
                schema,
                data,
                control,
                treatment,
                into,
                after,
            } => {
                self.live()?;
                let isets = self.isets(into.as_deref(), ctx)?;
                let name = self.trial_name(trial.as_ref(), &isets)?;
                let subject = name.to_string();
                let regime = self.regime_now(ctx);
                let control = self.control(control.as_ref(), regime, &subject, ctx)?;
                let treatment = self.treatment(treatment.as_ref(), &subject, ctx)?;
                let binding = self.bind(schema.as_ref(), &subject, ctx);
                let data = self.data(data.as_ref(), "data", ctx)?;
                let plan = Plan {
                    at: ctx.moment(),
                    data,
                    control,
                    treatment,
                };
                let id = self.create(
                    name,
                    binding,
                    Body::Trial(Trial {
                        regime,
                        plans: vec![plan],
                        report: None,
                    }),
                    ctx,
                )?;
                self.join(id, &isets, ctx)?;
                self.place(id, None, after.as_deref(), ctx)?;
                self.conform(id, ctx);
            }
            Operation::Report {
                trial,
                plan,
                readings,
                control_readings,
                artifacts,
                data,
                schema,
                control,
                treatment,
                into,
                after,
            } => {
                self.live()?;
                let existing = trial
                    .as_ref()
                    .and_then(|name| self.names.get(name))
                    .copied();
                let (id, retrospective) = if let Some(bound) = existing {
                    ctx.depend(bound.since.seq);
                    let id = bound.id;
                    if self.work[&id].trial().is_none() {
                        return Err(refuse(
                            Fault::Unknown,
                            "trial",
                            format!("{} is an iset", self.work[&id].name),
                        ));
                    }
                    for (field, given) in [
                        ("schema", schema.is_some()),
                        ("control", control.is_some()),
                        ("treatment", treatment.is_some()),
                        ("into", into.is_some()),
                        ("after", after.is_some()),
                    ] {
                        if given {
                            return Err(refuse(
                                Fault::Malformed,
                                field,
                                "applies only when the report creates its trial",
                            ));
                        }
                    }
                    self.ensure_open(id, "trial", ctx)?;
                    (id, None)
                } else {
                    let isets = self.isets(into.as_deref(), ctx)?;
                    let name = self.trial_name(trial.as_ref(), &isets)?;
                    let subject = name.to_string();
                    let regime = self.regime_now(ctx);
                    let control = self.control(control.as_ref(), regime, &subject, ctx)?;
                    let treatment = self.treatment(treatment.as_ref(), &subject, ctx)?;
                    let binding = self.bind(schema.as_ref(), &subject, ctx);
                    let id = self.create(
                        name,
                        binding,
                        Body::Trial(Trial {
                            regime,
                            plans: Vec::new(),
                            report: None,
                        }),
                        ctx,
                    )?;
                    self.join(id, &isets, ctx)?;
                    self.place(id, None, after.as_deref(), ctx)?;
                    (id, Some((control, treatment)))
                };
                let written = Written {
                    readings: readings.as_ref(),
                    control_readings: control_readings.as_ref(),
                    artifacts: artifacts.as_deref(),
                    data: data.as_ref(),
                };
                self.report(id, retrospective, *plan, written, ctx)?;
            }
            Operation::Edit {
                work,
                rev,
                data,
                set,
                delete,
                schema,
                unbind,
                control,
                treatment,
            } => {
                let edit = Change {
                    data: data.as_ref(),
                    set: set.as_deref(),
                    delete: delete.as_deref(),
                    schema: schema.as_ref(),
                    unbind: *unbind,
                    control: control.as_ref(),
                    treatment: treatment.as_ref(),
                };
                self.edit(work, *rev, &edit, ctx)?;
            }
            Operation::Member { iset, add, drop } => {
                self.live()?;
                let id = self.resolve_iset(iset, "iset", ctx)?;
                self.ensure_open(id, "iset", ctx)?;
                if add.is_none() && drop.is_none() {
                    return Err(refuse(Fault::Malformed, "add", "member needs add or drop"));
                }
                for (i, member) in add.iter().flatten().enumerate() {
                    let path = format!("add[{i}]");
                    let member = self.resolve(member, &path, ctx)?;
                    self.link(member, id, &path, ctx)?;
                }
                for (i, member) in drop.iter().flatten().enumerate() {
                    let path = format!("drop[{i}]");
                    let member = self.resolve(member, &path, ctx)?;
                    let removed = match self.work.get_mut(&id).map(|w| &mut w.body) {
                        Some(Body::Iset(iset)) => iset.members.remove(&member),
                        _ => false,
                    };
                    if !removed {
                        return Err(refuse(Fault::Unknown, path, "not a member"));
                    }
                    self.touch(id, ctx);
                }
            }
            Operation::After { work, add, drop } => {
                self.live()?;
                let id = self.resolve(work, "work", ctx)?;
                self.ensure_open(id, "work", ctx)?;
                if add.is_none() && drop.is_none() {
                    return Err(refuse(Fault::Malformed, "add", "after needs add or drop"));
                }
                for (i, input) in add.iter().flatten().enumerate() {
                    let path = format!("add[{i}]");
                    let input = self.resolve(input, &path, ctx)?;
                    self.order(input, id, &path, ctx)?;
                }
                for (i, input) in drop.iter().flatten().enumerate() {
                    let path = format!("drop[{i}]");
                    let input = self.resolve(input, &path, ctx)?;
                    let removed = self
                        .work
                        .get_mut(&id)
                        .is_some_and(|w| w.inputs.remove(&input));
                    if !removed {
                        return Err(refuse(Fault::Unknown, path, "not an input"));
                    }
                    self.touch(id, ctx);
                }
            }
            Operation::Judge { iset, answer, seen } => {
                self.judge(iset, answer, seen.map(|s| s.0), ctx)?;
            }
            Operation::Baseline {
                slug,
                artifacts,
                readings,
                note,
            } => self.baseline(
                slug,
                artifacts.as_deref(),
                readings.as_ref(),
                note.as_deref(),
                ctx,
            )?,
            Operation::Elect {
                baseline,
                rationale,
                adopts,
            } => {
                self.live()?;
                let rationale = required(rationale, "rationale")?;
                let id = self.resolve_baseline(baseline, "baseline", ctx)?;
                self.elect(id, rationale, adopts.as_deref(), ctx)?;
            }
            Operation::Void { target, reason } => self.void(target, reason, ctx)?,
            Operation::Remove { target } => self.remove(target, ctx)?,
            Operation::Rename { target, to } => self.rename(target, to, ctx)?,
            Operation::Assign {
                work,
                owner,
                instructions,
            } => {
                let id = self.supervised(work, ctx)?;
                let name = required(owner, "owner")?;
                let instructions = instructions
                    .as_deref()
                    .map(|i| required(i, "instructions"))
                    .transpose()?;
                if let Some(work) = self.work.get_mut(&id) {
                    work.owner = Some(Owner {
                        name,
                        instructions,
                        bound: work.rev,
                    });
                }
            }
            Operation::Release { work } => {
                let id = self.supervised(work, ctx)?;
                if let Some(work) = self.work.get_mut(&id) {
                    work.owner = None;
                }
            }
            Operation::Start { work } => {
                let id = self.supervised(work, ctx)?;
                let now = ctx.moment();
                if let Some(work) = self.work.get_mut(&id) {
                    work.running = Some(now);
                }
            }
            Operation::Stop { work } => {
                let id = self.supervised(work, ctx)?;
                if let Some(work) = self.work.get_mut(&id) {
                    work.running = None;
                }
            }
            Operation::Reopen { iset } => {
                if !ctx.legacy {
                    return Err(refuse(
                        Fault::Reserved,
                        "op",
                        "reopen exists only in migrated FS 3 history",
                    ));
                }
                let id = self.resolve_iset(iset, "iset", ctx)?;
                if let Some(Body::Iset(iset)) = self.work.get_mut(&id).map(|w| &mut w.body)
                    && let Some(judgment) = iset.judgment.take()
                {
                    iset.superseded.push(judgment);
                }
                self.touch(id, ctx);
            }
        }
        Ok(())
    }

    fn live(&self) -> Result<(), OpRefusal> {
        if self.conclusion.is_some() {
            Err(refuse(
                Fault::Concluded,
                "frontier",
                "the frontier is concluded; only void, remove and rename apply",
            ))
        } else {
            Ok(())
        }
    }

    fn ensure_open(&self, id: Id, path: &str, ctx: &Ctx) -> Result<(), OpRefusal> {
        if ctx.legacy {
            return Ok(());
        }
        let work = &self.work[&id];
        if work.void.is_some() {
            return Err(refuse(Fault::Void, path, format!("{} is void", work.name)));
        }
        if work.resolved() {
            let verb = if work.iset().is_some() {
                "judged"
            } else {
                "reported"
            };
            return Err(refuse(
                Fault::Resolved,
                path,
                format!("{} is {verb}; resolved work never changes", work.name),
            ));
        }
        Ok(())
    }

    fn supervised(&self, work: &Name, ctx: &mut Ctx) -> Result<Id, OpRefusal> {
        self.live()?;
        let id = self.resolve(work, "work", ctx)?;
        self.ensure_open(id, "work", ctx)?;
        Ok(id)
    }

    fn regime_now(&self, ctx: &mut Ctx) -> Option<RegimeNumber> {
        let regime = self.current_regime()?;
        ctx.depend(regime.at.pos.seq);
        Some(regime.number)
    }

    fn resolve(&self, name: &Name, path: &str, ctx: &mut Ctx) -> Result<Id, OpRefusal> {
        let Some(bound) = self.names.get(name) else {
            let text = name.to_string();
            let names: Vec<String> = self.names.keys().map(ToString::to_string).collect();
            let nearby = slug::nearest(&text, names.iter().map(String::as_str));
            return Err(refuse(
                Fault::Unknown,
                path,
                slug::suggestion(&format!("work {text:?}"), &nearby),
            ));
        };
        ctx.depend(bound.since.seq);
        ctx.named(&name, bound.id);
        Ok(bound.id)
    }

    fn resolve_iset(&self, slug: &Slug, path: &str, ctx: &mut Ctx) -> Result<Id, OpRefusal> {
        let id = self.resolve(&Name::Slug(slug.clone()), path, ctx)?;
        if self.work[&id].iset().is_some() {
            Ok(id)
        } else {
            Err(refuse(
                Fault::Unknown,
                path,
                format!("{slug} is a trial, not an iset"),
            ))
        }
    }

    fn resolve_baseline(&self, slug: &Slug, path: &str, ctx: &mut Ctx) -> Result<Id, OpRefusal> {
        let Some(bound) = self.baseline_names.get(slug) else {
            let names: Vec<&str> = self.baseline_names.keys().map(Slug::as_str).collect();
            let nearby = slug::nearest(slug.as_str(), names);
            return Err(refuse(
                Fault::Unknown,
                path,
                slug::suggestion(&format!("baseline {:?}", slug.as_str()), &nearby),
            ));
        };
        ctx.depend(bound.since.seq);
        ctx.named(&slug, bound.id);
        Ok(bound.id)
    }

    fn isets(&self, slugs: Option<&[Slug]>, ctx: &mut Ctx) -> Result<Vec<Id>, OpRefusal> {
        let mut out = Vec::new();
        for (i, slug) in slugs.into_iter().flatten().enumerate() {
            let path = format!("into[{i}]");
            let id = self.resolve_iset(slug, &path, ctx)?;
            self.ensure_open(id, &path, ctx)?;
            out.push(id);
        }
        Ok(out)
    }

    fn free(&self, name: &Name, path: &str) -> Result<(), OpRefusal> {
        if self.names.contains_key(name) {
            Err(refuse(
                Fault::Collision,
                path,
                format!("{name} names existing work"),
            ))
        } else {
            Ok(())
        }
    }

    fn alloc(&mut self) -> Result<Id, OpRefusal> {
        let id = Id(self.next);
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| refuse(Fault::Malformed, "op", "record identities are exhausted"))?;
        Ok(id)
    }

    fn create(
        &mut self,
        name: Name,
        binding: Option<Binding>,
        body: Body,
        ctx: &mut Ctx,
    ) -> Result<Id, OpRefusal> {
        let id = self.alloc()?;
        self.names
            .insert(name.clone(), Bound { id, since: ctx.pos });
        self.work.insert(
            id,
            Work {
                name,
                created: ctx.moment(),
                rev: ctx.pos,
                binding,
                inputs: BTreeSet::new(),
                owner: None,
                running: None,
                void: None,
                body,
            },
        );
        ctx.fresh.insert(id);
        ctx.implicit.insert(id, ctx.dep);
        ctx.trace.created.push(id);
        ctx.log.created.push(Created::Work(id));
        Ok(id)
    }

    fn touch(&mut self, id: Id, ctx: &mut Ctx) {
        if let Some(work) = self.work.get_mut(&id) {
            ctx.pre_rev.entry(id).or_insert(work.rev);
            work.rev = ctx.pos;
            let dep = ctx.implicit.entry(id).or_insert(0);
            *dep = (*dep).max(ctx.dep);
        }
    }

    fn trial_name(&mut self, given: Option<&Name>, isets: &[Id]) -> Result<Name, OpRefusal> {
        if let Some(name) = given {
            self.free(name, "trial")?;
            if let Name::Numbered(slug, n) = name
                && let Some(bound) = self.names.get(&Name::Slug(slug.clone()))
                && self.work[&bound.id].iset().is_some()
            {
                let counter = self.counters.entry(bound.id).or_insert(0);
                *counter = (*counter).max(*n);
            }
            return Ok(name.clone());
        }
        let first = *isets.first().ok_or_else(|| {
            refuse(
                Fault::Malformed,
                "trial",
                "a trial without a name must join an iset (into)",
            )
        })?;
        let slug = self.work[&first].name.slug().clone();
        let counter = self.counters.entry(first).or_insert(0);
        loop {
            *counter += 1;
            let name = Name::Numbered(slug.clone(), *counter);
            if !self.names.contains_key(&name) {
                return Ok(name);
            }
        }
    }

    fn join(&mut self, id: Id, isets: &[Id], ctx: &mut Ctx) -> Result<(), OpRefusal> {
        for (i, iset) in isets.iter().enumerate() {
            self.link(id, *iset, &format!("into[{i}]"), ctx)?;
        }
        Ok(())
    }

    fn place(
        &mut self,
        id: Id,
        into: Option<&[Slug]>,
        after: Option<&[Name]>,
        ctx: &mut Ctx,
    ) -> Result<(), OpRefusal> {
        let isets = self.isets(into, ctx)?;
        self.join(id, &isets, ctx)?;
        for (i, input) in after.into_iter().flatten().enumerate() {
            let path = format!("after[{i}]");
            let input = self.resolve(input, &path, ctx)?;
            self.order(input, id, &path, ctx)?;
        }
        Ok(())
    }

    /// A path of membership (member → iset) and input (input → dependent) edges.
    fn route(&self, from: Id, to: Id) -> Option<Vec<Id>> {
        let mut next: BTreeMap<Id, Vec<Id>> = BTreeMap::new();
        for (&id, work) in &self.work {
            if let Some(iset) = work.iset() {
                for member in &iset.members {
                    next.entry(*member).or_default().push(id);
                }
            }
            for input in &work.inputs {
                next.entry(*input).or_default().push(id);
            }
        }
        let mut parent = BTreeMap::new();
        let mut queue = VecDeque::from([from]);
        while let Some(node) = queue.pop_front() {
            if node == to {
                let mut path = vec![to];
                let mut cursor = to;
                while let Some(&p) = parent.get(&cursor) {
                    path.push(p);
                    cursor = p;
                }
                path.reverse();
                return Some(path);
            }
            for &successor in next.get(&node).into_iter().flatten() {
                if successor != from && !parent.contains_key(&successor) {
                    parent.insert(successor, node);
                    queue.push_back(successor);
                }
            }
        }
        None
    }

    fn cycle(&self, start: Id, path: &[Id]) -> String {
        std::iter::once(start)
            .chain(path.iter().copied())
            .map(|id| self.name_of(id).unwrap_or_default())
            .collect::<Vec<_>>()
            .join(" → ")
    }

    fn link(&mut self, member: Id, iset: Id, path: &str, ctx: &mut Ctx) -> Result<(), OpRefusal> {
        if member == iset {
            return Err(refuse(Fault::Cycle, path, "an iset cannot contain itself"));
        }
        if self.work[&iset]
            .iset()
            .is_some_and(|i| i.members.contains(&member))
        {
            ctx.warn(
                self.work[&iset].name.to_string(),
                format!("{} is already a member", self.work[&member].name),
            );
            return Ok(());
        }
        if let Some(route) = self.route(iset, member) {
            return Err(refuse(
                Fault::Cycle,
                path,
                format!("cycle {}", self.cycle(member, &route)),
            ));
        }
        if let Some(Body::Iset(i)) = self.work.get_mut(&iset).map(|w| &mut w.body) {
            i.members.insert(member);
        }
        self.touch(iset, ctx);
        Ok(())
    }

    fn order(&mut self, input: Id, work: Id, path: &str, ctx: &mut Ctx) -> Result<(), OpRefusal> {
        if input == work {
            return Err(refuse(Fault::Cycle, path, "work cannot wait for itself"));
        }
        if self.work[&work].inputs.contains(&input) {
            ctx.warn(
                self.work[&work].name.to_string(),
                format!("{} is already an input", self.work[&input].name),
            );
            return Ok(());
        }
        if let Some(route) = self.route(work, input) {
            return Err(refuse(
                Fault::Cycle,
                path,
                format!("cycle {}", self.cycle(input, &route)),
            ));
        }
        if self.work[&input].void.is_some() {
            ctx.warn(
                self.work[&work].name.to_string(),
                format!(
                    "input {} is void; void never satisfies",
                    self.work[&input].name
                ),
            );
        }
        if let Some(w) = self.work.get_mut(&work) {
            w.inputs.insert(input);
        }
        self.touch(work, ctx);
        Ok(())
    }

    fn bind(&self, request: Option<&SchemaRef>, subject: &str, ctx: &mut Ctx) -> Option<Binding> {
        let request = request?;
        let definitions = self.schemas.get(&request.id);
        let found = match request.rev {
            None => definitions.and_then(|d| d.last()),
            Some(rev) => definitions.and_then(|d| d.iter().find(|def| def.at.pos == rev)),
        };
        if let Some(definition) = found {
            ctx.depend(definition.at.pos.seq);
            Some(Binding::Exact {
                id: request.id.clone(),
                at: definition.at.pos,
            })
        } else {
            ctx.warn(
                subject,
                format!("schema {request} has no such definition; the binding stays unvalidated"),
            );
            Some(Binding::Requested {
                id: request.id.clone(),
                rev: request.rev,
            })
        }
    }

    fn lookup(&self, kind: RefKind, id: &str) -> Result<(Ref, Seq), String> {
        match kind {
            RefKind::Trial | RefKind::Iset => {
                let name: Name = id.parse()?;
                let bound = self
                    .names
                    .get(&name)
                    .ok_or_else(|| format!("unknown {} {id:?}", kind.as_str()))?;
                match (kind, &self.work[&bound.id].body) {
                    (RefKind::Trial, Body::Trial(_)) => Ok((Ref::Trial(bound.id), bound.since.seq)),
                    (RefKind::Iset, Body::Iset(_)) => Ok((Ref::Iset(bound.id), bound.since.seq)),
                    _ => Err(format!("{id} is not a {}", kind.as_str())),
                }
            }
            RefKind::Baseline => {
                let slug: Slug = id.parse().map_err(|e: SlugError| e.to_string())?;
                let bound = self
                    .baseline_names
                    .get(&slug)
                    .ok_or_else(|| format!("unknown baseline {id:?}"))?;
                Ok((Ref::Baseline(bound.id), bound.since.seq))
            }
            RefKind::Metric => {
                let (key, regime) = id
                    .rsplit_once('@')
                    .ok_or_else(|| "a metric reference is key@regime".to_owned())?;
                let key: MetricKey = key.parse()?;
                let number: RegimeNumber = regime
                    .parse()
                    .map_err(|_| format!("invalid regime {regime:?}"))?;
                let regime = self
                    .regime(number)
                    .ok_or_else(|| format!("no regime {number}"))?;
                if !regime.gauge.metrics.contains_key(&key) {
                    return Err(format!("regime {number} has no metric {key}"));
                }
                Ok((Ref::Metric(number, key), regime.at.pos.seq))
            }
        }
    }

    fn value(&self, value: &Json, path: &str, ctx: &mut Ctx) -> Result<Data, OpRefusal> {
        let mut dep = 0;
        let mut named = Vec::new();
        let parsed = Data::parse(value, &mut |kind, id| {
            let (r, since) = self.lookup(kind, id)?;
            dep = dep.max(since);
            if let Some(record) = r.id() {
                named.push((id.to_owned(), record));
            }
            Ok(r)
        });
        ctx.depend(dep);
        ctx.trace.refs.extend(named);
        parsed.map_err(|e| data_error(path, e))
    }

    fn data(&self, value: Option<&Json>, path: &str, ctx: &mut Ctx) -> Result<Data, OpRefusal> {
        match value {
            None => Ok(Data::empty()),
            Some(value) if value.is_object() => self.value(value, path, ctx),
            Some(_) => Err(refuse(Fault::Malformed, path, "data is an object")),
        }
    }

    fn conform(&self, id: Id, ctx: &mut Ctx) {
        let violations = self.violations(id);
        if let Some(first) = violations.first() {
            let more = violations.len() - 1;
            let tail = if more > 0 {
                format!(" and {more} more")
            } else {
                String::new()
            };
            ctx.warn(
                self.work[&id].name.to_string(),
                format!("nonconforming: {first}{tail}"),
            );
        }
    }

    fn control(
        &self,
        given: Option<&Slug>,
        regime: Option<RegimeNumber>,
        subject: &str,
        ctx: &mut Ctx,
    ) -> Result<Option<Control>, OpRefusal> {
        if let Some(slug) = given {
            let id = self.resolve_baseline(slug, "control", ctx)?;
            let baseline = &self.baselines[&id];
            if baseline.void.is_some() {
                ctx.warn(subject, format!("control {slug} is void"));
            }
            if baseline.regime != regime {
                ctx.warn(subject, format!("control {slug} belongs to another regime"));
            }
            return Ok(Some(Control {
                baseline: id,
                defaulted: false,
            }));
        }
        if regime.is_none() {
            return Ok(None);
        }
        if let Some(pos) = self.incumbency {
            ctx.depend(pos.seq);
        }
        Ok(self.incumbent().map(|(_, baseline)| Control {
            baseline,
            defaulted: true,
        }))
    }

    fn treatment(
        &self,
        given: Option<&TreatmentSpec>,
        subject: &str,
        ctx: &mut Ctx,
    ) -> Result<Option<Treatment>, OpRefusal> {
        match given {
            None => Ok(None),
            Some(TreatmentSpec::Baseline(slug)) => {
                let id = self.resolve_baseline(slug, "treatment", ctx)?;
                if self.baselines[&id].void.is_some() {
                    ctx.warn(subject, format!("treatment {slug} is void"));
                }
                Ok(Some(Treatment::Baseline(id)))
            }
            Some(TreatmentSpec::Identity(text)) => Ok(Some(Treatment::Identity(required(
                text,
                "treatment.identity",
            )?))),
        }
    }

    fn readings(
        &self,
        input: &Readings,
        regime: Option<RegimeNumber>,
        subject: &str,
        ctx: &mut Ctx,
    ) -> Result<RecordedReadings, OpRefusal> {
        let gauge = regime.and_then(|r| self.regime(r)).map(|r| &r.gauge);
        let mut out = BTreeMap::new();
        for (key, value) in input {
            let invalid = || {
                refuse(
                    Fault::Malformed,
                    format!("readings.{key}"),
                    "expected a finite number or \"number unit\"",
                )
            };
            let (number, unit) = match value {
                ReadingValue::Number(n) if n.is_finite() => (*n, None),
                ReadingValue::Number(_) => return Err(invalid()),
                ReadingValue::Text(text) => {
                    let parts: Vec<_> = text.split_whitespace().collect();
                    let [number, unit] = parts.as_slice() else {
                        return Err(invalid());
                    };
                    let number = number
                        .parse::<f64>()
                        .ok()
                        .filter(|n| n.is_finite())
                        .ok_or_else(invalid)?;
                    let unit: Unit = unit.parse().map_err(|e: UnitError| {
                        refuse(Fault::Unit, format!("readings.{key}"), e.to_string())
                    })?;
                    (number, Some(unit))
                }
            };
            let reading = if let Some(metric) = gauge
                .and_then(|g| g.metrics.get(key))
                .filter(|m| !m.retired)
            {
                Reading::Listed(match unit {
                    None => number,
                    Some(unit) => unit.convert(number, &metric.unit).map_err(|error| {
                        refuse(
                            Fault::Unit,
                            format!("readings.{key}"),
                            match error {
                                UnitError::Incompatible { from, to } => format!(
                                    "{from} ({}) does not convert to the metric's {to} ({})",
                                    from.kind(),
                                    to.kind()
                                ),
                                error => error.to_string(),
                            },
                        )
                    })?,
                })
            } else {
                let closest = gauge.and_then(|g| {
                    slug::nearest(key.as_str(), g.metrics.keys().map(MetricKey::as_str))
                        .into_iter()
                        .next()
                });
                ctx.warn(
                    subject,
                    match (gauge, closest) {
                        (None, _) => format!("{key}: no regime; stored as an unlisted reading"),
                        (Some(_), Some(closest)) => {
                            format!("{key}: unlisted reading; closest metric {closest}")
                        }
                        (Some(_), None) => format!("{key}: unlisted reading"),
                    },
                );
                Reading::Unlisted(value.clone())
            };
            out.insert(key.clone(), reading);
        }
        Ok(out)
    }

    fn freeze(&mut self, id: Id, readings: bool, pos: Pos) {
        if let Some(baseline) = self.baselines.get_mut(&id) {
            baseline.identity_frozen.get_or_insert(pos);
            if readings {
                baseline.readings_frozen.get_or_insert(pos);
            }
        }
    }

    fn report(
        &mut self,
        id: Id,
        retrospective: Option<(Option<Control>, Option<Treatment>)>,
        plan: Option<Executed>,
        written: Written<'_>,
        ctx: &mut Ctx,
    ) -> Result<(), OpRefusal> {
        let work = &self.work[&id];
        let subject = work.name.to_string();
        let trial = work
            .trial()
            .ok_or_else(|| refuse(Fault::Unknown, "trial", format!("{subject} is an iset")))?;
        let executed = match plan {
            None => trial.plans.len().checked_sub(1),
            Some(Executed::Unknown) => None,
            Some(Executed::Revision(rev)) => Some(
                trial
                    .plans
                    .iter()
                    .position(|p| p.at.pos == rev)
                    .ok_or_else(|| {
                        refuse(
                            Fault::Unknown,
                            "plan",
                            format!("{rev} is not a revision of {subject}'s plan"),
                        )
                    })?,
            ),
        };
        let (control, treatment) = match retrospective {
            Some(sources) => sources,
            None => executed
                .or_else(|| trial.plans.len().checked_sub(1))
                .and_then(|index| trial.plans.get(index))
                .map(|p| (p.control, p.treatment.clone()))
                .unwrap_or_default(),
        };
        let regime = trial.regime;
        let readings = self.readings(
            written.readings.unwrap_or(&Readings::new()),
            regime,
            &subject,
            ctx,
        )?;
        let control_readings = written
            .control_readings
            .map(|r| self.readings(r, regime, &subject, ctx))
            .transpose()?;
        let data = self.data(written.data, "data", ctx)?;
        let mut consumes = BTreeSet::new();
        if let Some(control) = control {
            consumes.insert(control.baseline);
            self.freeze(control.baseline, control_readings.is_none(), ctx.pos);
        }
        if let Some(Treatment::Baseline(baseline)) = &treatment {
            consumes.insert(*baseline);
            self.freeze(*baseline, false, ctx.pos);
        }
        for source in &consumes {
            if self.is_void(*source) {
                ctx.warn(
                    &subject,
                    format!(
                        "rests on void {}",
                        self.name_of(*source).unwrap_or_default()
                    ),
                );
            }
        }
        let report = Report {
            at: ctx.moment(),
            plan: executed,
            control,
            treatment,
            readings,
            control_readings,
            artifacts: written
                .artifacts
                .map(<[String]>::to_vec)
                .unwrap_or_default(),
            data,
            consumes,
        };
        if let Some(Body::Trial(trial)) = self.work.get_mut(&id).map(|w| &mut w.body) {
            trial.report = Some(report);
        }
        self.touch(id, ctx);
        self.conform(id, ctx);
        Ok(())
    }

    fn edit(
        &mut self,
        work: &Name,
        rev: Option<Pos>,
        change: &Change<'_>,
        ctx: &mut Ctx,
    ) -> Result<(), OpRefusal> {
        self.live()?;
        let id = self.resolve(work, "work", ctx)?;
        self.ensure_open(id, "work", ctx)?;
        self.guard(id, rev, ctx)?;
        let Change {
            data,
            set,
            delete,
            schema,
            unbind,
            control,
            treatment,
        } = *change;
        if data.is_none()
            && set.is_none()
            && delete.is_none()
            && schema.is_none()
            && unbind.is_none()
            && control.is_none()
            && treatment.is_none()
        {
            return Err(refuse(Fault::Malformed, "op", "the edit names no change"));
        }
        if data.is_some() && (set.is_some() || delete.is_some()) {
            return Err(refuse(
                Fault::Malformed,
                "data",
                "replaces the data whole; it excludes set and delete",
            ));
        }
        if unbind == Some(false) || (unbind.is_some() && schema.is_some()) {
            return Err(refuse(
                Fault::Malformed,
                "unbind",
                "only unbind: true, without schema, removes the binding",
            ));
        }
        let subject = self.work[&id].name.to_string();
        let replacement = data.map(|d| self.data(Some(d), "data", ctx)).transpose()?;
        let mut edits = Vec::new();
        for (i, item) in set.into_iter().flatten().enumerate() {
            edits.push((
                item.path.clone(),
                self.value(&item.value, &format!("set[{i}].value"), ctx)?,
            ));
        }
        let deletions: Vec<Path> = delete.map(<[Vec<String>]>::to_vec).unwrap_or_default();
        let is_trial = self.work[&id].trial().is_some();
        if !is_trial && (control.is_some() || treatment.is_some()) {
            return Err(refuse(
                Fault::Malformed,
                if control.is_some() {
                    "control"
                } else {
                    "treatment"
                },
                "applies to trials",
            ));
        }
        let regime = self.work[&id].trial().and_then(|t| t.regime);
        let control = match control {
            Some(slug) => self.control(Some(slug), regime, &subject, ctx)?,
            None => None,
        };
        let treatment = self.treatment(treatment, &subject, ctx)?;
        let binding = if unbind == Some(true) {
            Some(None)
        } else {
            schema.map(|s| self.bind(Some(s), &subject, ctx))
        };
        let moment = ctx.moment();
        let changed = |current: &Data| -> Result<Data, OpRefusal> {
            let mut next = replacement.clone().unwrap_or_else(|| current.clone());
            if !edits.is_empty() || !deletions.is_empty() {
                next.edit(edits.clone(), &deletions)
                    .map_err(|e| data_error("set", e))?;
            }
            Ok(next)
        };
        let work = self
            .work
            .get_mut(&id)
            .ok_or_else(|| refuse(Fault::Unknown, "work", "unknown"))?;
        match &mut work.body {
            Body::Iset(iset) => iset.data = changed(&iset.data)?,
            Body::Trial(trial) => {
                let current = trial
                    .plans
                    .last()
                    .cloned()
                    .ok_or_else(|| refuse(Fault::Resolved, "work", "the trial has no plan"))?;
                trial.plans.push(Plan {
                    at: moment,
                    data: changed(&current.data)?,
                    control: control.or(current.control),
                    treatment: treatment.or(current.treatment),
                });
            }
        }
        if let Some(binding) = binding {
            work.binding = binding;
        }
        self.touch(id, ctx);
        self.conform(id, ctx);
        Ok(())
    }

    fn guard(&self, id: Id, rev: Option<Pos>, ctx: &mut Ctx) -> Result<(), OpRefusal> {
        if ctx.fresh.contains(&id) {
            return Ok(());
        }
        let work = &self.work[&id];
        let actual = ctx.pre_rev.get(&id).copied().unwrap_or(work.rev);
        let Some(rev) = rev else {
            return if ctx.legacy {
                Ok(())
            } else {
                Err(refuse(
                    Fault::Guard,
                    "rev",
                    format!("editing {} needs its revision, {actual}", work.name),
                ))
            };
        };
        if let Some(prior) = ctx.guards.insert(id, rev)
            && prior != rev
        {
            return Err(refuse(
                Fault::Guard,
                "rev",
                format!("differs from this patch's earlier guard {prior}"),
            ));
        }
        if rev == actual {
            Ok(())
        } else {
            Err(refuse(
                Fault::Guard,
                "rev",
                format!("{} is at revision {actual}, not {rev}", work.name),
            ))
        }
    }

    fn judge(
        &mut self,
        slug: &Slug,
        answer: &Json,
        seen: Option<Seq>,
        ctx: &mut Ctx,
    ) -> Result<(), OpRefusal> {
        self.live()?;
        let id = self.resolve_iset(slug, "iset", ctx)?;
        self.ensure_open(id, "iset", ctx)?;
        if !answer.is_object() {
            return Err(refuse(Fault::Malformed, "answer", "an answer is an object"));
        }
        let answer = self.value(answer, "answer", ctx)?;
        let reach = self.reach(id);
        if let Some(seen) = seen
            && !ctx.legacy
        {
            self.seen(id, seen, ctx.dep, &reach, ctx)?;
        }
        let consumes: BTreeSet<Id> = reach.iter().copied().filter(|m| self.done(*m)).collect();
        let passed = reach.difference(&consumes).copied().collect();
        let work = &self.work[&id];
        let open = work
            .iset()
            .is_some_and(|i| i.members.iter().any(|m| self.is_open(*m)));
        if open || work.inputs.iter().any(|input| !self.done(*input)) {
            ctx.warn(
                work.name.to_string(),
                "judged with an open member or an input not done",
            );
        }
        let judgment = Judgment {
            at: ctx.moment(),
            answer,
            seen,
            consumes,
            passed,
        };
        if let Some(Body::Iset(iset)) = self.work.get_mut(&id).map(|w| &mut w.body)
            && let Some(current) = iset.judgment.replace(judgment)
        {
            iset.superseded.push(current);
        }
        self.touch(id, ctx);
        self.conform(id, ctx);
        ctx.log.judged.push(id);
        Ok(())
    }

    /// Refuse a guarded judgment when another patch after `seen` changed what it rests on.
    fn seen(
        &self,
        iset: Id,
        seen: Seq,
        resolved: Seq,
        reach: &BTreeSet<Id>,
        ctx: &Ctx,
    ) -> Result<(), OpRefusal> {
        let here = ctx.pos.seq;
        let late = |seq: Seq| seq > seen && seq < here;
        let mut changed = Vec::new();
        if late(resolved) {
            changed.push(iset);
        }
        for id in std::iter::once(iset).chain(reach.iter().copied()) {
            let Some(work) = self.work.get(&id) else {
                continue;
            };
            let before = ctx.pre_rev.get(&id).copied().unwrap_or(work.rev);
            let implicit = if work.rev.seq == here {
                ctx.implicit.get(&id).copied().unwrap_or_default()
            } else {
                0
            };
            if (late(before.seq) || late(implicit)) && !changed.contains(&id) {
                changed.push(id);
            }
        }
        if changed.is_empty() {
            return Ok(());
        }
        let names: Vec<String> = changed.iter().filter_map(|id| self.name_of(*id)).collect();
        Err(refuse(
            Fault::Guard,
            "seen",
            format!("changed after seq {seen}: {}", names.join(", ")),
        ))
    }

    fn void(&mut self, target: &Target, reason: &str, ctx: &mut Ctx) -> Result<(), OpRefusal> {
        let reason = required(reason, "reason")?;
        let void = Some(Void {
            at: ctx.moment(),
            reason,
        });
        let id = match target {
            Target::Work(name) => {
                let id = self.resolve(name, "work", ctx)?;
                let work = self
                    .work
                    .get_mut(&id)
                    .ok_or_else(|| refuse(Fault::Unknown, "work", "unknown"))?;
                if work.void.is_some() {
                    return Err(refuse(
                        Fault::Void,
                        "work",
                        format!("{} is already void", work.name),
                    ));
                }
                work.void = void;
                self.touch(id, ctx);
                id
            }
            Target::Baseline(slug) => {
                let id = self.resolve_baseline(slug, "baseline", ctx)?;
                let elected = self.elections.iter().any(|e| e.baseline == id);
                let baseline = self
                    .baselines
                    .get_mut(&id)
                    .ok_or_else(|| refuse(Fault::Unknown, "baseline", "unknown"))?;
                if baseline.void.is_some() {
                    return Err(refuse(
                        Fault::Void,
                        "baseline",
                        format!("{slug} is already void"),
                    ));
                }
                baseline.void = void;
                baseline.rev = ctx.pos;
                if elected {
                    self.incumbency = Some(ctx.pos);
                }
                id
            }
        };
        ctx.log.voided.push(id);
        Ok(())
    }

    fn baseline(
        &mut self,
        slug: &Slug,
        artifacts: Option<&[String]>,
        readings: Option<&Readings>,
        note: Option<&str>,
        ctx: &mut Ctx,
    ) -> Result<(), OpRefusal> {
        self.live()?;
        let note = note.map(|n| required(n, "note")).transpose()?;
        let empty = Readings::new();
        let Some(bound) = self.baseline_names.get(slug).copied() else {
            let regime = self.regime_now(ctx);
            let readings = self.readings(readings.unwrap_or(&empty), regime, slug.as_str(), ctx)?;
            let id = self.alloc()?;
            self.baseline_names
                .insert(slug.clone(), Bound { id, since: ctx.pos });
            self.baselines.insert(
                id,
                Baseline {
                    slug: slug.clone(),
                    created: ctx.moment(),
                    rev: ctx.pos,
                    regime,
                    artifacts: artifacts.map(<[String]>::to_vec).unwrap_or_default(),
                    readings,
                    note,
                    identity_frozen: None,
                    readings_frozen: None,
                    void: None,
                },
            );
            ctx.trace.created.push(id);
            ctx.log.created.push(Created::Baseline(id));
            return Ok(());
        };
        let id = bound.id;
        ctx.depend(bound.since.seq);
        let current = &self.baselines[&id];
        if current.void.is_some() && !ctx.legacy {
            return Err(refuse(Fault::Void, "slug", format!("{slug} is void")));
        }
        let regime = current.regime;
        let mut merged = current.readings.clone();
        merged.extend(self.readings(readings.unwrap_or(&empty), regime, slug.as_str(), ctx)?);
        let current = &self.baselines[&id];
        if !ctx.legacy {
            if let (Some(at), Some(artifacts)) = (current.identity_frozen, artifacts)
                && artifacts != current.artifacts.as_slice()
            {
                return Err(refuse(
                    Fault::Frozen,
                    "artifacts",
                    format!("{slug}'s identity froze at {at}"),
                ));
            }
            if let Some(at) = current.readings_frozen
                && merged != current.readings
            {
                return Err(refuse(
                    Fault::Frozen,
                    "readings",
                    format!("{slug}'s readings froze at {at}"),
                ));
            }
        }
        let pos = ctx.pos;
        if let Some(baseline) = self.baselines.get_mut(&id) {
            if let Some(artifacts) = artifacts {
                baseline.artifacts = artifacts.to_vec();
            }
            baseline.readings = merged;
            if note.is_some() {
                baseline.note = note;
            }
            baseline.rev = pos;
        }
        Ok(())
    }

    fn elect(
        &mut self,
        id: Id,
        rationale: String,
        adopts: Option<&[Slug]>,
        ctx: &mut Ctx,
    ) -> Result<(), OpRefusal> {
        let baseline = &self.baselines[&id];
        let slug = baseline.slug.clone();
        if baseline.void.is_some() {
            return Err(refuse(
                Fault::Election,
                "baseline",
                format!("{slug} is void"),
            ));
        }
        let current = self.current_regime().map(|r| r.number);
        if baseline.regime != current {
            return Err(refuse(
                Fault::Election,
                "baseline",
                format!("{slug} is not of the current regime"),
            ));
        }
        let mut adopted = BTreeMap::new();
        let mut consumes = BTreeSet::from([id]);
        for (i, iset) in adopts.into_iter().flatten().enumerate() {
            let iset = self.resolve_iset(iset, &format!("adopts[{i}]"), ctx)?;
            let work = &self.work[&iset];
            let judged = work
                .iset()
                .and_then(|i| i.judgment.as_ref())
                .map(|j| j.at.pos);
            if judged.is_some() || work.void.is_some() {
                consumes.insert(iset);
            }
            if judged.is_none() && work.void.is_none() {
                ctx.warn(slug.to_string(), format!("adopts unjudged {}", work.name));
            }
            if work.void.is_some() {
                ctx.warn(slug.to_string(), format!("adopts void {}", work.name));
            }
            adopted.insert(iset, judged);
        }
        self.freeze(id, true, ctx.pos);
        self.elections.push(Election {
            at: ctx.moment(),
            baseline: id,
            regime: current,
            rationale,
            adopts: adopted,
            consumes,
        });
        self.incumbency = Some(ctx.pos);
        ctx.log
            .created
            .push(Created::Election(self.elections.len() - 1));
        Ok(())
    }

    fn remove(&mut self, target: &Target, ctx: &mut Ctx) -> Result<(), OpRefusal> {
        match target {
            Target::Work(name) => {
                let id = self.resolve(name, "work", ctx)?;
                for iset in self.containers(id) {
                    if let Some(Body::Iset(i)) = self.work.get_mut(&iset).map(|w| &mut w.body) {
                        i.members.remove(&id);
                    }
                    self.touch(iset, ctx);
                }
                for work in self.waiting_on(id) {
                    if let Some(w) = self.work.get_mut(&work) {
                        w.inputs.remove(&id);
                    }
                    self.touch(work, ctx);
                }
                if let Some(work) = self.work.remove(&id) {
                    self.names.remove(&work.name);
                    self.removed.insert(
                        id,
                        Removed {
                            kind: work.kind(),
                            at: ctx.pos,
                        },
                    );
                }
            }
            Target::Baseline(slug) => {
                let id = self.resolve_baseline(slug, "baseline", ctx)?;
                self.baseline_names.remove(slug);
                self.baselines.remove(&id);
                self.removed.insert(
                    id,
                    Removed {
                        kind: Kind::Baseline,
                        at: ctx.pos,
                    },
                );
                if self.elections.iter().any(|e| e.baseline == id) {
                    self.incumbency = Some(ctx.pos);
                }
            }
        }
        Ok(())
    }

    fn rename(&mut self, target: &Target, to: &Name, ctx: &mut Ctx) -> Result<(), OpRefusal> {
        match target {
            Target::Work(name) => {
                let id = self.resolve(name, "work", ctx)?;
                self.free(to, "to")?;
                if self.work[&id].iset().is_some() && to.as_slug().is_none() {
                    return Err(refuse(Fault::Malformed, "to", "an iset's name is a slug"));
                }
                self.names.remove(name);
                self.names.insert(to.clone(), Bound { id, since: ctx.pos });
                if let Some(work) = self.work.get_mut(&id) {
                    work.name = to.clone();
                }
            }
            Target::Baseline(slug) => {
                let id = self.resolve_baseline(slug, "baseline", ctx)?;
                let to = to
                    .as_slug()
                    .ok_or_else(|| refuse(Fault::Malformed, "to", "a baseline's name is a slug"))?;
                if self.baseline_names.contains_key(to) {
                    return Err(refuse(
                        Fault::Collision,
                        "to",
                        format!("{to} names an existing baseline"),
                    ));
                }
                self.baseline_names.remove(slug);
                self.baseline_names
                    .insert(to.clone(), Bound { id, since: ctx.pos });
                if let Some(baseline) = self.baselines.get_mut(&id) {
                    baseline.slug = to.clone();
                }
            }
        }
        Ok(())
    }

    fn define(
        &mut self,
        schema: &Slug,
        definition: &Json,
        rev: Option<Pos>,
        ctx: &mut Ctx,
    ) -> Result<(), OpRefusal> {
        self.live()?;
        let parsed = Schema::parse(definition).map_err(|message| {
            let (path, message) = message.split_once(": ").unwrap_or(("definition", &message));
            refuse(Fault::Grammar, path, message)
        })?;
        let definitions = self.schemas.get(schema);
        let current = definitions.and_then(|d| d.last()).map(|d| d.at.pos);
        let fresh = definitions
            .and_then(|d| d.first())
            .is_some_and(|d| d.at.pos.seq == ctx.pos.seq);
        match (current, rev) {
            (Some(current), rev) if !fresh => {
                let before = *ctx.pre_def.entry(schema.clone()).or_insert(current);
                match rev {
                    None if !ctx.legacy => {
                        return Err(refuse(
                            Fault::Guard,
                            "rev",
                            format!("redefining {schema} needs its current revision, {before}"),
                        ));
                    }
                    Some(rev) if rev != before => {
                        return Err(refuse(
                            Fault::Guard,
                            "rev",
                            format!("{schema} is at revision {before}, not {rev}"),
                        ));
                    }
                    _ => {}
                }
                if self.work.values().any(|w| match &w.binding {
                    Some(Binding::Exact { id, .. } | Binding::Requested { id, .. }) => id == schema,
                    None => false,
                }) {
                    ctx.warn(
                        schema.to_string(),
                        "redefines a bound schema id; bound work keeps its definition",
                    );
                }
            }
            (None, Some(_)) => {
                return Err(refuse(
                    Fault::Guard,
                    "rev",
                    format!("{schema} has no definition to guard"),
                ));
            }
            _ => {}
        }
        self.schemas
            .entry(schema.clone())
            .or_default()
            .push(Definition {
                at: ctx.moment(),
                schema: parsed,
            });
        ctx.log.created.push(Created::Definition(schema.clone()));
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
            return Err(refuse(
                Fault::Malformed,
                format!("metrics.{}", spec.key),
                "a KPI needs a direction",
            ));
        }
        if metric.refs.values().any(|v| !v.is_finite()) {
            return Err(refuse(
                Fault::Malformed,
                format!("metrics.{}.refs", spec.key),
                "reference lines are finite",
            ));
        }
        Ok(metric)
    }

    fn gauge_size(&self, ctx: &mut Ctx) {
        if let Some(regime) = self.regimes.last() {
            let count = regime.gauge.metrics.values().filter(|m| !m.retired).count();
            if count > 8 {
                ctx.warn(
                    format!("regime {}", regime.number),
                    format!("{count} unretired metrics; more than eight"),
                );
            }
        }
    }

    fn has_reading(&self, regime: RegimeNumber, key: &MetricKey) -> bool {
        self.baselines
            .values()
            .any(|b| b.regime == Some(regime) && b.readings.contains_key(key))
            || self.work.values().filter_map(Work::trial).any(|t| {
                t.regime == Some(regime)
                    && t.report.as_ref().is_some_and(|r| {
                        r.readings.contains_key(key)
                            || r.control_readings
                                .as_ref()
                                .is_some_and(|c| c.contains_key(key))
                    })
            })
    }

    fn regime_change(
        &mut self,
        contract: &str,
        metrics: &[MetricSpec],
        reason: Option<&str>,
        bridge: Option<&Bridge>,
        ctx: &mut Ctx,
    ) -> Result<(), OpRefusal> {
        self.live()?;
        let contract = required(contract, "contract")?;
        let reason = reason.map(|r| required(r, "reason")).transpose()?;
        if !self.regimes.is_empty() && reason.is_none() {
            return Err(refuse(
                Fault::Malformed,
                "reason",
                "a regime change after the first needs a reason",
            ));
        }
        let outgoing = self.incumbent().map(|(_, b)| b);
        if let Some(bridge) = bridge {
            if outgoing.is_none() {
                return Err(refuse(
                    Fault::Bridge,
                    "bridge",
                    "a bridge needs an outgoing incumbent baseline",
                ));
            }
            if self.baseline_names.contains_key(&bridge.slug) {
                return Err(refuse(
                    Fault::Collision,
                    "bridge.slug",
                    format!("{} names an existing baseline", bridge.slug),
                ));
            }
        }
        let mut gauge = Gauge {
            contract,
            metrics: BTreeMap::new(),
        };
        for spec in metrics {
            if gauge.metrics.contains_key(&spec.key) {
                return Err(refuse(
                    Fault::Malformed,
                    format!("metrics.{}", spec.key),
                    "duplicate metric",
                ));
            }
            gauge
                .metrics
                .insert(spec.key.clone(), Self::metric(spec, None)?);
        }
        let number = self
            .current_regime()
            .map_or(Some(RegimeNumber::FIRST), |r| r.number.next())
            .ok_or_else(|| refuse(Fault::Malformed, "op", "regimes are exhausted"))?;
        self.regimes.push(Regime {
            number,
            gauge,
            reason: reason.clone(),
            bridge: None,
            at: ctx.moment(),
        });
        self.incumbency = Some(ctx.pos);
        self.gauge_size(ctx);
        ctx.log.created.push(Created::Regime(number));
        if let (Some(bridge), Some(outgoing)) = (bridge, outgoing) {
            let artifacts = self.baselines[&outgoing].artifacts.clone();
            let readings =
                self.readings(&bridge.readings, Some(number), bridge.slug.as_str(), ctx)?;
            let note = bridge
                .note
                .as_deref()
                .map(|n| required(n, "bridge.note"))
                .transpose()?;
            let id = self.alloc()?;
            self.baseline_names
                .insert(bridge.slug.clone(), Bound { id, since: ctx.pos });
            self.baselines.insert(
                id,
                Baseline {
                    slug: bridge.slug.clone(),
                    created: ctx.moment(),
                    rev: ctx.pos,
                    regime: Some(number),
                    artifacts,
                    readings,
                    note,
                    identity_frozen: None,
                    readings_frozen: None,
                    void: None,
                },
            );
            ctx.log.created.push(Created::Baseline(id));
            if let Some(regime) = self.regimes.last_mut() {
                regime.bridge = Some(id);
            }
            self.elect(id, reason.unwrap_or_default(), None, ctx)?;
        }
        Ok(())
    }

    fn gauge(
        &mut self,
        contract: Option<&str>,
        metrics: Option<&[MetricSpec]>,
        retire: Option<&[MetricKey]>,
        restore: Option<&[MetricKey]>,
        ctx: &mut Ctx,
    ) -> Result<(), OpRefusal> {
        self.live()?;
        let number = self
            .current_regime()
            .ok_or_else(|| refuse(Fault::Unknown, "op", "no regime; install one with regime"))?
            .number;
        let index = number.index();
        if let Some(contract) = contract {
            let contract = required(contract, "contract")?;
            if self.regimes[index].gauge.contract != contract {
                ctx.warn(
                    format!("regime {number}"),
                    "contract edited; only the caller can judge comparability",
                );
            }
            self.regimes[index].gauge.contract = contract;
        }
        for spec in metrics.into_iter().flatten() {
            let previous = self.regimes[index].gauge.metrics.get(&spec.key);
            let metric = Self::metric(spec, previous)?;
            if previous.is_some_and(|old| {
                (old.unit != metric.unit || old.better != metric.better)
                    && self.has_reading(number, &spec.key)
            }) {
                return Err(refuse(
                    Fault::Meaning,
                    format!("metrics.{}", spec.key),
                    "a metric's unit and direction change only before its first reading; change regime",
                ));
            }
            self.regimes[index]
                .gauge
                .metrics
                .insert(spec.key.clone(), metric);
        }
        for (keys, retired, field) in [(retire, true, "retire"), (restore, false, "restore")] {
            for key in keys.into_iter().flatten() {
                self.regimes[index]
                    .gauge
                    .metrics
                    .get_mut(key)
                    .ok_or_else(|| refuse(Fault::Unknown, field, format!("no metric {key}")))?
                    .retired = retired;
            }
        }
        self.gauge_size(ctx);
        Ok(())
    }
}

/// A report's written observations.
#[derive(Clone, Copy)]
struct Written<'a> {
    readings: Option<&'a Readings>,
    control_readings: Option<&'a Readings>,
    artifacts: Option<&'a [String]>,
    data: Option<&'a Json>,
}

/// An edit's written changes.
#[derive(Clone, Copy)]
struct Change<'a> {
    data: Option<&'a Json>,
    set: Option<&'a [SetPath]>,
    delete: Option<&'a [Vec<String>]>,
    schema: Option<&'a SchemaRef>,
    unbind: Option<bool>,
    control: Option<&'a Slug>,
    treatment: Option<&'a TreatmentSpec>,
}
