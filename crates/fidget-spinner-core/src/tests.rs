//! Acceptance witnesses for the ledger's law: finality, void, guards, data,
//! structure, supervision and annotation, and the FS 3 migration. Every scenario ends by proving that a
//! replay from nothing reproduces the incrementally patched state.

use std::error::Error;

use serde_json::{Value, json};
use time::{Duration, OffsetDateTime};

use crate::*;

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

fn stamp(seq: Seq) -> Stamp {
    Stamp {
        seq,
        time: OffsetDateTime::UNIX_EPOCH + Duration::minutes(seq),
    }
}

fn ops(patch: Value) -> Result<Vec<Op>> {
    let Value::Array(items) = patch else {
        return Err("a patch is an array".into());
    };
    Ok(items
        .into_iter()
        .map(Op::parse)
        .collect::<std::result::Result<_, _>>()?)
}

/// A frontier and the patches committed to it.
#[derive(Default)]
struct Ledger {
    frontier: Frontier,
    patches: Vec<(Vec<Op>, Stamp)>,
}

impl Ledger {
    fn next(&self) -> Result<Stamp> {
        Ok(stamp(Seq::try_from(self.patches.len())? + 1))
    }
    fn commit(&mut self, patch: Value) -> Result<Patched> {
        let ops = ops(patch)?;
        let stamp = self.next()?;
        let patched = self.frontier.patched(&ops, stamp)?;
        self.frontier = patched.frontier.clone();
        self.patches.push((ops, stamp));
        Ok(patched)
    }
    fn refuse(&self, patch: Value) -> Result<Refusal> {
        match self.frontier.patched(&ops(patch)?, self.next()?) {
            Ok(_) => Err("the patch was accepted".into()),
            Err(refusal) => Ok(refusal),
        }
    }
    fn fault(&self, patch: Value) -> Result<Fault> {
        Ok(self.refuse(patch)?.refusal.fault)
    }
    fn id(&self, name: &str) -> Result<Id> {
        Ok(self
            .frontier
            .named(&name.parse()?)
            .ok_or_else(|| format!("no work {name}"))?
            .0)
    }
    fn rev(&self, name: &str) -> Result<String> {
        Ok(self.frontier.work()[&self.id(name)?].rev.to_string())
    }
    fn judgment(&self, name: &str) -> Result<&Judgment> {
        Ok(self.frontier.work()[&self.id(name)?]
            .iset()
            .and_then(|i| i.judgment.as_ref())
            .ok_or_else(|| format!("{name} is unjudged"))?)
    }
    fn replays(&self) -> Result {
        let mut replayed = Frontier::default();
        for (ops, stamp) in &self.patches {
            replayed.replay(ops, *stamp)?;
        }
        assert_eq!(format!("{replayed:?}"), format!("{:?}", self.frontier));
        Ok(())
    }
}

#[test]
fn resolution_is_final() -> Result {
    let mut l = Ledger::default();
    l.commit(json!([
        {"op":"frontier","objective":"o"},
        {"op":"regime","contract":"c","metrics":[{"key":"elapsed","unit":"ms","better":"lower","kpi":true}]},
        {"op":"baseline","slug":"b0","readings":{"elapsed":10}},
        {"op":"elect","baseline":"b0","rationale":"start"},
        {"op":"iset","slug":"q"},
        {"op":"plan","into":["q"]}
    ]))?;
    let (q, t) = (l.id("q")?, l.id("q#1")?);
    assert_eq!(l.frontier.state(t), Some(State::Ready));
    assert_eq!(l.frontier.state(q), Some(State::Waiting));
    l.commit(json!([{"op":"report","trial":"q#1","readings":{"elapsed":"8 ms"}}]))?;
    assert_eq!(l.frontier.state(q), Some(State::Tested));
    // The report consumed its defaulted control's readings, which froze.
    assert_eq!(
        l.fault(json!([{"op":"baseline","slug":"b0","readings":{"elapsed":9}}]))?,
        Fault::Frozen
    );
    l.commit(json!([{"op":"judge","iset":"q","answer":{"holds":true}}]))?;
    let rev = l.rev("q")?;
    for write in [
        json!([{"op":"report","trial":"q#1","readings":{"elapsed":7}}]),
        json!([{"op":"judge","iset":"q","answer":{"holds":false}}]),
        json!([{"op":"edit","work":"q","rev":rev,"data":{"x":1}}]),
        json!([{"op":"plan","into":["q"]}]),
    ] {
        assert_eq!(l.fault(write)?, Fault::Resolved);
    }
    // A later answer is a new iset; its judgment stops at resolved work.
    l.commit(json!([
        {"op":"iset","slug":"q2","members":["q"]},
        {"op":"report","trial":"t2","into":["q2"]},
        {"op":"judge","iset":"q2","answer":{"holds":false}}
    ]))?;
    assert_eq!(
        l.judgment("q2")?.consumes,
        [q, l.id("t2")?].into_iter().collect()
    );
    l.replays()
}

#[test]
fn void_marks_one_hop_and_stays() -> Result {
    let mut l = Ledger::default();
    l.commit(json!([
        {"op":"frontier","objective":"o"},
        {"op":"iset","slug":"a"},
        {"op":"report","trial":"t","into":["a"]},
        {"op":"judge","iset":"a","answer":{}},
        {"op":"iset","slug":"b","members":["a"]},
        {"op":"judge","iset":"b","answer":{}}
    ]))?;
    let (a, t) = (l.id("a")?, l.id("t")?);
    let voided = l.commit(json!([{"op":"void","work":"t","reason":"broken harness"}]))?;
    assert_eq!(voided.voided[0].consumers, [Consumer::Judgment(a)]);
    assert!(l.frontier.invalidated(t));
    assert_eq!(l.frontier.void_sources(&l.judgment("a")?.consumes), [t]);
    assert!(
        l.frontier
            .void_sources(&l.judgment("b")?.consumes)
            .is_empty()
    );
    assert_eq!(
        l.fault(json!([{"op":"void","work":"t","reason":"again"}]))?,
        Fault::Void
    );
    // A reassessment over the marked answer leaves its mark in place.
    l.commit(json!([
        {"op":"iset","slug":"again","members":["a"]},
        {"op":"report","trial":"t2","into":["again"]},
        {"op":"judge","iset":"again","answer":{}}
    ]))?;
    assert_eq!(l.frontier.void_sources(&l.judgment("a")?.consumes), [t]);
    // A consumer of an already-void source rests on it from birth.
    l.commit(json!([
        {"op":"baseline","slug":"bv"},
        {"op":"void","baseline":"bv","reason":"misbuilt"}
    ]))?;
    let born = l.commit(json!([{"op":"report","trial":"t3","control":"bv"}]))?;
    assert!(born.warnings.contains_key(&0));
    let bv = l.frontier.baseline_named(&"bv".parse()?).ok_or("no bv")?.0;
    let report = l.frontier.work()[&l.id("t3")?]
        .trial()
        .and_then(|t| t.report.as_ref())
        .ok_or("unreported")?;
    assert_eq!(l.frontier.void_sources(&report.consumes), [bv]);
    // Removing a consumed source marks its consumer.
    l.commit(json!([{"op":"remove","work":"t2"}]))?;
    assert!(l.frontier.removed_source(&l.judgment("again")?.consumes));
    l.replays()
}

#[test]
fn judgment_guard_sees_implicit_resolution() -> Result {
    let mut l = Ledger::default();
    l.commit(json!([
        {"op":"frontier","objective":"o"},
        {"op":"regime","contract":"c","metrics":[{"key":"m","unit":"s"}]},
        {"op":"baseline","slug":"b0"},
        {"op":"elect","baseline":"b0","rationale":"start"},
        {"op":"iset","slug":"q"},
        {"op":"plan","trial":"t","into":["q"]}
    ]))?;
    l.commit(json!([{"op":"report","trial":"t"}]))?;
    l.commit(json!([{"op":"iset","slug":"elsewhere"}]))?;
    l.commit(json!([
        {"op":"baseline","slug":"b1"},
        {"op":"elect","baseline":"b1","rationale":"better"}
    ]))?;
    // A defaulted control resolves to the incumbent elected after seq 2.
    let refusal = l.refuse(json!([
        {"op":"report","trial":"late","into":["q"]},
        {"op":"judge","iset":"q","answer":{},"seen":2}
    ]))?;
    assert_eq!((refusal.index, refusal.refusal.fault), (1, Fault::Guard));
    let rev = l.rev("q")?;
    l.commit(json!([{"op":"edit","work":"q","rev":rev,"data":{"note":"reframed"}}]))?;
    assert_eq!(
        l.fault(json!([{"op":"judge","iset":"q","answer":{},"seen":4}]))?,
        Fault::Guard
    );
    // An explicit control and the unrelated iset leave the guard intact.
    l.commit(json!([
        {"op":"report","trial":"late","into":["q"],"control":"b0"},
        {"op":"judge","iset":"q","answer":{},"seen":5}
    ]))?;
    assert_eq!(l.judgment("q")?.seen, Some(5));
    l.replays()
}

#[test]
fn data_conforms_or_warns() -> Result {
    let mut l = Ledger::default();
    l.commit(json!([{"op":"frontier","objective":"o"}]))?;
    assert_eq!(
        l.fault(json!([{"op":"define","schema":"q","definition":
            {"carrier":"iset","data":{"x":{"type":"text"}},"answer":{}}}]))?,
        Fault::Grammar
    );
    let patched = l.commit(json!([
        {"op":"define","schema":"q","definition":{"carrier":"iset",
            "data":{"question":{"type":"text","presence":"required"}},
            "answer":{"verdict":{"type":{"enum":["holds","fails"]},"presence":"required"}}}},
        {"op":"iset","slug":"good","schema":"q","data":{"question":"why?"}},
        {"op":"iset","slug":"bad","schema":"q","data":{"question":3,"extra":true}},
        {"op":"iset","slug":"pending","schema":"later"},
        {"op":"iset","slug":"cites","data":{"see":{"$fs":"ref","kind":"iset","id":"good"}}}
    ]))?;
    assert!(patched.warnings.contains_key(&2) && !patched.warnings.contains_key(&1));
    let findings = l.frontier.findings();
    assert!(findings.contains(&Finding::Nonconforming(l.id("bad")?)));
    assert!(findings.contains(&Finding::Unvalidated(l.id("pending")?)));
    assert!(!findings.contains(&Finding::Nonconforming(l.id("good")?)));
    let dangling = l.refuse(json!([{"op":"iset","slug":"dangling",
        "data":{"see":{"$fs":"ref","kind":"iset","id":"absent"}}}]))?;
    assert!(dangling.to_string().contains("absent"), "{dangling}");
    // References follow renames.
    l.commit(json!([{"op":"rename","work":"good","to":"better"}]))?;
    let cites = l.frontier.work()[&l.id("cites")?].data().ok_or("no data")?;
    assert_eq!(
        cites.compact(&|r| l.frontier.ref_name(r)),
        r#"{"see":@iset:better}"#
    );
    // Edits change paths simultaneously; overlapping paths refuse.
    let rev = l.rev("cites")?;
    assert_eq!(
        l.fault(json!([{"op":"edit","work":"cites","rev":rev,
            "set":[{"path":["a"],"value":{}},{"path":["a","b"],"value":1}]}]))?,
        Fault::Malformed
    );
    l.commit(json!([{"op":"edit","work":"cites","rev":rev,
        "set":[{"path":["n"],"value":1}],"delete":[["see"]]}]))?;
    assert_eq!(
        l.frontier.work()[&l.id("cites")?]
            .data()
            .ok_or("no data")?
            .compact(&|r| l.frontier.ref_name(r)),
        r#"{"n":1}"#
    );
    // An answer outside the binding is stored, with a warning.
    let judged = l.commit(json!([{"op":"judge","iset":"better","answer":{"verdict":"maybe"}}]))?;
    assert!(judged.warnings.contains_key(&0));
    assert!(
        l.frontier
            .findings()
            .contains(&Finding::Nonconforming(l.id("better")?))
    );
    l.replays()
}

#[test]
fn structure_stays_acyclic() -> Result {
    let mut l = Ledger::default();
    l.commit(json!([
        {"op":"frontier","objective":"o"},
        {"op":"iset","slug":"a"},
        {"op":"iset","slug":"b","into":["a"]},
        {"op":"iset","slug":"c","into":["a"]},
        {"op":"plan","trial":"t","into":["b","c"]}
    ]))?;
    let [a, b, c, t] = [l.id("a")?, l.id("b")?, l.id("c")?, l.id("t")?];
    assert_eq!(l.frontier.below(a), [b, c, t].into_iter().collect());
    assert_eq!(
        l.fault(json!([{"op":"member","iset":"b","add":["a"]}]))?,
        Fault::Cycle
    );
    // Work waiting on an iset that holds it closes a cycle; the refusal names it.
    let refusal = l.refuse(json!([{"op":"after","work":"t","add":["b"]}]))?;
    assert_eq!(refusal.refusal.fault, Fault::Cycle);
    assert!(refusal.refusal.message.contains('b') && refusal.refusal.message.contains('t'));
    // Waiting on one's own member is redundant, not cyclic.
    l.commit(json!([{"op":"after","work":"a","add":["t"]}]))?;
    // A judgment reaches through open isets to done work.
    l.commit(json!([{"op":"report","trial":"t"},{"op":"judge","iset":"a","answer":{}}]))?;
    let judgment = l.judgment("a")?;
    assert_eq!(judgment.consumes, [t].into_iter().collect());
    assert_eq!(judgment.passed, [b, c].into_iter().collect());
    l.replays()
}

#[test]
fn guards_check_the_pre_patch_state() -> Result {
    let mut l = Ledger::default();
    l.commit(json!([
        {"op":"frontier","objective":"o"},
        {"op":"iset","slug":"q","data":{"n":1}}
    ]))?;
    let first = l.rev("q")?;
    assert_eq!(
        l.fault(json!([{"op":"edit","work":"q","data":{"n":2}}]))?,
        Fault::Guard
    );
    l.commit(json!([{"op":"edit","work":"q","rev":first,"data":{"n":2}}]))?;
    assert_eq!(
        l.fault(json!([{"op":"edit","work":"q","rev":first,"data":{"n":3}}]))?,
        Fault::Guard
    );
    // A patch's own writes never invalidate its guards, but they must agree.
    let second = l.rev("q")?;
    l.commit(json!([
        {"op":"edit","work":"q","rev":second,"set":[{"path":["n"],"value":3}]},
        {"op":"edit","work":"q","rev":second,"set":[{"path":["m"],"value":1}]}
    ]))?;
    let third = l.rev("q")?;
    assert_eq!(
        l.fault(json!([
            {"op":"edit","work":"q","rev":third,"set":[{"path":["n"],"value":4}]},
            {"op":"edit","work":"q","rev":second,"set":[{"path":["m"],"value":2}]}
        ]))?,
        Fault::Guard
    );
    // Work created earlier in the patch needs no guard.
    l.commit(json!([
        {"op":"iset","slug":"r"},
        {"op":"edit","work":"r","data":{"fresh":true}}
    ]))?;
    assert_eq!(l.fault(json!([{"op":"guard","head":1}]))?, Fault::Guard);
    l.commit(json!([{"op":"guard","head":4},{"op":"iset","slug":"s"}]))?;
    assert!(Op::parse(json!({"op":"guard","head":9_007_199_254_740_992_u64})).is_err());
    l.replays()
}

#[test]
fn fs3_history_replays_under_legacy_law() -> Result {
    let history = [
        json!([
            {"op":"frontier","objective":"legacy"},
            {"op":"regime","contract":"c","metrics":[{"key":"elapsed","unit":"ms","better":"lower","kpi":true}]},
            {"op":"baseline","slug":"b0","readings":{"elapsed":10}},
            {"op":"accept","baseline":"b0","rationale":"start"},
            {"op":"hypothesis","slug":"h","title":"H","claim":"c"},
            {"op":"hypothesis","slug":"g","title":"G"},
            {"op":"edge","from":"h","to":"g","carries":"why"}
        ]),
        json!([
            {"op":"trial","hypothesis":"h","note":"first","readings":{"elapsed":8}},
            {"op":"accept","hypothesis":"h","rationale":"faster"}
        ]),
        json!([
            {"op":"trial","hypothesis":"h","note":"second","readings":{"elapsed":9}},
            {"op":"reject","hypothesis":"h","rationale":"regressed"}
        ]),
        json!([
            {"op":"review","slug":"r","title":"R","charter":"c","members":["h","g"]},
            {"op":"lesson","review":"r","text":"learned"},
            {"op":"reopen","hypothesis":"h"},
            {"op":"void","trial":"h#2","reason":"bad"}
        ]),
        json!([
            {"op":"baseline","slug":"b1","readings":{"elapsed":7},"adopts":["h"]},
            {"op":"accept","baseline":"b1","rationale":"better"}
        ]),
    ];
    let mut translator = legacy::Translator::new(5);
    let mut l = Ledger::default();
    for patch in history {
        let Value::Array(raw) = patch else {
            return Err("a patch is an array".into());
        };
        let ops = ops(Value::Array(translator.patch(&raw)?))?;
        let stamp = l.next()?;
        l.frontier.replay(&ops, stamp)?;
        l.patches.push((ops, stamp));
    }
    let f = &l.frontier;
    let (h, g) = (l.id("h")?, l.id("g")?);
    let hypothesis = f.work()[&h].iset().ok_or("h is no iset")?;
    assert!(hypothesis.judgment.is_none());
    assert_eq!(hypothesis.prior.len(), 2);
    assert!(f.is_void(l.id("h#2")?) && !f.is_void(l.id("h#1")?));
    assert!(f.work()[&g].inputs.contains(&h));
    assert_eq!(
        f.work()[&g].data().and_then(|d| d.text("title")).as_deref(),
        Some("G")
    );
    assert_eq!(
        l.judgment("r")?.answer.text("lesson").as_deref(),
        Some("learned")
    );
    assert!(
        f.elections()
            .last()
            .is_some_and(|e| e.adopts.contains_key(&h))
    );
    // Current law follows the migrated history.
    l.commit(
        json!([{"op":"judge","iset":"g","answer":{"disposition":"accepted","rationale":"r"}}]),
    )?;
    assert_eq!(
        l.fault(
            json!([{"op":"judge","iset":"g","answer":{"disposition":"rejected","rationale":"r"}}])
        )?,
        Fault::Resolved
    );
    assert!(bar_reserved(&ops(json!([{"op":"reopen","iset":"g"}]))?).is_err());
    l.replays()
}

/// Only a change to what work asks stales its assignment. Annotations, which any
/// record takes, resolved, void or concluded, change no revision and trip no guard.
#[test]
fn only_specification_stales_an_assignment() -> Result {
    let stale = |ledger: &Ledger| -> Vec<Finding> {
        ledger
            .frontier
            .findings()
            .into_iter()
            .filter(|f| matches!(f, Finding::Stale(_)))
            .collect()
    };
    let mut ledger = Ledger::default();
    ledger.commit(json!([
        {"op":"frontier","objective":"o"},
        {"op":"iset","slug":"q"},
        {"op":"plan","trial":"t","into":["q"]},
        {"op":"plan","trial":"u"},
        {"op":"assign","work":"q","owner":"w"},
        {"op":"assign","work":"t","owner":"w"}
    ]))?;
    let rev = ledger.rev("t")?;
    ledger.commit(json!([
        {"op":"member","iset":"q","add":["u"]},
        {"op":"annotate","work":"t","text":"Blocked on the cache fix."},
        {"op":"start","work":"t"}
    ]))?;
    assert!(stale(&ledger).is_empty());
    assert_eq!(ledger.rev("t")?, rev);
    ledger.commit(json!([{"op":"edit","work":"t","rev":rev,"data":{"change":"narrower"}}]))?;
    assert_eq!(stale(&ledger), [Finding::Stale(ledger.id("t")?)]);
    ledger.commit(json!([{"op":"report","trial":"t"},{"op":"report","trial":"u"}]))?;
    ledger.commit(json!([{"op":"annotate","work":"t","text":"Second run only."}]))?;
    ledger.commit(json!([{"op":"judge","iset":"q","answer":{},"seen":4}]))?;
    let long = vec!["word"; 101].join(" ");
    assert_eq!(
        ledger.fault(json!([{"op":"annotate","work":"q","text":long}]))?,
        Fault::Malformed
    );
    ledger.commit(json!([
        {"op":"void","work":"u","reason":"flaky harness"},
        {"op":"conclude","lesson":"l"},
        {"op":"annotate","work":"u","text":"Harness fixed in b2."}
    ]))?;
    assert_eq!(
        ledger.fault(json!([{"op":"plan","trial":"v"}]))?,
        Fault::Concluded
    );
    ledger.replays()
}
