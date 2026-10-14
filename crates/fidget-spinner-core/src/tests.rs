use crate::*;
use std::error::Error;
use time::OffsetDateTime;

fn patch(frontier: &mut Frontier, json: &str) -> Result<Patched, Box<dyn Error>> {
    let ops = serde_json::from_str::<Vec<Op>>(json)?;
    assert_eq!(
        serde_json::from_str::<Vec<Op>>(&serde_json::to_string(&ops)?)?,
        ops
    );
    let patched = frontier.patched(&ops, OffsetDateTime::UNIX_EPOCH)?;
    *frontier = patched.frontier.clone();
    Ok(patched)
}

#[test]
fn unit_conversion_table() -> Result<(), Box<dyn Error>> {
    let rows = [
        ("ns", "s", 1e-9),
        ("µs", "s", 1e-6),
        ("us", "µs", 1.0),
        ("ms", "s", 1e-3),
        ("s", "s", 1.0),
        ("min", "s", 60.0),
        ("h", "s", 3600.0),
        ("B", "B", 1.0),
        ("kB", "B", 1e3),
        ("MB", "B", 1e6),
        ("GB", "B", 1e9),
        ("TB", "B", 1e12),
        ("KiB", "B", 1024.0),
        ("MiB", "B", 1_048_576.0),
        ("GiB", "B", 1_073_741_824.0),
        ("TiB", "B", 1_099_511_627_776.0),
        ("%", "1", 0.01),
        ("1", "%", 100.0),
        ("cases", "cases", 1.0),
    ];
    for (from, to, expected) in rows {
        let (from, to) = (from.parse::<Unit>()?, to.parse::<Unit>()?);
        let actual = from.convert(1.0, &to)?;
        assert!((actual - expected).abs() <= expected * 1e-12);
        assert!((to.convert(actual, &from)? - 1.0).abs() < 1e-12);
    }
    for (from, to) in [("cases", "items"), ("ms", "MB"), ("%", "cases")] {
        assert!(from.parse::<Unit>()?.convert(1.0, &to.parse()?).is_err());
    }
    for invalid in ["", "MS", "case_count", "2", "mb/s"] {
        assert!(invalid.parse::<Unit>().is_err());
    }
    Ok(())
}

#[test]
fn inquiry_across_epochs_and_regimes() -> Result<(), Box<dyn Error>> {
    let mut f = Frontier::default();
    let _ = patch(
        &mut f,
        r#"[
        {"op":"frontier","objective":"Reduce latency"},
        {"op":"regime","contract":"Fixed machine and cohort","metrics":[{"key":"latency","unit":"ms","better":"lower","kpi":true}]},
        {"op":"hypothesis","slug":"a","title":"Claim without evidence"},
        {"op":"hypothesis","slug":"b","title":"Measured change","after":["a"]},
        {"op":"hypothesis","slug":"c","title":"Future work"},
        {"op":"review","slug":"audit","title":"Audit","charter":"Which changes help?","members":["a","b"]},
        {"op":"baseline","slug":"old","readings":{"latency":100},"artifacts":["commit:old"],"docket":["b","c"]},
        {"op":"accept","baseline":"old","rationale":"Reference"},
        {"op":"accept","hypothesis":"a","rationale":"Asserted"},
        {"op":"trial","hypothesis":"b","note":"Measured","readings":{"latency":"0.05 s"},"against":null,"artifacts":null},
        {"op":"accept","hypothesis":"b","rationale":"Twice as fast"}
    ]"#,
    )?;
    let assay = Assay::new(&f);
    let comparison = &assay.comparisons[&"b#1".parse()?][&"latency".parse()?];
    assert_eq!(comparison.movement, Some(Movement::Better));
    assert!(comparison.ratio.is_some_and(|r| (r - 0.5).abs() < 1e-12));
    assert_eq!(assay.reviews[&"audit".parse()?], ReviewState::Ready);
    let receipt = patch(
        &mut f,
        r#"[
        {"op":"baseline","slug":"next","readings":{"latency":40}},
        {"op":"accept","baseline":"next","rationale":"Measured improvement"},
        {"op":"reject","baseline":"next","rationale":"Artifacts were wrong"}
    ]"#,
    )?;
    assert!(receipt.warnings[&1].iter().any(|w| matches!(w, Warning::DocketMoved { hypotheses, .. } if hypotheses.len() == 1 && hypotheses[0].as_str() == "c")));
    assert_eq!(f.incumbent_baseline(), Some(&"old".parse()?));
    assert_eq!(f.epochs().len(), 2);
    assert_eq!(f.epochs()[1].number.get(), 2);
    assert_eq!(
        f.hypotheses()[&"c".parse()?].docket.as_ref(),
        Some(&"next".parse()?)
    );
    assert!(
        Assay::new(&f).dockets[&"next".parse()?]
            .members
            .contains(&"c".parse()?)
    );
    let receipt = patch(
        &mut f,
        r#"[
        {"op":"docket","baseline":"old","add":["c"]},
        {"op":"regime","contract":"New protocol","reason":"Measure a new cohort","metrics":[{"key":"latency","unit":"s","better":"lower","kpi":true}],"bridge":{"slug":"bridge","readings":{"latency":"100 ms"}}},
        {"op":"trial","hypothesis":"c","against":"old","note":"Cross-regime evidence"},
        {"op":"regime","contract":"Third protocol","reason":"Changed envelope","metrics":[{"key":"latency","unit":"ms","better":"lower","kpi":true}]},
        {"op":"baseline","slug":"third","adopts":["c"]},
        {"op":"accept","baseline":"third","rationale":"New reference"},
        {"op":"trial","hypothesis":"c","note":"First reading","readings":{"latncy":"10 ms"}},
        {"op":"trial","hypothesis":"c","note":"Second reading","readings":{"latncy":10}},
        {"op":"trial","hypothesis":"c","note":"Third reading","readings":{"latncy":10}},
        {"op":"waive","finding":"unbacked/a","note":"Known assumption"},
        {"op":"lesson","review":"audit","text":"The measured change helped"},
        {"op":"conclude","lesson":"Protocol changes prevent a single comparison"}
    ]"#,
    )?;
    assert!(
        receipt.warnings[&1]
            .iter()
            .any(|w| matches!(w, Warning::DocketMoved { .. }))
    );
    assert!(
        receipt.warnings[&2]
            .iter()
            .any(|w| matches!(w, Warning::CrossRegimeTrial { .. }))
    );
    assert!(receipt.warnings[&6].iter().any(|w| matches!(w, Warning::UnlistedReading { closest: Some(key), subject, .. } if key.as_str() == "latency" && subject == "c#2")));
    let assay = Assay::new(&f);
    assert_eq!(f.baselines()[&"bridge".parse()?].artifacts, ["commit:old"]);
    let _ = serde_json::to_value(&f)?;
    assert_eq!(assay.findings.len(), 5);
    for id in [
        "unbacked/a",
        "unbridged/3",
        "unlisted/3/latncy",
        "unread/third/latency",
        "unaccepted/third/c",
    ] {
        let identity = id.parse::<FindingId>()?;
        assert_eq!(identity.to_string(), id);
        assert!(assay.findings.contains_key(&identity));
    }
    assert_eq!(
        assay.findings[&"unbacked/a".parse()?].as_deref(),
        Some("Known assumption")
    );
    assert!(!assay.comparisons.contains_key(&"c#1".parse()?));
    let receipt = patch(
        &mut f,
        r#"[{"op":"void","trial":"c#2","reason":"Invalid sample"}]"#,
    )?;
    assert!(receipt.cleared.contains_key(&"unlisted/3/latncy".parse()?));
    assert!(receipt.warnings[&0].contains(&Warning::ReopenedFrontier));
    let receipt = patch(&mut f, r#"[{"op":"unvoid","trial":"c#2"}]"#)?;
    assert!(receipt.raised.contains_key(&"unlisted/3/latncy".parse()?));
    Ok(())
}

#[test]
fn refused_patches_are_atomic() -> Result<(), Box<dyn Error>> {
    let mut f = Frontier::default();
    let _ = patch(
        &mut f,
        r#"[{"op":"frontier","objective":"Original"},{"op":"hypothesis","slug":"x","title":"Existing"}]"#,
    )?;
    let original = f.clone();
    // Every failure position follows real mutations, including a newly created record.
    for count in 0..24 {
        let mut ops = Vec::new();
        for n in 0..count {
            ops.push(Op::Frontier {
                objective: format!("Replacement {n}"),
            });
        }
        ops.push(serde_json::from_str(
            r#"{"op":"hypothesis","slug":"new","title":"Temporary"}"#,
        )?);
        ops.push(serde_json::from_str(
            r#"{"op":"accept","hypothesis":"absent","rationale":"Refused"}"#,
        )?);
        assert!(f.patched(&ops, OffsetDateTime::UNIX_EPOCH).is_err());
        assert!(f.replay(&ops, OffsetDateTime::UNIX_EPOCH).is_err());
        assert_eq!(f, original);
    }
    for malformed in [
        r#"{"op":"accept","hypothesis":"x","baseline":"x","rationale":"No"}"#,
        r#"{"op":"accept","rationale":"No"}"#,
        r#"{"op":"trial","hypothesis":"x","note":"No","unexpected":true}"#,
        r#"{"op":"void","trial":"x#0","reason":"No"}"#,
        r#"{"op":"waive","finding":"unbridged/0","note":"No"}"#,
        r#"{"op":"waive","finding":"unlisted/2/key/extra","note":"No"}"#,
    ] {
        assert!(serde_json::from_str::<Op>(malformed).is_err());
    }
    Ok(())
}

#[test]
fn cycles_refuse_the_whole_patch() -> Result<(), Box<dyn Error>> {
    let mut f = Frontier::default();
    let _ = patch(
        &mut f,
        r#"[
        {"op":"frontier","objective":"Acyclic inquiry"},
        {"op":"hypothesis","slug":"a","title":"A"},
        {"op":"hypothesis","slug":"b","title":"B","after":["a"]},
        {"op":"hypothesis","slug":"c","title":"C","after":["b"]}
    ]"#,
    )?;
    for json in [
        r#"[{"op":"edge","from":"c","to":"a"}]"#,
        r#"[{"op":"edge","from":"a","to":"a"}]"#,
    ] {
        let ops = serde_json::from_str::<Vec<Op>>(json)?;
        let result = f.patched(&ops, OffsetDateTime::UNIX_EPOCH);
        assert!(
            matches!(result, Err(Refusal::Cycle { index: 0, path, .. }) if path.first() == path.last() && path.len() >= 2)
        );
    }
    Ok(())
}
