#![allow(
    dead_code,
    unused_imports,
    reason = "the shared MCP process harness serves both split integration scenario suites"
)]

include!("support/mcp_harness.rs");

#[test]
fn renamed_tag_guides_stale_mcp_context() -> TestResult {
    let project_root = temp_project_root("renamed_tag_guidance")?;
    init_project(&project_root)?;
    {
        let mut store = must(ProjectStore::open(&project_root), "open project store")?;
        let _ = must(
            store.register_tag(
                must(TagName::new("ls"), "old tag")?,
                must(NonEmptyText::new("local search shorthand"), "description")?,
            ),
            "register old tag",
        )?;
        let _ = must(
            store.rename_tag(RenameTagRequest {
                tag: must(TagName::new("ls"), "old tag")?,
                expected_revision: None,
                new_name: must(TagName::new("search/local"), "new tag")?,
            }),
            "rename tag",
        )?;
    }

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let response = harness.call_tool(
        74,
        "tag.add",
        json!({"name": "ls", "description": "stale shorthand"}),
    )?;
    assert_tool_error(&response);
    let message = must_some(tool_error_message(&response), "rename guidance")?;
    assert!(message.contains("renamed"));
    assert!(message.contains("search/local"));
    Ok(())
}

#[test]
fn frontier_open_is_the_grounding_surface_for_live_state() -> TestResult {
    let project_root = temp_project_root("frontier_open")?;
    init_project(&project_root)?;
    let _ = seed_clean_git_repository(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        10,
        "tag.add",
        json!({"name": "root-conquest", "description": "root work"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        11,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        12,
        "condition.define",
        json!({"key": "instance", "value_type": "string"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        13,
        "frontier.create",
        json!({
            "label": "LP root frontier",
            "objective": "Drive root cash-out on braid rails",
            "slug": "lp-root",
        }),
    )?);
    create_nodes_kpi(&mut harness, 131, "lp-root")?;
    assert_tool_ok(&harness.call_tool(
        14,
        "hypothesis.record",
        json!({
            "frontier": "lp-root",
            "slug": "node-local-loop",
            "title": "Node-local logical cut loop",
            "summary": "Push cut cash-out below root.",
            "body": "Thread node-local logical cuts through native LP reoptimization so the same intervention can cash out below root on parity rails without corrupting root ownership semantics.",
            "expected_yield": "medium",
            "confidence": "medium",
            "tags": ["root-conquest"],
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        15,
        "experiment.open",
        json!({
            "hypothesis": "node-local-loop",
            "slug": "baseline-20s",
            "title": "Baseline parity 20s",
            "summary": "Reference rail.",
            "tags": ["root-conquest"],
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        16,
        "experiment.close",
        json!({
            "experiment": "baseline-20s",
            "keep_hypothesis_on_worklist": true,
            "backend": "manual",
            "command": {"argv": ["baseline-20s"]},
            "conditions": {"instance": "4x5-braid"},
            "primary_metric": {"key": "nodes_solved", "value": 220.0},
            "verdict": "kept",
            "rationale": "Baseline retained as the current comparison line for the slice."
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        17,
        "experiment.open",
        json!({
            "hypothesis": "node-local-loop",
            "slug": "loop-20s",
            "title": "Loop parity 20s",
            "summary": "Live challenger.",
            "tags": ["root-conquest"],
            "parents": [{"kind": "experiment", "selector": "baseline-20s"}],
        }),
    )?);

    let frontier_open =
        harness.call_tool_full(18, "frontier.open", json!({"frontier": "lp-root"}))?;
    assert_tool_ok(&frontier_open);
    let content = tool_content(&frontier_open);
    assert_no_opaque_ids(content).map_err(io::Error::other)?;
    assert_eq!(content["frontier"]["slug"].as_str(), Some("lp-root"));
    assert_eq!(
        must_some(content["active_tags"].as_array(), "active tags array")?
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>(),
        vec!["root-conquest"]
    );
    assert!(
        must_some(
            content["active_metric_keys"].as_array(),
            "active metric keys array"
        )?
        .iter()
        .any(|metric| metric["key"].as_str() == Some("nodes_solved"))
    );
    let worklist_hypotheses = must_some(
        content["worklist_hypotheses"].as_array(),
        "worklist hypotheses array",
    )?;
    assert_eq!(worklist_hypotheses.len(), 1);
    assert_eq!(
        worklist_hypotheses[0]["hypothesis"]["slug"].as_str(),
        Some("node-local-loop")
    );
    assert!(worklist_hypotheses[0]["hypothesis"].get("id").is_none());
    assert_eq!(
        worklist_hypotheses[0]["latest_closed_experiment"]["slug"].as_str(),
        Some("baseline-20s")
    );
    assert_eq!(
        must_some(
            content["open_experiments"].as_array(),
            "open experiments array"
        )?[0]["slug"]
            .as_str(),
        Some("loop-20s")
    );
    assert!(
        must_some(
            content["open_experiments"].as_array(),
            "open experiments array",
        )?[0]
            .get("hypothesis_id")
            .is_none()
    );
    assert!(worklist_hypotheses[0]["hypothesis"].get("body").is_none());
    Ok(())
}

#[test]
fn frontier_update_mutates_objective_and_kpi_grounding() -> TestResult {
    let project_root = temp_project_root("frontier_update")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        70,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        71,
        "frontier.create",
        json!({
            "label": "LP root frontier",
            "objective": "Initial root push",
            "slug": "lp-root",
        }),
    )?);

    let updated = harness.call_tool_full(
        72,
        "frontier.update",
        json!({
            "frontier": "lp-root",
            "objective": "Drive structural LP cash-out on parity rails",
            "situation": "Structural LP churn is the active hill.",
            "unknowns": ["How far queued structural reuse can cash out below root."],
        }),
    )?;
    assert_tool_ok(&updated);
    let updated_content = tool_content(&updated);
    assert_eq!(
        updated_content["record"]["objective"].as_str(),
        Some("Drive structural LP cash-out on parity rails")
    );
    assert!(
        updated_content["record"]["brief"]
            .get("scoreboard_metric_keys")
            .is_none()
    );

    let kpi = harness.call_tool_full(
        73,
        "kpi.create",
        json!({
            "frontier": "lp-root",
            "metric": "nodes_solved",
        }),
    )?;
    assert_tool_ok(&kpi);

    let frontier_open =
        harness.call_tool_full(74, "frontier.open", json!({ "frontier": "lp-root" }))?;
    assert_tool_ok(&frontier_open);
    let open_content = tool_content(&frontier_open);
    assert_eq!(
        open_content["frontier"]["objective"].as_str(),
        Some("Drive structural LP cash-out on parity rails")
    );
    assert_eq!(
        must_some(
            open_content["kpis"]
                .as_array()
                .and_then(|items| items.first()),
            "frontier KPI entry",
        )?["metric"]["key"]
            .as_str(),
        Some("nodes_solved")
    );

    let kpi_metrics = harness.call_tool_full(
        75,
        "metric.keys",
        json!({
            "frontier": "lp-root",
            "scope": "kpi",
        }),
    )?;
    assert_tool_ok(&kpi_metrics);
    assert_eq!(
        must_some(
            tool_content(&kpi_metrics)["metrics"]
                .as_array()
                .and_then(|items| items.first()),
            "KPI metric entry",
        )?["key"]
            .as_str(),
        Some("nodes_solved")
    );

    Ok(())
}

#[test]
fn experiment_nearest_finds_structural_buckets_and_champion() -> TestResult {
    let project_root = temp_project_root("experiment_nearest")?;
    init_project(&project_root)?;
    let _ = seed_clean_git_repository(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        80,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        81,
        "condition.define",
        json!({"key": "instance", "value_type": "string"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        82,
        "condition.define",
        json!({"key": "profile", "value_type": "string"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        83,
        "condition.define",
        json!({"key": "duration_s", "value_type": "numeric"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        84,
        "frontier.create",
        json!({
            "label": "Comparator frontier",
            "objective": "Keep exact-slice comparators cheap to find",
            "slug": "comparators",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        85,
        "kpi.create",
        json!({
            "frontier": "comparators",
            "metric": "nodes_solved",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        86,
        "hypothesis.record",
        json!({
            "frontier": "comparators",
            "slug": "structural-loop",
            "title": "Structural loop",
            "summary": "Compare exact-slice structural LP lines.",
            "body": "Thread structural LP reuse through the same 4x5 parity slice so exact-slice comparators remain easy to recover and dead branches stay visible before the next iteration starts.",
            "expected_yield": "medium",
            "confidence": "medium",
        }),
    )?);

    for (id, slug, verdict, value, duration_s) in [
        (87_u64, "exact-kept", "kept", 111.0, 60),
        (89_u64, "exact-accepted", "accepted", 125.0, 60),
        (91_u64, "exact-rejected", "rejected", 98.0, 60),
        (93_u64, "different-duration", "accepted", 140.0, 20),
    ] {
        assert_tool_ok(&harness.call_tool(
            id,
            "experiment.open",
            json!({
                "hypothesis": "structural-loop",
                "slug": slug,
                "title": format!("{slug} rail"),
                "summary": format!("{slug} summary"),
            }),
        )?);
        assert_tool_ok(&harness.call_tool(
            id + 1,
            "experiment.close",
            json!({
                "experiment": slug,
                "keep_hypothesis_on_worklist": true,
                "backend": "manual",
                "command": {"argv": [slug]},
                "conditions": {
                    "instance": "4x5",
                    "profile": "parity",
                    "duration_s": duration_s,
                },
                "primary_metric": {"key": "nodes_solved", "value": value},
                "verdict": verdict,
                "rationale": format!("{slug} outcome"),
            }),
        )?);
    }

    let nearest = harness.call_tool_full(
        95,
        "experiment.nearest",
        json!({
            "frontier": "comparators",
            "conditions": {
                "instance": "4x5",
                "profile": "parity",
                "duration_s": 60,
            },
        }),
    )?;
    assert_tool_ok(&nearest);
    let content = tool_content(&nearest);
    assert_eq!(content["metric"]["key"].as_str(), Some("nodes_solved"));
    assert_eq!(
        content["accepted"]["experiment"]["slug"].as_str(),
        Some("exact-accepted")
    );
    assert_eq!(
        content["kept"]["experiment"]["slug"].as_str(),
        Some("exact-kept")
    );
    assert_eq!(
        content["rejected"]["experiment"]["slug"].as_str(),
        Some("exact-rejected")
    );
    assert_eq!(
        content["champion"]["experiment"]["slug"].as_str(),
        Some("exact-accepted")
    );
    assert!(
        must_some(
            content["accepted"]["reasons"].as_array(),
            "accepted comparator reasons",
        )?
        .iter()
        .any(|reason| reason.as_str() == Some("exact dimension match"))
    );

    Ok(())
}

#[test]
fn registry_and_history_surfaces_render_timestamps_as_strings() -> TestResult {
    let project_root = temp_project_root("timestamp_text")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let dimension = harness.call_tool_full(
        19,
        "condition.define",
        json!({
            "key": "duration_s",
            "value_type": "numeric",
            "description": "Wallclock timeout in seconds.",
        }),
    )?;
    assert_tool_ok(&dimension);
    assert!(tool_content(&dimension)["record"]["created_at"].is_string());
    assert!(tool_content(&dimension)["record"]["updated_at"].is_null());

    let conditions = harness.call_tool_full(20, "condition.list", json!({}))?;
    assert_tool_ok(&conditions);
    let listed = must_some(
        tool_content(&conditions)["conditions"]
            .as_array()
            .and_then(|items| items.first()),
        "defined condition in list",
    )?;
    assert!(listed["created_at"].is_string());
    assert!(listed["updated_at"].is_null());

    let frontier = harness.call_tool_full(
        21,
        "frontier.create",
        json!({
            "label": "alpha",
            "objective": "Trace timestamp presentation discipline",
        }),
    )?;
    assert_tool_ok(&frontier);
    let frontier_slug = must_some(
        tool_content(&frontier)["record"]["slug"].as_str(),
        "frontier slug",
    )?;

    let history =
        harness.call_tool_full(22, "frontier.history", json!({ "frontier": frontier_slug }))?;
    assert_tool_ok(&history);
    let history_entry = must_some(
        tool_content(&history)["history"]
            .as_array()
            .and_then(|items| items.first()),
        "frontier history entry",
    )?;
    assert!(history_entry["occurred_at"].is_string());

    Ok(())
}

#[test]
fn metric_define_accepts_builtin_and_custom_unit_tokens() -> TestResult {
    let project_root = temp_project_root("metric_units")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let microseconds = harness.call_tool_full(
        23,
        "metric.define",
        json!({
            "key": "oracle_solve_wallclock_micros",
            "dimension": "time",
            "display_unit": "micros",
            "objective": "minimize",
        }),
    )?;
    assert_tool_ok(&microseconds);
    assert_eq!(
        tool_content(&microseconds)["record"]["display_unit"].as_str(),
        Some("microseconds")
    );

    let bytes = harness.call_tool_full(
        24,
        "metric.define",
        json!({
            "key": "telemetry_payload",
            "dimension": "bytes",
            "display_unit": "mib",
            "objective": "minimize",
        }),
    )?;
    assert_tool_ok(&bytes);
    assert_eq!(
        tool_content(&bytes)["record"]["display_unit"].as_str(),
        Some("mebibytes")
    );

    let ratio = harness.call_tool_full(
        25,
        "metric.define",
        json!({
            "key": "treatment_control_ratio",
            "dimension": "ratio",
            "display_unit": "ratio",
            "objective": "minimize",
        }),
    )?;
    assert_tool_ok(&ratio);
    assert_eq!(
        tool_content(&ratio)["record"]["dimension"].as_str(),
        Some("dimensionless")
    );
    assert_eq!(
        tool_content(&ratio)["record"]["display_unit"].as_str(),
        Some("dimensionless")
    );

    let placeholder = harness.call_tool(
        26,
        "metric.define",
        json!({
            "key": "bad_custom_placeholder",
            "dimension": "dimensionless",
            "display_unit": "custom",
            "objective": "minimize",
        }),
    )?;
    assert_tool_error(&placeholder);
    assert!(
        must_some(tool_error_message(&placeholder), "metric unit error")?.contains("metric unit")
    );

    Ok(())
}

#[test]
fn hypothesis_body_discipline_is_enforced_over_mcp() -> TestResult {
    let project_root = temp_project_root("single_paragraph")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        20,
        "frontier.create",
        json!({
            "label": "Import frontier",
            "objective": "Stress hypothesis discipline",
            "slug": "discipline",
        }),
    )?);

    let response = harness.call_tool(
        21,
        "hypothesis.record",
        json!({
            "frontier": "discipline",
            "title": "Paragraph discipline",
            "summary": "Should reject multi-paragraph bodies.",
            "body": "first paragraph\n\nsecond paragraph",
            "expected_yield": "medium",
            "confidence": "medium",
        }),
    )?;
    assert_tool_error(&response);
    assert!(must_some(tool_error_message(&response), "fault message")?.contains("paragraph"));
    Ok(())
}

#[test]
fn experiment_close_drives_metric_best_and_analysis() -> TestResult {
    let project_root = temp_project_root("metric_best")?;
    init_project(&project_root)?;
    let closing_commit = seed_clean_git_repository(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        40,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        41,
        "condition.define",
        json!({"key": "instance", "value_type": "string"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        42,
        "frontier.create",
        json!({
            "label": "Metric frontier",
            "objective": "Test best-of ranking",
            "slug": "metric-frontier",
        }),
    )?);
    create_nodes_kpi(&mut harness, 421, "metric-frontier")?;
    assert_tool_ok(&harness.call_tool(
        43,
        "hypothesis.record",
        json!({
            "frontier": "metric-frontier",
            "slug": "reopt-dominance",
            "title": "Node reopt dominates native LP spend",
            "summary": "Track node LP wallclock concentration on braid rails.",
            "body": "Matched LP site traces indicate native LP spend is dominated by node reoptimization on the braid rails, so the next interventions should target node-local LP churn instead of root-only machinery.",
            "expected_yield": "medium",
            "confidence": "medium",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        44,
        "experiment.open",
        json!({
            "hypothesis": "reopt-dominance",
            "slug": "trace-baseline",
            "title": "Trace baseline",
            "summary": "First matched trace.",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        45,
        "experiment.close",
        json!({
            "experiment": "trace-baseline",
            "keep_hypothesis_on_worklist": true,
            "backend": "manual",
            "command": {"argv": ["trace-baseline"]},
            "conditions": {"instance": "4x5-braid"},
            "primary_metric": {"key": "nodes_solved", "value": 217.0},
            "verdict": "kept",
            "rationale": "Baseline trace is real but not dominant.",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        46,
        "experiment.open",
        json!({
            "hypothesis": "reopt-dominance",
            "slug": "trace-node-reopt",
            "title": "Trace node reopt",
            "summary": "Matched LP site traces with node focus.",
            "parents": [{"kind": "experiment", "selector": "trace-baseline"}],
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        47,
        "experiment.close",
        json!({
            "experiment": "trace-node-reopt",
            "keep_hypothesis_on_worklist": true,
            "backend": "manual",
            "command": {"argv": ["matched-lp-site-traces"]},
            "conditions": {"instance": "4x5-braid"},
            "primary_metric": {"key": "nodes_solved", "value": 273.0},
            "verdict": "accepted",
            "rationale": "Matched LP site traces show node reoptimization as the dominant sink.",
            "analysis": {
                "summary": "Node LP work is now the primary native sink.",
                "body": "The differential traces isolate node reoptimization as the dominant native LP wallclock site on the matched braid rail, which justifies prioritizing node-local LP control work over further root-only tuning."
            }
        }),
    )?);

    let best = harness.call_tool_full(
        48,
        "metric.best",
        json!({
            "frontier": "metric-frontier",
            "hypothesis": "reopt-dominance",
            "key": "nodes_solved",
        }),
    )?;
    assert_tool_ok(&best);
    let entries = must_some(
        tool_content(&best)["entries"].as_array(),
        "metric best entries",
    )?;
    assert_eq!(
        entries[0]["experiment"]["slug"].as_str(),
        Some("trace-node-reopt")
    );
    assert_eq!(entries[0]["value"].as_f64(), Some(273.0));

    let detail = harness.call_tool_full(
        49,
        "experiment.read",
        json!({"experiment": "trace-node-reopt"}),
    )?;
    assert_tool_ok(&detail);
    let content = tool_content(&detail);
    assert_no_opaque_ids(content).map_err(io::Error::other)?;
    assert_eq!(
        content["record"]["outcome"]["verdict"].as_str(),
        Some("accepted")
    );
    assert_eq!(
        content["record"]["outcome"]["analysis"]["summary"].as_str(),
        Some("Node LP work is now the primary native sink.")
    );
    assert_eq!(
        content["record"]["outcome"]["commit_hash"].as_str(),
        Some(closing_commit.as_str())
    );
    assert_eq!(content["record"]["slug"].as_str(), Some("trace-node-reopt"));
    assert!(content["record"].get("frontier_id").is_none());
    assert!(content["record"].get("hypothesis_id").is_none());
    assert_eq!(
        content["owning_hypothesis"]["slug"].as_str(),
        Some("reopt-dominance")
    );
    assert!(content["owning_hypothesis"].get("id").is_none());
    Ok(())
}

#[test]
fn synthetic_kpi_ranks_from_reported_observed_leaves() -> TestResult {
    let project_root = temp_project_root("synthetic_kpi")?;
    init_project(&project_root)?;
    let _closing_commit = seed_clean_git_repository(&project_root)?;
    let mut store = must(ProjectStore::open(&project_root), "open project store")?;

    let _ = must(
        store.define_metric(DefineMetricRequest {
            key: must(NonEmptyText::new("work_done"), "metric key")?,
            dimension: MetricDimension::Count,
            display_unit: Some(MetricUnit::Count),
            aggregation: MetricAggregation::Point,
            objective: OptimizationObjective::Maximize,
            description: None,
        }),
        "define work metric",
    )?;
    let _ = must(
        store.define_metric(DefineMetricRequest {
            key: must(NonEmptyText::new("elapsed_time"), "metric key")?,
            dimension: MetricDimension::Time,
            display_unit: Some(MetricUnit::Milliseconds),
            aggregation: MetricAggregation::Point,
            objective: OptimizationObjective::Minimize,
            description: None,
        }),
        "define elapsed metric",
    )?;
    let _ = must(
        store.define_synthetic_metric(DefineSyntheticMetricRequest {
            key: must(NonEmptyText::new("work_rate"), "synthetic key")?,
            expression: SyntheticMetricExpression::Div {
                left: Box::new(SyntheticMetricExpression::metric(must(
                    NonEmptyText::new("work_done"),
                    "left operand",
                )?)),
                right: Box::new(SyntheticMetricExpression::metric(must(
                    NonEmptyText::new("elapsed_time"),
                    "right operand",
                )?)),
            },
            aggregation: MetricAggregation::Point,
            objective: OptimizationObjective::Maximize,
            description: None,
        }),
        "define synthetic metric",
    )?;
    let _ = must(
        store.define_run_dimension(DefineRunDimensionRequest {
            key: must(NonEmptyText::new("instance"), "condition key")?,
            value_type: FieldValueType::String,
            description: None,
        }),
        "define condition",
    )?;
    let _ = must(
        store.create_frontier(CreateFrontierRequest {
            label: must(
                NonEmptyText::new("Synthetic KPI Frontier"),
                "frontier label",
            )?,
            objective: must(
                NonEmptyText::new("Verify synthetic KPI leaf enforcement"),
                "frontier objective",
            )?,
            slug: Some(must(Slug::new("synthetic-kpi-frontier"), "frontier slug")?),
        }),
        "create frontier",
    )?;

    let premature = store.create_kpi(CreateKpiRequest {
        frontier: "synthetic-kpi-frontier".to_owned(),
        metric: must(NonEmptyText::new("work_rate"), "synthetic kpi metric")?,
    });
    let premature_message = match premature {
        Ok(_) => {
            return Err(io::Error::other("synthetic KPI without KPI leaves should fail").into());
        }
        Err(error) => error.to_string(),
    };
    assert!(
        premature_message.contains("missing: work_done, elapsed_time"),
        "{premature_message}"
    );

    let _ = must(
        store.define_metric(DefineMetricRequest {
            key: must(NonEmptyText::new("unrelated_quality"), "unrelated metric")?,
            dimension: MetricDimension::Dimensionless,
            display_unit: Some(MetricUnit::Dimensionless),
            aggregation: MetricAggregation::Point,
            objective: OptimizationObjective::Maximize,
            description: None,
        }),
        "define unrelated metric",
    )?;
    for metric in [
        "work_done",
        "elapsed_time",
        "work_rate",
        "unrelated_quality",
    ] {
        let _ = must(
            store.create_kpi(CreateKpiRequest {
                frontier: "synthetic-kpi-frontier".to_owned(),
                metric: must(NonEmptyText::new(metric), "kpi metric")?,
            }),
            format!("create KPI {metric}"),
        )?;
    }
    let hypothesis = must(
        store.create_hypothesis_from_mcp(CreateHypothesisRequest {
            frontier: "synthetic-kpi-frontier".to_owned(),
            slug: Some(must(Slug::new("synthetic-rate"), "hypothesis slug")?),
            title: must(NonEmptyText::new("Synthetic rate moves"), "hypothesis title")?,
            summary: must(
                NonEmptyText::new("A derived rate should rank from observed leaves."),
                "hypothesis summary",
            )?,
            body: must(
                NonEmptyText::new(
                    "Derived work rate is the KPI of interest, but individual work and elapsed-time leaves are the only reportable experiment measurements.",
                ),
                "hypothesis body",
            )?,
            expected_yield: HypothesisAssessmentLevel::Medium,
            confidence: HypothesisAssessmentLevel::Medium,
            tags: BTreeSet::new(),
            parents: Vec::new(),
        }),
        "create hypothesis",
    )?;
    let _ = must(
        store.open_experiment_from_mcp(OpenExperimentRequest {
            hypothesis: hypothesis.slug.to_string(),
            slug: Some(must(Slug::new("rate-baseline"), "experiment slug")?),
            title: must(NonEmptyText::new("Rate baseline"), "experiment title")?,
            summary: None,
            tags: BTreeSet::new(),
            parents: Vec::new(),
        }),
        "open experiment",
    )?;
    let _ = must(
        store.close_experiment_from_mcp(CloseExperimentRequest {
            experiment: "rate-baseline".to_owned(),
            expected_revision: None,
            keep_hypothesis_on_worklist: Some(true),
            backend: ExecutionBackend::Manual,
            command: must(
                CommandRecipe::new(
                    None,
                    vec![must(NonEmptyText::new("rate-baseline"), "command")?],
                    BTreeMap::new(),
                ),
                "command recipe",
            )?,
            dimensions: BTreeMap::from([(
                must(NonEmptyText::new("instance"), "condition key")?,
                RunDimensionValue::String(must(NonEmptyText::new("toy"), "condition value")?),
            )]),
            primary_metric: Some(ReportedMetricValue {
                key: must(NonEmptyText::new("work_done"), "primary metric")?,
                value: 240.0,
                unit: None,
            }),
            supporting_metrics: vec![ReportedMetricValue {
                key: must(NonEmptyText::new("elapsed_time"), "supporting metric")?,
                value: 120.0,
                unit: Some(MetricUnit::Milliseconds),
            }],
            verdict: FrontierVerdict::Accepted,
            rationale: must(
                NonEmptyText::new("Observed leaves imply a derived rate in canonical units."),
                "rationale",
            )?,
            analysis: None,
        }),
        "close experiment",
    )?;

    let best = must(
        store.metric_best(MetricBestQuery {
            frontier: Some("synthetic-kpi-frontier".to_owned()),
            hypothesis: None,
            key: must(NonEmptyText::new("work_rate"), "metric best key")?,
            dimensions: BTreeMap::new(),
            include_rejected: true,
            limit: None,
            order: None,
        }),
        "rank synthetic metric",
    )?;
    assert_eq!(best.len(), 1);
    assert_eq!(best[0].experiment.slug.as_str(), "rate-baseline");
    assert_eq!(best[0].value, 240.0 / 120_000_000.0);

    let sql = must(
        store.frontier_query_sql(FrontierSqlQuery {
            frontier: "synthetic-kpi-frontier".to_owned(),
            sql: "SELECT metric_key, metric_kind, display_value FROM q_experiment_metric ORDER BY metric_key".to_owned(),
            params: Vec::new(),
            max_rows: None,
            timeout_ms: None,
        }),
        "query synthetic metric SQL view",
    )?;
    assert!(sql.rows.iter().any(|row| {
        row[0].as_str() == Some("work_rate")
            && row[1].as_str() == Some("synthetic")
            && row[2].as_f64() == Some(240.0 / 120_000_000.0)
    }));

    let kpi_metrics = must(
        store.metric_keys(MetricKeysQuery {
            frontier: Some("synthetic-kpi-frontier".to_owned()),
            scope: MetricScope::Kpi,
        }),
        "list KPI metrics",
    )?;
    let synthetic = must_some(
        kpi_metrics
            .iter()
            .find(|metric| metric.key.as_str() == "work_rate"),
        "synthetic KPI summary",
    )?;
    assert_eq!(synthetic.kind.as_str(), "synthetic");
    Ok(())
}

#[test]
fn frontier_query_sql_is_scoped_and_tabular() -> TestResult {
    let project_root = temp_project_root("frontier_query")?;
    init_project(&project_root)?;
    let _closing_commit = seed_clean_git_repository(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    seed_frontier_query_fixture(&mut harness)?;

    let schema = harness.call_tool(
        3060,
        "frontier.query.schema",
        json!({"frontier": "query-alpha"}),
    )?;
    assert_tool_ok(&schema);
    let schema_text = must_some(tool_text(&schema), "frontier query schema text")?;
    assert!(schema_text.starts_with("view|column|type|description"));
    assert!(schema_text.contains("q_experiment_metric|metric_key|text|Metric key."));
    assert!(!schema_text.contains("frontier_id"));

    let query = harness.call_tool(
        3061,
        "frontier.query.sql",
        json!({
            "frontier": "query-alpha",
            "sql": "select experiment_slug, hypothesis_slug, metric_key, display_value from q_experiment_metric where metric_key = ? order by experiment_slug",
            "params": ["nodes_solved"],
        }),
    )?;
    assert_tool_ok(&query);
    let text = must_some(tool_text(&query), "frontier query table text")?;
    assert!(text.starts_with("experiment_slug|hypothesis_slug|metric_key|display_value"));
    assert!(text.contains("query-alpha-run|query-alpha-hypothesis|nodes_solved|111"));
    assert!(!text.contains("query-beta"));

    let command = harness.call_tool(
        3062,
        "frontier.query.sql",
        json!({
            "frontier": "query-alpha",
            "sql": "select arg from q_experiment_command_arg where experiment_slug = ? order by ordinal",
            "params": ["query-alpha-run"],
        }),
    )?;
    assert_tool_ok(&command);
    let command_text = must_some(tool_text(&command), "frontier query command text")?;
    assert_eq!(command_text, "arg\nquery-alpha-command");

    let rows = must_some(tool_content(&query)["rows"].as_array(), "query rows")?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_str(), Some("query-alpha-run"));
    assert_eq!(rows[0][3].as_f64(), Some(111.0));
    Ok(())
}

#[test]
fn frontier_query_sql_rejects_mutation_and_escape_hatches() -> TestResult {
    let project_root = temp_project_root("frontier_query_hostile")?;
    init_project(&project_root)?;
    let _closing_commit = seed_clean_git_repository(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    seed_frontier_query_fixture(&mut harness)?;

    for (offset, sql, expected) in [
        (
            0,
            "select slug from experiments",
            "read-only and frontier-scoped",
        ),
        (
            1,
            "select frontier_id from __spinner_query_scope",
            "read-only and frontier-scoped",
        ),
        (
            2,
            "update metric_definitions set key = key",
            "read-only and frontier-scoped",
        ),
        (
            3,
            "attach database ':memory:' as aux",
            "read-only and frontier-scoped",
        ),
        (
            4,
            "pragma table_info(experiments)",
            "read-only and frontier-scoped",
        ),
        (
            5,
            "select name from pragma_table_info('experiments')",
            "read-only and frontier-scoped",
        ),
        (
            6,
            "select random() from q_experiment",
            "read-only and frontier-scoped",
        ),
        (7, "select 1; select 2", "multiple statements are rejected"),
    ] {
        let response = harness.call_tool(
            3070 + offset,
            "frontier.query.sql",
            json!({
                "frontier": "query-alpha",
                "sql": sql,
            }),
        )?;
        assert_tool_error(&response);
        assert!(
            must_some(tool_error_message(&response), "query policy error")?.contains(expected),
            "expected error fragment `{expected}` for `{sql}` but saw {response:#}"
        );
    }
    Ok(())
}

#[test]
fn experiment_close_rejects_dirty_worktree() -> TestResult {
    let project_root = temp_project_root("dirty_close")?;
    init_project(&project_root)?;
    let _ = seed_clean_git_repository(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        50,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        51,
        "condition.define",
        json!({"key": "instance", "value_type": "string"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        52,
        "frontier.create",
        json!({
            "label": "Dirty frontier",
            "objective": "Reject dirty closes",
            "slug": "dirty-frontier",
        }),
    )?);
    create_nodes_kpi(&mut harness, 521, "dirty-frontier")?;
    assert_tool_ok(&harness.call_tool(
        53,
        "hypothesis.record",
        json!({
            "frontier": "dirty-frontier",
            "slug": "dirty-hypothesis",
            "title": "Dirty close rejection",
            "summary": "A dirty worktree must block close.",
            "body": "When the experiment implementation state is not committed, closing the experiment should fail so the ledger never records an unrecoverable slice.",
            "expected_yield": "medium",
            "confidence": "medium",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        54,
        "experiment.open",
        json!({
            "hypothesis": "dirty-hypothesis",
            "slug": "dirty-run",
            "title": "Dirty run",
            "summary": "Leave the worktree dirty before closing.",
        }),
    )?);

    must(
        fs::write(project_root.join("dirty.txt"), "uncommitted\n"),
        "write dirty worktree file",
    )?;

    let response = harness.call_tool_full(
        55,
        "experiment.close",
        json!({
            "experiment": "dirty-run",
            "keep_hypothesis_on_worklist": true,
            "backend": "manual",
            "command": {"argv": ["dirty-run"]},
            "conditions": {"instance": "4x5-braid"},
            "primary_metric": {"key": "nodes_solved", "value": 13.0},
            "verdict": "rejected",
            "rationale": "Dirty worktree should abort the close.",
        }),
    )?;
    assert_tool_error(&response);
    let message = must_some(tool_error_message(&response), "dirty close error message")?;
    assert!(message.contains("clean git worktree"));
    assert!(message.contains("dirty.txt"));
    Ok(())
}

#[test]
fn experiment_scuffed_close_can_omit_kpi_and_dirty_commit() -> TestResult {
    let project_root = temp_project_root("scuffed_close")?;
    init_project(&project_root)?;
    let _ = seed_clean_git_repository(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        3050,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        3051,
        "condition.define",
        json!({"key": "instance", "value_type": "string"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        3052,
        "frontier.create",
        json!({
            "label": "Scuffed frontier",
            "objective": "Exercise invalid experiment closures.",
            "slug": "scuffed-frontier",
        }),
    )?);
    create_nodes_kpi(&mut harness, 3053, "scuffed-frontier")?;
    assert_tool_ok(&harness.call_tool(
        3054,
        "hypothesis.record",
        json!({
            "frontier": "scuffed-frontier",
            "slug": "scuffed-hypothesis",
            "title": "Scuffed hypothesis",
            "summary": "An accidentally opened experiment can be closed without fake KPI values.",
            "body": "The model opened an experiment for support work, then realized no meaningful KPI can be obtained from the setup.",
            "expected_yield": "low",
            "confidence": "high",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        3055,
        "experiment.open",
        json!({
            "hypothesis": "scuffed-hypothesis",
            "slug": "scuffed-run",
            "title": "Scuffed run",
        }),
    )?);
    must(
        fs::write(project_root.join("dirty.txt"), "uncommitted\n"),
        "write dirty worktree file",
    )?;

    let closed = harness.call_tool_full(
        3056,
        "experiment.close",
        json!({
            "experiment": "scuffed-run",
            "keep_hypothesis_on_worklist": false,
            "backend": "manual",
            "command": {"argv": ["scuffed-run"]},
            "conditions": {"instance": "4x5-braid"},
            "verdict": "scuffed",
            "rationale": "The setup was invalid, so recording a KPI value would be dummy data.",
        }),
    )?;
    assert_tool_ok(&closed);
    let outcome = &tool_content(&closed)["record"]["outcome"];
    assert_eq!(outcome["verdict"].as_str(), Some("scuffed"));
    assert!(outcome["primary_metric"].is_null());
    assert!(outcome["commit_hash"].is_null());
    Ok(())
}

#[test]
fn experiment_close_uses_command_worktree_when_present() -> TestResult {
    let project_root = temp_project_root("worktree_close")?;
    init_project(&project_root)?;
    let _ = seed_clean_git_repository(&project_root)?;
    let worktree_root = must_some(project_root.parent(), "worktree parent")?.join(format!(
        "{}-linked-worktree",
        must_some(project_root.file_name(), "project root name")?
    ));
    let _ = run_git(
        &project_root,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            "experiment-branch",
            worktree_root.as_str(),
        ],
    )?;
    must(
        fs::write(
            worktree_root.join("worktree.txt"),
            "linked worktree commit\n",
        ),
        "write linked worktree file",
    )?;
    let _ = run_git(&worktree_root, &["add", "worktree.txt"])?;
    let _ = run_git(
        &worktree_root,
        &[
            "-c",
            "user.name=Fidget Spinner Tests",
            "-c",
            "user.email=fidget-spinner-tests@example.invalid",
            "commit",
            "-q",
            "-m",
            "worktree experiment state",
        ],
    )?;
    let worktree_commit = run_git(&worktree_root, &["rev-parse", "HEAD"])?;
    must(
        fs::write(project_root.join("dirty.txt"), "main checkout dirt\n"),
        "write dirty main checkout file",
    )?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        56,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        57,
        "condition.define",
        json!({"key": "instance", "value_type": "string"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        58,
        "frontier.create",
        json!({
            "label": "Worktree frontier",
            "objective": "Close against linked worktree state",
            "slug": "worktree-frontier",
        }),
    )?);
    create_nodes_kpi(&mut harness, 581, "worktree-frontier")?;
    assert_tool_ok(&harness.call_tool(
        59,
        "hypothesis.record",
        json!({
            "frontier": "worktree-frontier",
            "slug": "worktree-hypothesis",
            "title": "Linked worktree closes should succeed",
            "summary": "Main checkout dirt should not block a clean linked worktree close.",
            "body": "When an experiment command names a linked worktree as its working directory, Spinner should capture cleanliness and HEAD from that worktree rather than from unrelated dirt in the bound checkout.",
            "expected_yield": "medium",
            "confidence": "medium",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        60,
        "experiment.open",
        json!({
            "hypothesis": "worktree-hypothesis",
            "slug": "worktree-run",
            "title": "Worktree run",
            "summary": "Close against the linked worktree.",
        }),
    )?);

    let closed = harness.call_tool_full(
        61,
        "experiment.close",
        json!({
            "experiment": "worktree-run",
            "keep_hypothesis_on_worklist": true,
            "backend": "worktree_process",
            "command": {
                "working_directory": worktree_root.as_str(),
                "argv": ["worktree-run"]
            },
            "conditions": {"instance": "4x5-braid"},
            "primary_metric": {"key": "nodes_solved", "value": 34.0},
            "verdict": "kept",
            "rationale": "The linked worktree is clean and should be the recorded implementation anchor.",
        }),
    )?;
    assert_tool_ok(&closed);
    assert_eq!(
        tool_content(&closed)["record"]["outcome"]["commit_hash"].as_str(),
        Some(worktree_commit.as_str())
    );
    Ok(())
}

// NT forbids replacing SQLite's directory while the worker holds the database
// open; Unix unlink semantics are the contract under test here.
#[cfg(not(windows))]
#[test]
fn already_bound_worker_refreshes_after_destructive_reseed() -> TestResult {
    let project_root = temp_project_root("same_path_reseed")?;

    let mut harness = McpHarness::spawn(None)?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let bind = harness.bind_project(60, &project_root)?;
    assert_tool_ok(&bind);

    assert_tool_ok(&harness.call_tool(
        601,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        61,
        "frontier.create",
        json!({
            "label": "alpha frontier",
            "objective": "first seeded frontier",
            "slug": "alpha",
        }),
    )?);
    create_nodes_kpi(&mut harness, 611, "alpha")?;
    let alpha_list = harness.call_tool_full(62, "frontier.list", json!({}))?;
    assert_tool_ok(&alpha_list);
    assert_eq!(frontier_slugs(&alpha_list), vec!["alpha"]);

    must(
        fs::remove_dir_all(fidget_spinner_store_sqlite::state_root_for_project_root(
            &project_root,
        )?),
        "remove project store",
    )?;
    init_project(&project_root)?;
    let mut reopened = must(ProjectStore::open(&project_root), "open recreated store")?;
    let _metric = must(
        reopened.define_metric(DefineMetricRequest {
            key: must(NonEmptyText::new("nodes_solved"), "metric key")?,
            dimension: MetricDimension::Count,
            display_unit: Some(must(MetricUnit::new("count"), "metric unit")?),
            aggregation: MetricAggregation::Point,
            objective: OptimizationObjective::Maximize,
            description: None,
        }),
        "define beta metric",
    )?;
    let _beta = must(
        reopened.create_frontier(CreateFrontierRequest {
            label: must(NonEmptyText::new("beta frontier"), "beta label")?,
            objective: must(
                NonEmptyText::new("second seeded frontier"),
                "beta objective",
            )?,
            slug: Some(must(Slug::new("beta"), "beta slug")?),
        }),
        "create beta frontier directly in recreated store",
    )?;
    let _kpi = must(
        reopened.create_kpi(CreateKpiRequest {
            frontier: "beta".to_owned(),
            metric: must(NonEmptyText::new("nodes_solved"), "kpi metric")?,
        }),
        "create beta KPI",
    )?;

    let beta_list = harness.call_tool_full(63, "frontier.list", json!({}))?;
    assert_tool_ok(&beta_list);
    assert_eq!(frontier_slugs(&beta_list), vec!["beta"]);
    Ok(())
}
