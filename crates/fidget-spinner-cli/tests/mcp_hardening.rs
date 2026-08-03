#![allow(
    dead_code,
    unused_imports,
    reason = "the shared MCP process harness serves both split integration scenario suites"
)]

include!("support/mcp_harness.rs");

#[test]
fn cold_start_exposes_bound_surface_and_new_toolset() -> TestResult {
    let project_root = temp_project_root("cold_start")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(None)?;
    let initialize = harness.initialize()?;
    assert_eq!(
        initialize["result"]["protocolVersion"].as_str(),
        Some("2025-11-25")
    );
    harness.notify_initialized()?;

    let tools = harness.tools_list()?;
    let tool_names = tool_names(&tools);
    assert!(tool_names.contains(&"frontier.open"));
    assert!(tool_names.contains(&"frontier.update"));
    assert!(tool_names.contains(&"frontier.query.schema"));
    assert!(tool_names.contains(&"frontier.query.sql"));
    assert!(tool_names.contains(&"hypothesis.record"));
    assert!(tool_names.contains(&"experiment.close"));
    assert!(tool_names.contains(&"experiment.scuff"));
    assert!(tool_names.contains(&"experiment.nearest"));
    assert!(tool_names.contains(&"kpi.reference.set"));
    assert!(tool_names.contains(&"kpi.reference.list"));
    assert!(tool_names.contains(&"kpi.reference.delete"));
    assert!(!tool_names.contains(&"node.list"));
    assert!(!tool_names.contains(&"research.record"));
    assert!(!tool_names.contains(&"frontier.brief.update"));

    let health = harness.call_tool(3, "system.health", json!({}))?;
    assert_tool_ok(&health);
    assert_eq!(tool_content(&health)["bound"].as_bool(), Some(false));

    let bind = harness.bind_project(4, &project_root)?;
    assert_tool_ok(&bind);
    assert_eq!(
        tool_content(&bind)["display_name"].as_str(),
        Some("mcp test project")
    );
    let state_root = must_some(
        tool_content(&bind)["state_root"].as_str(),
        "bind state root",
    )?;
    assert!(!state_root.starts_with(project_root.as_str()));
    assert!(state_root.contains("fidget-spinner/projects"));

    let rebound_health = harness.call_tool(5, "system.health", json!({}))?;
    assert_tool_ok(&rebound_health);
    assert_eq!(tool_content(&rebound_health)["bound"].as_bool(), Some(true));
    Ok(())
}

#[test]
fn telemetry_retains_coded_tool_specific_argument_failures() -> TestResult {
    let project_root = temp_project_root("coded_telemetry")?;
    init_project(&project_root)?;
    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let malformed = harness.call_tool(
        90,
        "frontier.create",
        json!({
            "label": 7,
            "objective": "Type errors retain their calling tool identity.",
        }),
    )?;
    assert_tool_error(&malformed);
    assert_eq!(
        tool_content(&malformed)["code"].as_str(),
        Some("invalid_protocol_input")
    );
    assert_eq!(
        tool_content(&malformed)["operation"].as_str(),
        Some("tools/call:frontier.create")
    );

    let telemetry = harness.call_tool_full(91, "system.telemetry", json!({}))?;
    assert_tool_ok(&telemetry);
    let telemetry = tool_content(&telemetry);
    assert!(telemetry["window_started_at"].is_string());
    assert_eq!(
        telemetry["operations"]["tools/call:frontier.create"]["fault_codes"]
            ["invalid_protocol_input"]
            .as_u64(),
        Some(1)
    );
    Ok(())
}

#[test]
fn frontier_archive_hides_default_enumeration_without_breaking_direct_reads() -> TestResult {
    let root = temp_project_root("frontier_archive_filter")?;
    init_project(&root)?;
    let mut store = must(ProjectStore::open(&root), "open store")?;
    let frontier = must(
        store.create_frontier(CreateFrontierRequest {
            label: must(NonEmptyText::new("archive me"), "frontier label")?,
            objective: must(
                NonEmptyText::new("archive filter test"),
                "frontier objective",
            )?,
            slug: Some(must(Slug::new("archive-me"), "frontier slug")?),
        }),
        "create frontier",
    )?;

    let archived = must(
        store.update_frontier(UpdateFrontierRequest {
            frontier: frontier.slug.to_string(),
            expected_revision: Some(frontier.revision),
            label: None,
            objective: None,
            status: Some(FrontierStatus::Archived),
            situation: None,
            unknowns: None,
        }),
        "archive frontier",
    )?;
    assert_eq!(archived.status, FrontierStatus::Archived);
    assert!(
        must(
            store.list_frontiers(ListFrontiersQuery {
                include_archived: false,
            }),
            "list active frontiers",
        )?
        .is_empty()
    );
    assert_eq!(
        must(
            store.list_frontiers(ListFrontiersQuery {
                include_archived: true,
            }),
            "list all frontiers",
        )?
        .len(),
        1
    );
    assert_eq!(
        must(store.read_frontier("archive-me"), "read archived frontier")?.status,
        FrontierStatus::Archived
    );
    assert_eq!(
        must(store.frontier_open("archive-me"), "open archived frontier")?
            .frontier
            .status,
        FrontierStatus::Archived
    );
    Ok(())
}

#[test]
fn archived_frontiers_are_absent_from_mcp_generic_surfaces() -> TestResult {
    let project_root = temp_project_root("archived_frontier_mcp_absence")?;
    init_project(&project_root)?;
    let _ = seed_clean_git_repository(&project_root)?;
    {
        let mut store = must(ProjectStore::open(&project_root), "open store")?;
        let _ = must(
            store.define_metric(DefineMetricRequest {
                key: must(NonEmptyText::new("nodes_solved"), "metric key")?,
                dimension: MetricDimension::Count,
                display_unit: Some(must(MetricUnit::new("count"), "metric unit")?),
                aggregation: MetricAggregation::Point,
                objective: OptimizationObjective::Maximize,
                description: Some(must(
                    NonEmptyText::new("Archive visibility fixture metric"),
                    "metric description",
                )?),
            }),
            "define metric",
        )?;
        for (slug, label) in [
            ("visible", "Visible Frontier"),
            ("archived", "Archived Frontier"),
        ] {
            let _ = must(
                store.create_frontier(CreateFrontierRequest {
                    label: must(NonEmptyText::new(label), "frontier label")?,
                    objective: must(
                        NonEmptyText::new("Ensure archived frontiers vanish from MCP"),
                        "frontier objective",
                    )?,
                    slug: Some(must(Slug::new(slug), "frontier slug")?),
                }),
                "create frontier",
            )?;
            let _ = must(
                store.create_kpi(CreateKpiRequest {
                    frontier: slug.to_owned(),
                    metric: must(NonEmptyText::new("nodes_solved"), "kpi metric")?,
                }),
                "create kpi",
            )?;
        }
        for (frontier, hypothesis, title) in [
            ("visible", "visible-hyp", "Visible Hypothesis"),
            ("archived", "archived-hyp", "Archived Hypothesis"),
        ] {
            let _ = must(
                store.create_hypothesis(CreateHypothesisRequest {
                    frontier: frontier.to_owned(),
                    slug: Some(must(Slug::new(hypothesis), "hypothesis slug")?),
                    title: must(NonEmptyText::new(title), "hypothesis title")?,
                    summary: must(
                        NonEmptyText::new("Archive visibility hypothesis"),
                        "hypothesis summary",
                    )?,
                    body: must(
                        NonEmptyText::new(
                            "Archive visibility fixture hypotheses exist only to verify that archived frontiers disappear completely from MCP generic queries.",
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
        }
        for (hypothesis, slug, title) in [
            ("visible-hyp", "visible-exp", "Visible Experiment"),
            ("archived-hyp", "archived-exp", "Archived Experiment"),
            ("archived-hyp", "archived-open", "Archived Open Experiment"),
        ] {
            let _ = must(
                store.open_experiment(OpenExperimentRequest {
                    hypothesis: hypothesis.to_owned(),
                    slug: Some(must(Slug::new(slug), "experiment slug")?),
                    title: must(NonEmptyText::new(title), "experiment title")?,
                    summary: Some(must(
                        NonEmptyText::new("Archive visibility experiment"),
                        "experiment summary",
                    )?),
                    tags: BTreeSet::new(),
                    parents: Vec::new(),
                }),
                "open experiment",
            )?;
        }
        for (experiment, value, verdict, rationale) in [
            (
                "visible-exp",
                10.0,
                FrontierVerdict::Accepted,
                "Visible frontier result should remain the best visible entry.",
            ),
            (
                "archived-exp",
                999.0,
                FrontierVerdict::Accepted,
                "Archived frontier result should never bleed back into MCP surfaces.",
            ),
        ] {
            let _ = must(
                store.close_experiment(CloseExperimentRequest {
                    experiment: experiment.to_owned(),
                    expected_revision: None,
                    keep_hypothesis_on_worklist: Some(true),
                    backend: ExecutionBackend::Manual,
                    command: CommandRecipe {
                        argv: vec![must(NonEmptyText::new(experiment), "command argv")?],
                        working_directory: None,
                        env: BTreeMap::new(),
                    },
                    dimensions: BTreeMap::new(),
                    primary_metric: Some(ReportedMetricValue {
                        key: must(NonEmptyText::new("nodes_solved"), "metric key")?,
                        value,
                        unit: Some(must(MetricUnit::new("count"), "metric unit")?),
                    }),
                    supporting_metrics: Vec::new(),
                    verdict,
                    rationale: must(NonEmptyText::new(rationale), "rationale")?,
                    analysis: None,
                }),
                "close experiment",
            )?;
        }
        let archived_frontier = must(store.read_frontier("archived"), "read archived frontier")?;
        let _ = must(
            store.update_frontier(UpdateFrontierRequest {
                frontier: "archived".to_owned(),
                expected_revision: Some(archived_frontier.revision),
                label: None,
                objective: None,
                status: Some(FrontierStatus::Archived),
                situation: None,
                unknowns: None,
            }),
            "archive frontier",
        )?;
    }

    let mut harness = McpHarness::spawn(None)?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let bind = harness.bind_project(600, &project_root)?;
    assert_tool_ok(&bind);
    assert_eq!(tool_content(&bind)["frontier_count"].as_u64(), Some(1));
    assert_eq!(tool_content(&bind)["hypothesis_count"].as_u64(), Some(1));
    assert_eq!(tool_content(&bind)["experiment_count"].as_u64(), Some(1));
    assert_eq!(
        tool_content(&bind)["open_experiment_count"].as_u64(),
        Some(0)
    );

    let status = harness.call_tool_full(601, "project.status", json!({}))?;
    assert_tool_ok(&status);
    assert_eq!(tool_content(&status)["frontier_count"].as_u64(), Some(1));
    assert_eq!(tool_content(&status)["hypothesis_count"].as_u64(), Some(1));
    assert_eq!(tool_content(&status)["experiment_count"].as_u64(), Some(1));
    assert_eq!(
        tool_content(&status)["open_experiment_count"].as_u64(),
        Some(0)
    );

    let frontiers = harness.call_tool_full(602, "frontier.list", json!({}))?;
    assert_tool_ok(&frontiers);
    assert_eq!(frontier_slugs(&frontiers), vec!["visible"]);

    let hidden_hypotheses =
        harness.call_tool(603, "hypothesis.list", json!({"frontier": "archived"}))?;
    assert_tool_error(&hidden_hypotheses);

    let hidden_best = harness.call_tool(
        604,
        "metric.best",
        json!({"hypothesis": "archived-hyp", "key": "nodes_solved"}),
    )?;
    assert_tool_error(&hidden_best);

    let best = harness.call_tool_full(
        605,
        "metric.best",
        json!({"key": "nodes_solved", "limit": 1}),
    )?;
    assert_tool_ok(&best);
    let best_entries = must_some(tool_content(&best)["entries"].as_array(), "best entries")?;
    assert_eq!(
        best_entries[0]["experiment"]["slug"].as_str(),
        Some("visible-exp")
    );
    assert_eq!(best_entries[0]["value"].as_f64(), Some(10.0));

    let hidden_anchor = harness.call_tool(
        606,
        "experiment.nearest",
        json!({"experiment": "archived-exp", "metric": "nodes_solved"}),
    )?;
    assert_tool_error(&hidden_anchor);

    let nearest =
        harness.call_tool_full(607, "experiment.nearest", json!({"metric": "nodes_solved"}))?;
    assert_tool_ok(&nearest);
    assert_eq!(
        tool_content(&nearest)["accepted"]["experiment"]["slug"].as_str(),
        Some("visible-exp")
    );
    assert_eq!(
        tool_content(&nearest)["champion"]["experiment"]["slug"].as_str(),
        Some("visible-exp")
    );
    Ok(())
}

#[test]
fn experiment_tags_are_loaded_from_the_junction_table() -> TestResult {
    let root = temp_project_root("experiment_tags_junction")?;
    init_project(&root)?;
    let mut store = must(ProjectStore::open(&root), "open store")?;
    let tag = must(TagName::new("junction-tag"), "tag name")?;
    let _ = must(
        store.register_tag(
            tag.clone(),
            must(NonEmptyText::new("junction tag"), "tag description")?,
        ),
        "register tag",
    )?;
    let frontier = must(
        store.create_frontier(CreateFrontierRequest {
            label: must(NonEmptyText::new("tag frontier"), "frontier label")?,
            objective: must(NonEmptyText::new("tag test"), "frontier objective")?,
            slug: Some(must(Slug::new("tag-frontier"), "frontier slug")?),
        }),
        "create frontier",
    )?;
    let hypothesis = must(
        store.create_hypothesis(CreateHypothesisRequest {
            frontier: frontier.slug.to_string(),
            slug: Some(must(Slug::new("tag-hypothesis"), "hypothesis slug")?),
            title: must(NonEmptyText::new("Tag hypothesis"), "hypothesis title")?,
            summary: must(
                NonEmptyText::new("Tag hypothesis summary"),
                "hypothesis summary",
            )?,
            body: must(NonEmptyText::new("Tag hypothesis body."), "hypothesis body")?,
            expected_yield: HypothesisAssessmentLevel::Medium,
            confidence: HypothesisAssessmentLevel::Medium,
            tags: BTreeSet::new(),
            parents: Vec::new(),
        }),
        "create hypothesis",
    )?;
    let tags = BTreeSet::from([tag.clone()]);
    let experiment = must(
        store.open_experiment(OpenExperimentRequest {
            hypothesis: hypothesis.slug.to_string(),
            slug: Some(must(Slug::new("tag-experiment"), "experiment slug")?),
            title: must(NonEmptyText::new("Tag experiment"), "experiment title")?,
            summary: None,
            tags,
            parents: Vec::new(),
        }),
        "open experiment",
    )?;

    assert_eq!(
        must(
            store.read_experiment(experiment.slug.as_str()),
            "read experiment"
        )?
        .record
        .tags,
        vec![tag.clone()]
    );
    assert_eq!(
        must(
            store.list_experiments(ListExperimentsQuery {
                frontier: Some(frontier.slug.to_string()),
                ..ListExperimentsQuery::default()
            }),
            "list experiments",
        )?
        .into_iter()
        .next()
        .and_then(|summary| summary.tags.into_iter().next()),
        Some(tag)
    );
    Ok(())
}

#[test]
fn metric_rename_and_merge_operate_on_normalized_outcomes() -> TestResult {
    let root = temp_project_root("metric_rename_normalized_outcomes")?;
    init_project(&root)?;
    let _ = seed_clean_git_repository(&root)?;
    let mut store = must(ProjectStore::open(&root), "open store")?;
    for key in ["root_wallclock_ms", "root_elapsed_ms"] {
        let _ = must(
            store.define_metric(DefineMetricRequest {
                key: must(NonEmptyText::new(key), "metric key")?,
                dimension: MetricDimension::Time,
                display_unit: Some(must(MetricUnit::new("ms"), "metric unit")?),
                aggregation: MetricAggregation::Point,
                objective: OptimizationObjective::Minimize,
                description: None,
            }),
            format!("define metric {key}"),
        )?;
    }
    let frontier = must(
        store.create_frontier(CreateFrontierRequest {
            label: must(
                NonEmptyText::new("metric rename frontier"),
                "frontier label",
            )?,
            objective: must(
                NonEmptyText::new("Keep normalized outcome metric keys coherent"),
                "frontier objective",
            )?,
            slug: Some(must(Slug::new("metric-rename-frontier"), "frontier slug")?),
        }),
        "create frontier",
    )?;
    for key in ["root_wallclock_ms", "root_elapsed_ms"] {
        let _ = must(
            store.create_kpi(CreateKpiRequest {
                frontier: frontier.slug.to_string(),
                metric: must(NonEmptyText::new(key), "kpi metric")?,
            }),
            format!("create KPI {key}"),
        )?;
    }
    let hypothesis = must(
        store.create_hypothesis(CreateHypothesisRequest {
            frontier: frontier.slug.to_string(),
            slug: Some(must(Slug::new("metric-rename-hyp"), "hypothesis slug")?),
            title: must(NonEmptyText::new("Metric rename hypothesis"), "hypothesis title")?,
            summary: must(
                NonEmptyText::new("Metric rename should preserve normalized outcomes."),
                "hypothesis summary",
            )?,
            body: must(
                NonEmptyText::new(
                    "Metric rename and merge should operate through metric ids after outcome normalization, so closed experiment rows remain readable and rankable.",
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
    for (slug, metric, value) in [
        ("rename-exp", "root_wallclock_ms", 123.0),
        ("merge-exp", "root_elapsed_ms", 111.0),
    ] {
        let _ = must(
            store.open_experiment(OpenExperimentRequest {
                hypothesis: hypothesis.slug.to_string(),
                slug: Some(must(Slug::new(slug), "experiment slug")?),
                title: must(
                    NonEmptyText::new(format!("{slug} experiment")),
                    "experiment title",
                )?,
                summary: None,
                tags: BTreeSet::new(),
                parents: Vec::new(),
            }),
            format!("open experiment {slug}"),
        )?;
        let _ = must(
            store.close_experiment(CloseExperimentRequest {
                experiment: slug.to_owned(),
                expected_revision: None,
                keep_hypothesis_on_worklist: Some(true),
                backend: ExecutionBackend::Manual,
                command: CommandRecipe {
                    working_directory: None,
                    argv: vec![must(NonEmptyText::new(slug), "command argv")?],
                    env: BTreeMap::new(),
                },
                dimensions: BTreeMap::new(),
                primary_metric: Some(ReportedMetricValue {
                    key: must(NonEmptyText::new(metric), "reported metric")?,
                    value,
                    unit: Some(must(MetricUnit::new("ms"), "reported unit")?),
                }),
                supporting_metrics: Vec::new(),
                verdict: FrontierVerdict::Accepted,
                rationale: must(
                    NonEmptyText::new("Closed metric row for rename regression."),
                    "rationale",
                )?,
                analysis: None,
            }),
            format!("close experiment {slug}"),
        )?;
    }

    let renamed = must(
        store.rename_metric(RenameMetricRequest {
            metric: must(NonEmptyText::new("root_wallclock_ms"), "old metric key")?,
            new_key: must(NonEmptyText::new("root_wallclock"), "new metric key")?,
        }),
        "rename metric",
    )?;
    assert_eq!(renamed.key.as_str(), "root_wallclock");
    assert_eq!(
        must(
            store.read_experiment("rename-exp"),
            "read renamed experiment"
        )?
        .record
        .outcome
        .and_then(|outcome| outcome.primary_metric.map(|metric| metric.key))
        .as_ref()
        .map(NonEmptyText::as_str),
        Some("root_wallclock")
    );

    must(
        store.merge_metric(MergeMetricRequest {
            source: must(NonEmptyText::new("root_elapsed_ms"), "source metric")?,
            target: must(NonEmptyText::new("root_wallclock"), "target metric")?,
        }),
        "merge metric",
    )?;
    let kpis = must(
        store.list_kpis(KpiListQuery {
            frontier: frontier.slug.to_string(),
        }),
        "list KPIs",
    )?;
    assert_eq!(kpis.len(), 1);
    assert_eq!(kpis[0].metric.key.as_str(), "root_wallclock");
    let best = must(
        store.metric_best(MetricBestQuery {
            frontier: Some(frontier.slug.to_string()),
            hypothesis: None,
            key: must(NonEmptyText::new("root_wallclock"), "metric best key")?,
            dimensions: BTreeMap::new(),
            include_rejected: true,
            limit: None,
            order: None,
        }),
        "metric best",
    )?;
    assert_eq!(best.len(), 2);
    assert_eq!(best[0].experiment.slug.as_str(), "merge-exp");
    assert_eq!(best[0].value, 111.0);
    assert_eq!(best[1].experiment.slug.as_str(), "rename-exp");
    assert_eq!(best[1].value, 123.0);
    Ok(())
}

#[test]
fn binding_via_git_directory_resolves_repo_root() -> TestResult {
    let project_root = temp_project_root("git_directory_bind")?;
    init_git_repository(&project_root)?;
    let git_dir = project_root.join(fidget_spinner_store_sqlite::GIT_DIR_NAME);

    let mut harness = McpHarness::spawn(None)?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let bind = harness.bind_project(6, &git_dir)?;
    assert_tool_ok(&bind);
    assert_eq!(
        tool_content(&bind)["project_root"].as_str(),
        Some(project_root.as_str())
    );
    assert_eq!(tool_content(&bind)["frontier_count"].as_u64(), Some(0));
    Ok(())
}

#[test]
fn tag_add_lock_only_rejects_mcp_tag_creation() -> TestResult {
    let project_root = temp_project_root("tag_add_lock")?;
    init_project(&project_root)?;
    {
        let mut store = must(ProjectStore::open(&project_root), "open project store")?;
        let _ = must(
            store.set_registry_lock(SetRegistryLockRequest {
                registry: RegistryName::tags(),
                mode: RegistryLockMode::Definition,
                locked: true,
            }),
            "lock tag registry",
        )?;
        let supervisor_response = store.register_tag(
            must(TagName::new("supervisor-invented"), "tag")?,
            must(
                NonEmptyText::new("supervisor remains authoritative"),
                "description",
            )?,
        );
        assert!(supervisor_response.is_ok());
    }

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let response = harness.call_tool(
        70,
        "tag.add",
        json!({"name": "model-invented", "description": "should be rejected"}),
    )?;
    assert_tool_error(&response);
    assert_eq!(
        tool_content(&response)["kind"].as_str(),
        Some("PolicyViolation")
    );
    assert!(
        must_some(tool_error_message(&response), "policy message")?
            .contains("new tag creation is locked from the Tags page")
    );
    Ok(())
}

#[test]
fn kpi_creation_lock_rejects_mcp_only() -> TestResult {
    let project_root = temp_project_root("kpi_creation_lock")?;
    init_project(&project_root)?;
    {
        let mut store = must(ProjectStore::open(&project_root), "open project store")?;
        let _ = must(
            store.create_frontier(CreateFrontierRequest {
                label: must(NonEmptyText::new("KPI Lock Frontier"), "frontier label")?,
                objective: must(NonEmptyText::new("Govern model KPI promotion"), "objective")?,
                slug: Some(must(Slug::new("kpi-lock"), "frontier slug")?),
            }),
            "create frontier",
        )?;
        for key in ["nodes_solved", "supervisor_nodes"] {
            let _ = must(
                store.define_metric(DefineMetricRequest {
                    key: must(NonEmptyText::new(key), "metric key")?,
                    dimension: MetricDimension::Count,
                    display_unit: Some(must(MetricUnit::new("count"), "metric unit")?),
                    aggregation: MetricAggregation::Point,
                    objective: OptimizationObjective::Maximize,
                    description: None,
                }),
                "define metric",
            )?;
        }
        let _ = must(
            store.set_frontier_registry_lock(SetFrontierRegistryLockRequest {
                registry: RegistryName::kpis(),
                mode: RegistryLockMode::Assignment,
                frontier: "kpi-lock".to_owned(),
                locked: true,
            }),
            "lock frontier KPI creation",
        )?;
        let supervisor_kpi = store.create_kpi(CreateKpiRequest {
            frontier: "kpi-lock".to_owned(),
            metric: must(NonEmptyText::new("supervisor_nodes"), "metric key")?,
        });
        assert!(supervisor_kpi.is_ok());
    }

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let response = harness.call_tool(
        71,
        "kpi.create",
        json!({"frontier": "kpi-lock", "metric": "nodes_solved"}),
    )?;
    assert_tool_error(&response);
    assert_eq!(
        tool_content(&response)["kind"].as_str(),
        Some("PolicyViolation")
    );
    assert!(
        must_some(tool_error_message(&response), "policy message")?
            .contains("MCP KPI creation is locked")
    );
    Ok(())
}

#[test]
fn kpi_order_is_canonical_metric_scope_order() -> TestResult {
    let project_root = temp_project_root("kpi_order")?;
    init_project(&project_root)?;
    let mut store = must(ProjectStore::open(&project_root), "open project store")?;
    let _ = must(
        store.create_frontier(CreateFrontierRequest {
            label: must(NonEmptyText::new("Ordered KPI Frontier"), "frontier label")?,
            objective: must(NonEmptyText::new("Keep KPI order canonical"), "objective")?,
            slug: Some(must(Slug::new("kpi-order"), "frontier slug")?),
        }),
        "create frontier",
    )?;
    for key in ["zeta_nodes", "alpha_nodes"] {
        let _ = must(
            store.define_metric(DefineMetricRequest {
                key: must(NonEmptyText::new(key), "metric key")?,
                dimension: MetricDimension::Count,
                display_unit: Some(must(MetricUnit::new("count"), "metric unit")?),
                aggregation: MetricAggregation::Point,
                objective: OptimizationObjective::Maximize,
                description: None,
            }),
            "define metric",
        )?;
        let _ = must(
            store.create_kpi(CreateKpiRequest {
                frontier: "kpi-order".to_owned(),
                metric: must(NonEmptyText::new(key), "metric key")?,
            }),
            "create KPI",
        )?;
    }

    assert_eq!(
        kpi_metric_keys(&store)?,
        ["zeta_nodes".to_owned(), "alpha_nodes".to_owned()]
    );
    assert_eq!(kpi_ordinals(&store)?, [0, 1]);

    must(
        store.move_kpi(MoveKpiRequest {
            frontier: "kpi-order".to_owned(),
            kpi: "alpha_nodes".to_owned(),
            direction: MoveKpiDirection::Up,
        }),
        "move KPI up",
    )?;
    assert_eq!(
        kpi_metric_keys(&store)?,
        ["alpha_nodes".to_owned(), "zeta_nodes".to_owned()]
    );
    assert_eq!(kpi_scope_metric_keys(&store)?, kpi_metric_keys(&store)?);
    assert_eq!(kpi_ordinals(&store)?, [0, 1]);

    must(
        store.delete_kpi(DeleteKpiRequest {
            frontier: "kpi-order".to_owned(),
            kpi: "alpha_nodes".to_owned(),
        }),
        "delete KPI",
    )?;
    assert_eq!(kpi_metric_keys(&store)?, ["zeta_nodes".to_owned()]);
    assert_eq!(kpi_ordinals(&store)?, [0]);
    Ok(())
}

#[test]
fn kpi_references_are_mcp_settable_normalized_and_queryable() -> TestResult {
    let project_root = temp_project_root("kpi_references")?;
    init_project(&project_root)?;
    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        1160,
        "metric.define",
        json!({
            "key": "root_wallclock",
            "dimension": "time",
            "display_unit": "milliseconds",
            "objective": "minimize",
            "description": "Root solve wallclock.",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        1161,
        "frontier.create",
        json!({
            "label": "KPI reference frontier",
            "objective": "Render baseline reference lines.",
            "slug": "kpi-reference-frontier",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        1162,
        "kpi.create",
        json!({
            "frontier": "kpi-reference-frontier",
            "metric": "root_wallclock",
        }),
    )?);

    let set = harness.call_tool(
        1163,
        "kpi.reference.set",
        json!({
            "frontier": "kpi-reference-frontier",
            "kpi": "root_wallclock",
            "label": "rival",
            "value": 8.5,
            "unit": "seconds",
        }),
    )?;
    assert_tool_ok(&set);
    let set_text = must_some(tool_text(&set), "reference set text")?;
    assert!(set_text.contains("comparison only"));
    assert!(set_text.contains("experiment.close"));
    assert_eq!(
        tool_content(&set)["record"]["label"].as_str(),
        Some("rival")
    );
    assert_eq!(tool_content(&set)["record"]["value"].as_f64(), Some(8500.0));
    assert_eq!(
        tool_content(&set)["record"]["canonical_value"].as_f64(),
        Some(8_500_000_000.0)
    );

    let kpis = harness.call_tool(
        1164,
        "kpi.list",
        json!({"frontier": "kpi-reference-frontier"}),
    )?;
    assert_tool_ok(&kpis);
    assert_eq!(
        tool_content(&kpis)["kpis"][0]["references"][0]["value"].as_f64(),
        Some(8500.0)
    );

    let updated = harness.call_tool(
        1165,
        "kpi.reference.set",
        json!({
            "frontier": "kpi-reference-frontier",
            "kpi": "root_wallclock",
            "label": "rival",
            "value": 8400.0,
        }),
    )?;
    assert_tool_ok(&updated);
    assert_eq!(
        tool_content(&updated)["record"]["value"].as_f64(),
        Some(8400.0)
    );

    let references = harness.call_tool(
        1166,
        "kpi.reference.list",
        json!({"frontier": "kpi-reference-frontier"}),
    )?;
    assert_tool_ok(&references);
    assert_eq!(tool_content(&references)["count"].as_u64(), Some(1));
    assert_eq!(
        tool_content(&references)["references"][0]["canonical_value"].as_f64(),
        Some(8_400_000_000.0)
    );

    let query = harness.call_tool(
        1167,
        "frontier.query.sql",
        json!({
            "frontier": "kpi-reference-frontier",
            "sql": "select metric_key, label, display_value, canonical_value from q_kpi_reference order by reference_ordinal",
        }),
    )?;
    assert_tool_ok(&query);
    let text = must_some(tool_text(&query), "kpi reference query text")?;
    assert!(text.contains("root_wallclock|rival|8400"));
    assert!(text.contains("8400000000"));

    assert_tool_ok(&harness.call_tool(
        1168,
        "kpi.reference.delete",
        json!({
            "frontier": "kpi-reference-frontier",
            "kpi": "root_wallclock",
            "reference": "rival",
        }),
    )?);
    let empty = harness.call_tool(
        1169,
        "kpi.reference.list",
        json!({"frontier": "kpi-reference-frontier"}),
    )?;
    assert_tool_ok(&empty);
    assert_eq!(tool_content(&empty)["count"].as_u64(), Some(0));
    Ok(())
}

fn kpi_metric_keys(store: &ProjectStore) -> TestResult<Vec<String>> {
    Ok(must(
        store.list_kpis(KpiListQuery {
            frontier: "kpi-order".to_owned(),
        }),
        "list KPIs",
    )?
    .into_iter()
    .map(|kpi| kpi.metric.key.to_string())
    .collect())
}

fn kpi_ordinals(store: &ProjectStore) -> TestResult<Vec<u32>> {
    Ok(must(
        store.list_kpis(KpiListQuery {
            frontier: "kpi-order".to_owned(),
        }),
        "list KPIs",
    )?
    .into_iter()
    .map(|kpi| kpi.ordinal.value())
    .collect())
}

fn kpi_scope_metric_keys(store: &ProjectStore) -> TestResult<Vec<String>> {
    Ok(must(
        store.metric_keys(MetricKeysQuery {
            frontier: Some("kpi-order".to_owned()),
            scope: MetricScope::Kpi,
        }),
        "list KPI metric keys",
    )?
    .into_iter()
    .map(|metric| metric.key.to_string())
    .collect())
}

#[test]
fn mandatory_tag_family_rejects_future_mcp_tag_sets() -> TestResult {
    let project_root = temp_project_root("mandatory_tag_family")?;
    init_project(&project_root)?;
    {
        let mut store = must(ProjectStore::open(&project_root), "open project store")?;
        let phase = must(
            store.create_tag_family(CreateTagFamilyRequest {
                name: must(TagFamilyName::new("phase"), "family")?,
                description: must(NonEmptyText::new("experiment phase"), "description")?,
                mandatory: true,
            }),
            "create tag family",
        )?;
        let _ = must(
            store.register_tag(
                must(TagName::new("baseline"), "tag")?,
                must(NonEmptyText::new("baseline phase"), "tag description")?,
            ),
            "register tag",
        )?;
        let _ = must(
            store.assign_tag_family(AssignTagFamilyRequest {
                tag: must(TagName::new("baseline"), "tag")?,
                expected_revision: None,
                family: Some(phase.name),
            }),
            "assign tag family",
        )?;
    }

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
            "label": "Governed Frontier",
            "objective": "Test mandatory family",
            "slug": "governed",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        710,
        "kpi.create",
        json!({
            "frontier": "governed",
            "metric": "nodes_solved",
        }),
    )?);
    let rejected = harness.call_tool(
        72,
        "hypothesis.record",
        json!({
            "frontier": "governed",
            "title": "No phase tag",
            "summary": "Missing mandatory tag family.",
            "body": "One paragraph body.",
            "expected_yield": "medium",
            "confidence": "medium",
        }),
    )?;
    assert_tool_error(&rejected);
    assert!(
        must_some(tool_error_message(&rejected), "mandatory message")?
            .contains("mandatory tag family `phase` is missing")
    );

    let accepted = harness.call_tool(
        73,
        "hypothesis.record",
        json!({
            "frontier": "governed",
            "title": "Tagged phase",
            "summary": "Includes mandatory family.",
            "body": "One paragraph body.",
            "expected_yield": "medium",
            "confidence": "medium",
            "tags": ["baseline"],
        }),
    )?;
    assert_tool_ok(&accepted);
    Ok(())
}

#[test]
fn mcp_hypothesis_record_requires_frontier_kpi() -> TestResult {
    let project_root = temp_project_root("hypothesis_requires_kpi")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        74,
        "frontier.create",
        json!({
            "label": "No KPI Frontier",
            "objective": "Should be blocked before work starts",
            "slug": "no-kpi",
        }),
    )?);

    let rejected = harness.call_tool(
        75,
        "hypothesis.record",
        json!({
            "frontier": "no-kpi",
            "title": "Premature hypothesis",
            "summary": "No KPI exists yet.",
            "body": "One paragraph body.",
            "expected_yield": "medium",
            "confidence": "medium",
        }),
    )?;
    assert_tool_error(&rejected);
    assert_eq!(
        tool_content(&rejected)["kind"].as_str(),
        Some("PolicyViolation")
    );
    assert!(
        must_some(tool_error_message(&rejected), "KPI checkpoint message")?
            .contains("frontier `no-kpi` has no KPI metrics")
    );

    assert_tool_ok(&harness.call_tool(
        76,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        77,
        "kpi.create",
        json!({
            "frontier": "no-kpi",
            "metric": "nodes_solved",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        78,
        "hypothesis.record",
        json!({
            "frontier": "no-kpi",
            "title": "Grounded hypothesis",
            "summary": "KPI exists now.",
            "body": "One paragraph body.",
            "expected_yield": "medium",
            "confidence": "medium",
        }),
    )?);
    Ok(())
}

#[test]
fn mcp_rejects_hypothesis_lifecycle_state() -> TestResult {
    let project_root = temp_project_root("hypothesis_lifecycle_removed")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        79,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        80,
        "frontier.create",
        json!({
            "label": "Retirement Frontier",
            "objective": "Exercise hypothesis lifecycle.",
            "slug": "retire-frontier",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        81,
        "kpi.create",
        json!({
            "frontier": "retire-frontier",
            "metric": "nodes_solved",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        82,
        "hypothesis.record",
        json!({
            "frontier": "retire-frontier",
            "slug": "stale-branch",
            "title": "Stale branch",
            "summary": "This branch remains a visible graph vertex.",
            "body": "One paragraph body.",
            "expected_yield": "medium",
            "confidence": "medium",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        83,
        "hypothesis.record",
        json!({
            "frontier": "retire-frontier",
            "slug": "live-branch",
            "title": "Live branch",
            "summary": "This branch remains active.",
            "body": "One paragraph body.",
            "expected_yield": "medium",
            "confidence": "medium",
        }),
    )?);

    let initial = harness.call_tool_full(
        84,
        "hypothesis.list",
        json!({"frontier": "retire-frontier"}),
    )?;
    assert_tool_ok(&initial);
    assert_eq!(tool_content(&initial)["count"].as_u64(), Some(2));

    let rejected = harness.call_tool(
        85,
        "hypothesis.update",
        json!({
            "hypothesis": "stale-branch",
            "state": "retired",
        }),
    )?;
    assert_tool_error(&rejected);
    assert!(
        must_some(tool_error_message(&rejected), "hypothesis lifecycle error")?
            .contains("hypothesis lifecycle is derived from owned experiments")
    );

    let rejected = harness.call_tool(
        86,
        "hypothesis.update",
        json!({
            "hypothesis": "stale-branch",
            "lifecycle": "closed",
        }),
    )?;
    assert_tool_error(&rejected);
    assert!(
        must_some(tool_error_message(&rejected), "hypothesis lifecycle error")?
            .contains("hypothesis lifecycle is derived from owned experiments")
    );

    assert_tool_ok(&harness.call_tool(
        87,
        "hypothesis.attention.set",
        json!({
            "hypothesis": "stale-branch",
            "attention": "shelved",
        }),
    )?);

    let worklist = harness.call_tool_full(
        88,
        "hypothesis.list",
        json!({"frontier": "retire-frontier"}),
    )?;
    assert_tool_ok(&worklist);
    let worklist_hypotheses = must_some(
        tool_content(&worklist)["hypotheses"].as_array(),
        "hypothesis list",
    )?;
    assert_eq!(worklist_hypotheses.len(), 1);
    assert_eq!(worklist_hypotheses[0]["slug"].as_str(), Some("live-branch"));

    let shelved = harness.call_tool_full(
        89,
        "hypothesis.list",
        json!({"frontier": "retire-frontier", "attention": "shelved"}),
    )?;
    assert_tool_ok(&shelved);
    let shelved_hypotheses = must_some(
        tool_content(&shelved)["hypotheses"].as_array(),
        "hypothesis list",
    )?;
    assert_eq!(shelved_hypotheses.len(), 1);
    assert_eq!(shelved_hypotheses[0]["slug"].as_str(), Some("stale-branch"));
    Ok(())
}

#[test]
fn retired_assignment_lock_does_not_block_mcp_tag_sets() -> TestResult {
    let project_root = temp_project_root("retired_assignment_lock")?;
    init_project(&project_root)?;
    {
        let mut store = must(ProjectStore::open(&project_root), "open project store")?;
        let _ = must(
            store.register_tag(
                must(TagName::new("baseline"), "tag")?,
                must(NonEmptyText::new("baseline phase"), "tag description")?,
            ),
            "register tag",
        )?;
        let _ = must(
            store.set_registry_lock(SetRegistryLockRequest {
                registry: RegistryName::tags(),
                mode: RegistryLockMode::Assignment,
                locked: true,
            }),
            "set retired assignment lock",
        )?;
    }

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        169,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        170,
        "frontier.create",
        json!({
            "label": "Assignment Lock Frontier",
            "objective": "Assignment lock should be inert",
            "slug": "assignment-lock",
        }),
    )?);
    create_nodes_kpi(&mut harness, 1701, "assignment-lock")?;
    assert_tool_ok(&harness.call_tool(
        171,
        "hypothesis.record",
        json!({
            "frontier": "assignment-lock",
            "title": "Tagged despite assignment lock",
            "summary": "The retired assignment lock does not block tag sets.",
            "body": "One paragraph body.",
            "expected_yield": "medium",
            "confidence": "medium",
            "tags": ["baseline"],
        }),
    )?);
    Ok(())
}

#[test]
fn supervisor_tag_creation_can_attach_family_atomically() -> TestResult {
    let project_root = temp_project_root("tag_creation_family")?;
    init_project(&project_root)?;
    let mut store = must(ProjectStore::open(&project_root), "open project store")?;
    let family = must(
        store.create_tag_family(CreateTagFamilyRequest {
            name: must(TagFamilyName::new("surface"), "family")?,
            description: must(NonEmptyText::new("surface classifier"), "description")?,
            mandatory: false,
        }),
        "create tag family",
    )?;
    let tag = must(
        store.register_tag_in_family(
            must(TagName::new("ui"), "tag")?,
            must(NonEmptyText::new("navigator UI work"), "description")?,
            Some(family.name.clone()),
        ),
        "register tag in family",
    )?;
    assert_eq!(tag.family, Some(family.name));

    let rejected = store.register_tag_in_family(
        must(TagName::new("ghost"), "tag")?,
        must(NonEmptyText::new("not committed"), "description")?,
        Some(must(TagFamilyName::new("missing"), "missing family")?),
    );
    assert!(rejected.is_err());
    let ghost = must(TagName::new("ghost"), "tag")?;
    assert!(
        must(store.list_tags(), "list tags")?
            .into_iter()
            .all(|tag| tag.name != ghost)
    );
    Ok(())
}

#[test]
fn tag_locks_do_not_block_supervisor_registry_admin_edits() -> TestResult {
    let project_root = temp_project_root("tag_edit_lock")?;
    init_project(&project_root)?;
    let mut store = must(ProjectStore::open(&project_root), "open project store")?;
    let family = must(
        store.create_tag_family(CreateTagFamilyRequest {
            name: must(TagFamilyName::new("surface"), "family")?,
            description: must(NonEmptyText::new("surface classifier"), "description")?,
            mandatory: false,
        }),
        "create tag family",
    )?;
    let _ = must(
        store.register_tag(
            must(TagName::new("ui"), "tag")?,
            must(NonEmptyText::new("navigator UI work"), "description")?,
        ),
        "register tag",
    )?;
    let _ = must(
        store.register_tag(
            must(TagName::new("spare"), "tag")?,
            must(NonEmptyText::new("delete candidate"), "description")?,
        ),
        "register spare tag",
    )?;
    let _ = must(
        store.set_registry_lock(SetRegistryLockRequest {
            registry: RegistryName::tags(),
            mode: RegistryLockMode::Definition,
            locked: true,
        }),
        "set add lock",
    )?;
    let _ = must(
        store.set_registry_lock(SetRegistryLockRequest {
            registry: RegistryName::tags(),
            mode: RegistryLockMode::Family,
            locked: true,
        }),
        "set edit lock",
    )?;

    assert!(
        store
            .register_tag(
                must(TagName::new("raw"), "tag")?,
                must(NonEmptyText::new("raw tag without family"), "description")?,
            )
            .is_ok()
    );
    let classified = must(
        store.register_tag_in_family(
            must(TagName::new("classified"), "tag")?,
            must(
                NonEmptyText::new("family assignment remains available"),
                "description",
            )?,
            Some(family.name.clone()),
        ),
        "register classified tag",
    )?;
    let ui = must(
        store.assign_tag_family(AssignTagFamilyRequest {
            tag: must(TagName::new("ui"), "tag")?,
            expected_revision: None,
            family: Some(family.name.clone()),
        }),
        "assign tag family",
    )?;
    assert_eq!(ui.family, Some(family.name.clone()));
    let renamed = must(
        store.rename_tag(RenameTagRequest {
            tag: must(TagName::new("ui"), "tag")?,
            expected_revision: Some(ui.revision),
            new_name: must(TagName::new("interface"), "tag")?,
        }),
        "rename tag",
    )?;
    assert_eq!(renamed.name, must(TagName::new("interface"), "tag")?);
    let updated_family = must(
        store.set_tag_family_mandatory(fidget_spinner_store_sqlite::SetTagFamilyMandatoryRequest {
            family: family.name.clone(),
            expected_revision: Some(family.revision),
            mandatory: true,
        }),
        "set family mandatory",
    )?;
    assert!(updated_family.mandatory);
    let _ = must(
        store.merge_tag(MergeTagRequest {
            source: must(TagName::new("raw"), "tag")?,
            expected_revision: None,
            target: classified.name,
        }),
        "merge tag",
    );
    let _ = must(
        store.delete_tag(DeleteTagRequest {
            tag: must(TagName::new("spare"), "tag")?,
            expected_revision: None,
        }),
        "delete tag",
    );
    Ok(())
}
