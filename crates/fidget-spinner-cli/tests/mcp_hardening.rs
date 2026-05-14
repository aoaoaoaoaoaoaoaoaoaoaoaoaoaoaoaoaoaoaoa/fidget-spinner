use axum as _;
use clap as _;
use dirs as _;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::OnceLock;

use camino::Utf8PathBuf;
use fidget_spinner_core::{
    CommandRecipe, ExecutionBackend, FieldValueType, FrontierStatus, FrontierVerdict,
    HypothesisAssessmentLevel, MetricAggregation, MetricDimension, MetricUnit, NonEmptyText,
    OptimizationObjective, RegistryLockMode, RegistryName, ReportedMetricValue, RunDimensionValue,
    Slug, SyntheticMetricExpression, TagFamilyName, TagName,
};
use fidget_spinner_store_sqlite::{
    AssignTagFamilyRequest, CloseExperimentRequest, CreateFrontierRequest, CreateHypothesisRequest,
    CreateKpiRequest, CreateTagFamilyRequest, DefineMetricRequest, DefineRunDimensionRequest,
    DefineSyntheticMetricRequest, DeleteKpiRequest, DeleteTagRequest, FrontierSqlQuery,
    KpiListQuery, ListExperimentsQuery, ListFrontiersQuery, MergeMetricRequest, MergeTagRequest,
    MetricBestQuery, MetricKeysQuery, MetricScope, MoveKpiDirection, MoveKpiRequest,
    OpenExperimentRequest, ProjectStore, RenameMetricRequest, RenameTagRequest,
    SetFrontierRegistryLockRequest, SetRegistryLockRequest, UpdateFrontierRequest,
};
use libmcp as _;
use libmcp_testkit::assert_no_opaque_ids;
use maud as _;
use percent_encoding as _;
use plotters as _;
use serde as _;
use serde_json::{Value, json};
use time as _;
use tokio as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn ensure_test_state_home() -> TestResult<&'static Utf8PathBuf> {
    static STATE_HOME: OnceLock<Result<Utf8PathBuf, String>> = OnceLock::new();
    match STATE_HOME.get_or_init(|| {
        let root = std::env::temp_dir().join("fidget_spinner_test_state_home");
        fs::create_dir_all(&root).map_err(|error| format!("create temp state home: {error}"))?;
        let root = Utf8PathBuf::from(root.to_string_lossy().into_owned());
        fidget_spinner_store_sqlite::install_state_home_override(&root)
            .map_err(|error| format!("install state home override: {error}"))?;
        Ok(root)
    }) {
        Ok(path) => Ok(path),
        Err(error) => Err(io::Error::other(error.clone()).into()),
    }
}

fn must<T, E: std::fmt::Display, C: std::fmt::Display>(
    result: Result<T, E>,
    context: C,
) -> TestResult<T> {
    result.map_err(|error| io::Error::other(format!("{context}: {error}")).into())
}

fn must_some<T>(value: Option<T>, context: &str) -> TestResult<T> {
    value.ok_or_else(|| io::Error::other(context).into())
}

fn temp_project_root(name: &str) -> TestResult<Utf8PathBuf> {
    let _ = ensure_test_state_home()?;
    let root = std::env::temp_dir().join(format!(
        "fidget_spinner_mcp_{name}_{}_{}",
        std::process::id(),
        must(
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH),
            "current time after unix epoch",
        )?
        .as_nanos()
    ));
    must(fs::create_dir_all(&root), "create temp project root")?;
    Ok(Utf8PathBuf::from(root.to_string_lossy().into_owned()))
}

fn init_project(root: &Utf8PathBuf) -> TestResult {
    let _ = ensure_test_state_home()?;
    let _store = must(
        ProjectStore::init(
            root,
            must(NonEmptyText::new("mcp test project"), "display name")?,
        ),
        "init project store",
    )?;
    Ok(())
}

fn init_git_repository(root: &Utf8PathBuf) -> TestResult {
    let status = must(
        Command::new("git")
            .arg("init")
            .arg("-q")
            .arg(root.as_str())
            .status(),
        "run git init",
    )?;
    if !status.success() {
        return Err(io::Error::other("git init failed").into());
    }
    Ok(())
}

fn run_git(root: &Utf8PathBuf, args: &[&str]) -> TestResult<String> {
    let output = must(
        Command::new("git")
            .arg("-C")
            .arg(root.as_str())
            .args(args)
            .output(),
        format!("run git {}", args.join(" ")),
    )?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn seed_clean_git_repository(root: &Utf8PathBuf) -> TestResult<String> {
    init_git_repository(root)?;
    must(
        fs::write(root.join("seed.txt"), "seed\n"),
        "write git seed file",
    )?;
    let _ = run_git(root, &["add", "seed.txt"])?;
    let _ = run_git(
        root,
        &[
            "-c",
            "user.name=Fidget Spinner Tests",
            "-c",
            "user.email=fidget-spinner-tests@example.invalid",
            "commit",
            "-q",
            "-m",
            "seed",
        ],
    )?;
    run_git(root, &["rev-parse", "HEAD"])
}

fn binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_fidget-spinner-cli"))
}

struct McpHarness {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl McpHarness {
    fn spawn(project_root: Option<&Utf8PathBuf>) -> TestResult<Self> {
        let state_home = ensure_test_state_home()?;
        let mut command = Command::new(binary_path());
        let _ = command
            .arg("mcp")
            .arg("serve")
            .env("FIDGET_SPINNER_STATE_HOME", state_home.as_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if let Some(project_root) = project_root {
            let _ = command.arg("--project").arg(project_root.as_str());
        }
        let mut child = must(command.spawn(), "spawn mcp host")?;
        let stdin = must_some(child.stdin.take(), "host stdin")?;
        let stdout = BufReader::new(must_some(child.stdout.take(), "host stdout")?);
        Ok(Self {
            child,
            stdin,
            stdout,
        })
    }

    fn initialize(&mut self) -> TestResult<Value> {
        self.request(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": { "name": "mcp-hardening-test", "version": "0" }
            }
        }))
    }

    fn notify_initialized(&mut self) -> TestResult {
        self.notify(json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }))
    }

    fn tools_list(&mut self) -> TestResult<Value> {
        self.request(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {},
        }))
    }

    fn bind_project(&mut self, id: u64, path: &Utf8PathBuf) -> TestResult<Value> {
        self.call_tool(id, "project.bind", json!({ "path": path.as_str() }))
    }

    fn call_tool(&mut self, id: u64, name: &str, arguments: Value) -> TestResult<Value> {
        self.request(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments,
            }
        }))
    }

    fn call_tool_full(&mut self, id: u64, name: &str, arguments: Value) -> TestResult<Value> {
        let mut arguments = arguments.as_object().cloned().unwrap_or_default();
        let _ = arguments.insert("render".to_owned(), json!("json"));
        let _ = arguments.insert("detail".to_owned(), json!("full"));
        self.call_tool(id, name, Value::Object(arguments))
    }

    fn request(&mut self, message: Value) -> TestResult<Value> {
        let encoded = must(serde_json::to_string(&message), "request json")?;
        must(writeln!(self.stdin, "{encoded}"), "write request")?;
        must(self.stdin.flush(), "flush request")?;
        let mut line = String::new();
        let byte_count = must(self.stdout.read_line(&mut line), "read response")?;
        if byte_count == 0 {
            return Err(io::Error::other("unexpected EOF reading response").into());
        }
        must(serde_json::from_str(&line), "response json")
    }

    fn notify(&mut self, message: Value) -> TestResult {
        let encoded = must(serde_json::to_string(&message), "notify json")?;
        must(writeln!(self.stdin, "{encoded}"), "write notify")?;
        must(self.stdin.flush(), "flush notify")?;
        Ok(())
    }
}

impl Drop for McpHarness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn tool_content(response: &Value) -> &Value {
    &response["result"]["structuredContent"]
}

fn tool_text(response: &Value) -> Option<&str> {
    response["result"]["content"][0]["text"].as_str()
}

fn tool_error_message(response: &Value) -> Option<&str> {
    response["result"]["structuredContent"]["message"].as_str()
}

fn assert_tool_ok(response: &Value) {
    assert_eq!(
        response["result"]["isError"].as_bool(),
        Some(false),
        "tool response unexpectedly errored: {response:#}"
    );
}

fn assert_tool_error(response: &Value) {
    assert_eq!(
        response["result"]["isError"].as_bool(),
        Some(true),
        "tool response unexpectedly succeeded: {response:#}"
    );
}

fn create_nodes_kpi(harness: &mut McpHarness, id: u64, frontier: &str) -> TestResult {
    assert_tool_ok(&harness.call_tool(
        id,
        "kpi.create",
        json!({
            "frontier": frontier,
            "metric": "nodes_solved",
        }),
    )?);
    Ok(())
}

fn seed_frontier_query_fixture(harness: &mut McpHarness) -> TestResult {
    assert_tool_ok(&harness.call_tool(
        3000,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "dimension": "count",
            "display_unit": "count",
            "objective": "maximize",
            "description": "Node count for query fixture.",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        3001,
        "condition.define",
        json!({"key": "instance", "value_type": "string"}),
    )?);
    for (offset, frontier, label, value) in [
        (0, "query-alpha", "Query alpha", 111.0),
        (10, "query-beta", "Query beta", 999.0),
    ] {
        assert_tool_ok(&harness.call_tool(
            3010 + offset,
            "frontier.create",
            json!({
                "label": label,
                "objective": "Fixture frontier for scoped SQL queries",
                "slug": frontier,
            }),
        )?);
        create_nodes_kpi(harness, 3011 + offset, frontier)?;
        assert_tool_ok(&harness.call_tool(
            3012 + offset,
            "hypothesis.record",
            json!({
                "frontier": frontier,
                "slug": format!("{frontier}-hypothesis"),
                "title": format!("{label} hypothesis"),
                "summary": "Scoped SQL should only see this frontier when selected.",
                "body": "The query fixture records one closed experiment so scoped SQL can prove isolation.",
                "expected_yield": "medium",
                "confidence": "medium",
            }),
        )?);
        assert_tool_ok(&harness.call_tool(
            3013 + offset,
            "experiment.open",
            json!({
                "hypothesis": format!("{frontier}-hypothesis"),
                "slug": format!("{frontier}-run"),
                "title": format!("{label} run"),
            }),
        )?);
        assert_tool_ok(&harness.call_tool(
            3014 + offset,
            "experiment.close",
            json!({
                "experiment": format!("{frontier}-run"),
                "keep_hypothesis_on_worklist": true,
                "backend": "manual",
                "command": {"argv": [format!("{frontier}-command")]},
                "conditions": {"instance": frontier},
                "primary_metric": {"key": "nodes_solved", "value": value},
                "verdict": "accepted",
                "rationale": format!("{label} result belongs only to {frontier}."),
            }),
        )?);
    }
    Ok(())
}

fn tool_names(response: &Value) -> Vec<&str> {
    response["result"]["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["name"].as_str())
        .collect()
}

fn frontier_slugs(response: &Value) -> Vec<&str> {
    tool_content(response)["frontiers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|frontier| frontier["slug"].as_str())
        .collect()
}

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
                    primary_metric: ReportedMetricValue {
                        key: must(NonEmptyText::new("nodes_solved"), "metric key")?,
                        value,
                        unit: Some(must(MetricUnit::new("count"), "metric unit")?),
                    },
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
                primary_metric: ReportedMetricValue {
                    key: must(NonEmptyText::new(metric), "reported metric")?,
                    value,
                    unit: Some(must(MetricUnit::new("ms"), "reported unit")?),
                },
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
        .map(|outcome| outcome.primary_metric.key)
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

    assert_tool_ok(&harness.call_tool(
        86,
        "hypothesis.attention.set",
        json!({
            "hypothesis": "stale-branch",
            "attention": "shelved",
        }),
    )?);

    let worklist = harness.call_tool_full(
        87,
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
        88,
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
    assert_no_opaque_ids(content).map_err(|error| io::Error::other(error.to_string()))?;
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

    let placeholder = harness.call_tool(
        25,
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
    assert_no_opaque_ids(content).map_err(|error| io::Error::other(error.to_string()))?;
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

    for metric in ["work_done", "elapsed_time", "work_rate"] {
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
            primary_metric: ReportedMetricValue {
                key: must(NonEmptyText::new("work_done"), "primary metric")?,
                value: 240.0,
                unit: None,
            },
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
