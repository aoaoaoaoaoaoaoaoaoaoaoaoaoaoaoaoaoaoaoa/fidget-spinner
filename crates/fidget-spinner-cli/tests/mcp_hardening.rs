use axum as _;
use clap as _;
use dirs as _;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use camino::Utf8PathBuf;
use fidget_spinner_core::{NonEmptyText, Slug};
use fidget_spinner_store_sqlite::{CreateFrontierRequest, ProjectStore};
use libmcp as _;
use maud as _;
use percent_encoding as _;
use serde as _;
use serde_json::{Value, json};
use time as _;
use tokio as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

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
    let _store = must(
        ProjectStore::init(
            root,
            must(NonEmptyText::new("mcp test project"), "display name")?,
        ),
        "init project store",
    )?;
    Ok(())
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
        let mut command = Command::new(binary_path());
        let _ = command
            .arg("mcp")
            .arg("serve")
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
    assert!(tool_names.contains(&"hypothesis.record"));
    assert!(tool_names.contains(&"experiment.close"));
    assert!(tool_names.contains(&"artifact.record"));
    assert!(!tool_names.contains(&"node.list"));
    assert!(!tool_names.contains(&"research.record"));

    let health = harness.call_tool(3, "system.health", json!({}))?;
    assert_tool_ok(&health);
    assert_eq!(tool_content(&health)["bound"].as_bool(), Some(false));

    let bind = harness.bind_project(4, &project_root)?;
    assert_tool_ok(&bind);
    assert_eq!(
        tool_content(&bind)["display_name"].as_str(),
        Some("mcp test project")
    );

    let rebound_health = harness.call_tool(5, "system.health", json!({}))?;
    assert_tool_ok(&rebound_health);
    assert_eq!(tool_content(&rebound_health)["bound"].as_bool(), Some(true));
    Ok(())
}

#[test]
fn frontier_open_is_the_grounding_surface_for_live_state() -> TestResult {
    let project_root = temp_project_root("frontier_open")?;
    init_project(&project_root)?;

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
            "unit": "count",
            "objective": "maximize",
            "visibility": "canonical",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        12,
        "run.dimension.define",
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
    assert_tool_ok(&harness.call_tool(
        14,
        "hypothesis.record",
        json!({
            "frontier": "lp-root",
            "slug": "node-local-loop",
            "title": "Node-local logical cut loop",
            "summary": "Push cut cash-out below root.",
            "body": "Thread node-local logical cuts through native LP reoptimization so the same intervention can cash out below root on parity rails without corrupting root ownership semantics.",
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
            "backend": "manual",
            "command": {"argv": ["baseline-20s"]},
            "dimensions": {"instance": "4x5-braid"},
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
    let active_hypotheses = must_some(
        content["active_hypotheses"].as_array(),
        "active hypotheses array",
    )?;
    assert_eq!(active_hypotheses.len(), 1);
    assert_eq!(
        active_hypotheses[0]["hypothesis"]["slug"].as_str(),
        Some("node-local-loop")
    );
    assert_eq!(
        active_hypotheses[0]["latest_closed_experiment"]["slug"].as_str(),
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
    assert!(content.get("artifacts").is_none());
    assert!(active_hypotheses[0]["hypothesis"].get("body").is_none());
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
        }),
    )?;
    assert_tool_error(&response);
    assert!(must_some(tool_error_message(&response), "fault message")?.contains("paragraph"));
    Ok(())
}

#[test]
fn artifact_surface_preserves_reference_only() -> TestResult {
    let project_root = temp_project_root("artifact_reference")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        30,
        "frontier.create",
        json!({
            "label": "Artifacts frontier",
            "objective": "Keep dumps out of the token hot path",
            "slug": "artifacts",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        31,
        "hypothesis.record",
        json!({
            "frontier": "artifacts",
            "slug": "sourced-hypothesis",
            "title": "Sourced hypothesis",
            "summary": "Attach a large external source by reference only.",
            "body": "Treat large external writeups as artifact references rather than inline context so the ledger stays scientifically austere.",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        32,
        "artifact.record",
        json!({
            "kind": "document",
            "slug": "lp-review-doc",
            "label": "LP review tranche",
            "summary": "External markdown tranche.",
            "locator": "/tmp/lp-review.md",
            "attachments": [{"kind": "hypothesis", "selector": "sourced-hypothesis"}],
        }),
    )?);

    let artifact =
        harness.call_tool_full(33, "artifact.read", json!({"artifact": "lp-review-doc"}))?;
    assert_tool_ok(&artifact);
    let content = tool_content(&artifact);
    assert_eq!(
        content["record"]["locator"].as_str(),
        Some("/tmp/lp-review.md")
    );
    assert!(content["record"].get("body").is_none());
    assert_eq!(
        must_some(content["attachments"].as_array(), "artifact attachments")?[0]["kind"].as_str(),
        Some("hypothesis")
    );
    Ok(())
}

#[test]
fn experiment_close_drives_metric_best_and_analysis() -> TestResult {
    let project_root = temp_project_root("metric_best")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root))?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    assert_tool_ok(&harness.call_tool(
        40,
        "metric.define",
        json!({
            "key": "nodes_solved",
            "unit": "count",
            "objective": "maximize",
            "visibility": "canonical",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        41,
        "run.dimension.define",
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
    assert_tool_ok(&harness.call_tool(
        43,
        "hypothesis.record",
        json!({
            "frontier": "metric-frontier",
            "slug": "reopt-dominance",
            "title": "Node reopt dominates native LP spend",
            "summary": "Track node LP wallclock concentration on braid rails.",
            "body": "Matched LP site traces indicate native LP spend is dominated by node reoptimization on the braid rails, so the next interventions should target node-local LP churn instead of root-only machinery.",
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
            "backend": "manual",
            "command": {"argv": ["trace-baseline"]},
            "dimensions": {"instance": "4x5-braid"},
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
            "backend": "manual",
            "command": {"argv": ["matched-lp-site-traces"]},
            "dimensions": {"instance": "4x5-braid"},
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
    assert_eq!(
        content["record"]["outcome"]["verdict"].as_str(),
        Some("accepted")
    );
    assert_eq!(
        content["record"]["outcome"]["analysis"]["summary"].as_str(),
        Some("Node LP work is now the primary native sink.")
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
        61,
        "frontier.create",
        json!({
            "label": "alpha frontier",
            "objective": "first seeded frontier",
            "slug": "alpha",
        }),
    )?);
    let alpha_list = harness.call_tool_full(62, "frontier.list", json!({}))?;
    assert_tool_ok(&alpha_list);
    assert_eq!(frontier_slugs(&alpha_list), vec!["alpha"]);

    must(
        fs::remove_dir_all(project_root.join(fidget_spinner_store_sqlite::STORE_DIR_NAME)),
        "remove project store",
    )?;
    init_project(&project_root)?;
    let mut reopened = must(ProjectStore::open(&project_root), "open recreated store")?;
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

    let beta_list = harness.call_tool_full(63, "frontier.list", json!({}))?;
    assert_tool_ok(&beta_list);
    assert_eq!(frontier_slugs(&beta_list), vec!["beta"]);
    Ok(())
}
