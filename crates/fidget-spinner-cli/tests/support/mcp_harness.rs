use axum as _;
use clap as _;
use dirs as _;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
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
use libmcp_testkit as _;
use maud as _;
use percent_encoding as _;
use pulldown_cmark as _;
use serde as _;
use serde_json::{Value, json};
use time as _;
use tokio as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn assert_no_opaque_ids(value: &Value) -> Result<(), String> {
    fn walk(value: &Value, path: &str) -> Result<(), String> {
        match value {
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    walk(item, &format!("{path}[{index}]"))?;
                }
            }
            Value::Object(object) => {
                for (key, child) in object {
                    if key == "id" || key.ends_with("_id") {
                        return Err(format!(
                            "{path}.{key}: opaque identifier leaked into model-facing output"
                        ));
                    }
                    walk(child, &format!("{path}.{key}"))?;
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
        Ok(())
    }

    walk(value, "$")
}
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

#[cfg(target_os = "linux")]
fn install_test_executable(project_root: &Utf8PathBuf) -> TestResult<Utf8PathBuf> {
    let installation = project_root.join("installation");
    must(fs::create_dir(&installation), "create test installation")?;
    let canonical = installation.join("fidget-spinner-cli");
    let _installed_bytes = must(
        fs::copy(binary_path(), &canonical),
        "install initial test executable",
    )?;
    Ok(canonical)
}

#[cfg(target_os = "linux")]
fn replace_test_executable(canonical: &Path) -> TestResult<u64> {
    use std::os::unix::fs::MetadataExt;

    let staged = canonical.with_file_name(".fidget-spinner-cli.successor");
    let _staged_bytes = must(
        fs::copy(binary_path(), &staged),
        "stage successor test executable",
    )?;
    must(
        fs::rename(&staged, canonical),
        "atomically publish successor test executable",
    )?;
    Ok(must(
        fs::metadata(canonical),
        "inspect successor executable",
    )?
    .ino())
}

#[cfg(target_os = "linux")]
fn live_executable_inode(process_id: u32) -> TestResult<u64> {
    use std::os::unix::fs::MetadataExt;

    Ok(must(
        fs::metadata(format!("/proc/{process_id}/exe")),
        "inspect live host executable",
    )?
    .ino())
}

#[cfg(target_os = "linux")]
fn wait_for_live_executable(process_id: u32, successor_inode: u64) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if live_executable_inode(process_id)? == successor_inode {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "idle MCP host {process_id} did not adopt successor inode {successor_inode}"
            ))
            .into());
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

struct McpHarness {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl McpHarness {
    fn spawn(project_root: Option<&Utf8PathBuf>) -> TestResult<Self> {
        Self::spawn_from(&binary_path(), project_root)
    }

    fn spawn_from(executable: &Path, project_root: Option<&Utf8PathBuf>) -> TestResult<Self> {
        let state_home = ensure_test_state_home()?;
        let mut command = Command::new(executable);
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

    fn process_id(&self) -> u32 {
        self.child.id()
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
        self.read_response()
    }

    fn notify(&mut self, message: Value) -> TestResult {
        let encoded = must(serde_json::to_string(&message), "notify json")?;
        must(writeln!(self.stdin, "{encoded}"), "write notify")?;
        must(self.stdin.flush(), "flush notify")?;
        Ok(())
    }

    fn write_fragment(&mut self, fragment: &[u8]) -> TestResult {
        must(self.stdin.write_all(fragment), "write request fragment")?;
        must(self.stdin.flush(), "flush request fragment")?;
        Ok(())
    }

    fn read_response(&mut self) -> TestResult<Value> {
        let mut line = String::new();
        let byte_count = must(self.stdout.read_line(&mut line), "read response")?;
        if byte_count == 0 {
            return Err(io::Error::other("unexpected EOF reading response").into());
        }
        must(serde_json::from_str(&line), "response json")
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
