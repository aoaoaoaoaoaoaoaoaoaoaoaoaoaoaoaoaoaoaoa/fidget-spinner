use axum as _;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use camino::Utf8PathBuf;
use clap as _;
use dirs as _;
use fidget_spinner_core::NonEmptyText;
use fidget_spinner_store_sqlite::{ListNodesQuery, ProjectStore};
use libmcp as _;
use linkify as _;
use maud as _;
use serde as _;
use serde_json::{Value, json};
use time as _;
use tokio as _;
use uuid as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn must<T, E: std::fmt::Display>(result: Result<T, E>, context: &str) -> TestResult<T> {
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
            must(NonEmptyText::new("local.mcp.test"), "namespace")?,
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
    fn spawn(project_root: Option<&Utf8PathBuf>, envs: &[(&str, String)]) -> TestResult<Self> {
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
        for (key, value) in envs {
            let _ = command.env(key, value);
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
    response["result"]["content"]
        .as_array()
        .and_then(|content| content.first())
        .and_then(|entry| entry["text"].as_str())
}

#[test]
fn cold_start_exposes_health_and_telemetry() -> TestResult {
    let project_root = temp_project_root("cold_start")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let initialize = harness.initialize()?;
    assert_eq!(
        initialize["result"]["protocolVersion"].as_str(),
        Some("2025-11-25")
    );
    harness.notify_initialized()?;

    let tools = harness.tools_list()?;
    let tool_count = must_some(tools["result"]["tools"].as_array(), "tools array")?.len();
    assert!(tool_count >= 20);

    let health = harness.call_tool(3, "system.health", json!({}))?;
    assert_eq!(tool_content(&health)["ready"].as_bool(), Some(true));
    assert_eq!(tool_content(&health)["bound"].as_bool(), Some(false));

    let telemetry = harness.call_tool(4, "system.telemetry", json!({}))?;
    assert!(tool_content(&telemetry)["requests"].as_u64().unwrap_or(0) >= 3);

    let skills = harness.call_tool(15, "skill.list", json!({}))?;
    let skill_names = must_some(
        tool_content(&skills)["skills"].as_array(),
        "bundled skills array",
    )?
    .iter()
    .filter_map(|skill| skill["name"].as_str())
    .collect::<Vec<_>>();
    assert!(skill_names.contains(&"fidget-spinner"));
    assert!(skill_names.contains(&"frontier-loop"));

    let base_skill = harness.call_tool(16, "skill.show", json!({"name": "fidget-spinner"}))?;
    assert_eq!(
        tool_content(&base_skill)["name"].as_str(),
        Some("fidget-spinner")
    );
    Ok(())
}

#[test]
fn tool_output_defaults_to_porcelain_and_supports_json_render() -> TestResult {
    let project_root = temp_project_root("render_modes")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(21, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let porcelain = harness.call_tool(22, "project.status", json!({}))?;
    let porcelain_text = must_some(tool_text(&porcelain), "porcelain project.status text")?;
    assert!(porcelain_text.contains("root:"));
    assert!(!porcelain_text.contains("\"project_root\":"));

    let health = harness.call_tool(23, "system.health", json!({}))?;
    let health_text = must_some(tool_text(&health), "porcelain system.health text")?;
    assert!(health_text.contains("ready | bound"));
    assert!(health_text.contains("binary:"));

    let frontier = harness.call_tool(
        24,
        "frontier.init",
        json!({
            "label": "render frontier",
            "objective": "exercise porcelain output",
            "contract_title": "render contract",
            "benchmark_suites": ["smoke"],
            "promotion_criteria": ["retain key fields in porcelain"],
            "primary_metric": {
                "key": "score",
                "unit": "count",
                "objective": "maximize"
            }
        }),
    )?;
    assert_eq!(frontier["result"]["isError"].as_bool(), Some(false));

    let frontier_list = harness.call_tool(25, "frontier.list", json!({}))?;
    let frontier_text = must_some(tool_text(&frontier_list), "porcelain frontier.list text")?;
    assert!(frontier_text.contains("render frontier"));
    assert!(!frontier_text.contains("root_contract_node_id"));

    let json_render = harness.call_tool(26, "project.status", json!({"render": "json"}))?;
    let json_text = must_some(tool_text(&json_render), "json project.status text")?;
    assert!(json_text.contains("\"project_root\":"));
    assert!(json_text.trim_start().starts_with('{'));

    let json_full = harness.call_tool(
        27,
        "project.status",
        json!({"render": "json", "detail": "full"}),
    )?;
    let json_full_text = must_some(tool_text(&json_full), "json full project.status text")?;
    assert!(json_full_text.contains("\"schema\": {"));
    Ok(())
}

#[test]
fn safe_request_retries_after_worker_crash() -> TestResult {
    let project_root = temp_project_root("crash_retry")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(
        None,
        &[(
            "FIDGET_SPINNER_MCP_TEST_HOST_CRASH_ONCE_KEY",
            "tools/call:project.status".to_owned(),
        )],
    )?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(3, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let response = harness.call_tool(5, "project.status", json!({}))?;
    assert_eq!(response["result"]["isError"].as_bool(), Some(false));

    let telemetry = harness.call_tool(6, "system.telemetry", json!({}))?;
    assert_eq!(tool_content(&telemetry)["retries"].as_u64(), Some(1));
    assert_eq!(
        tool_content(&telemetry)["worker_restarts"].as_u64(),
        Some(1)
    );
    Ok(())
}

#[test]
fn safe_request_retries_after_worker_transient_fault() -> TestResult {
    let project_root = temp_project_root("transient_retry")?;
    init_project(&project_root)?;
    let marker = project_root.join("transient_once.marker");

    let mut harness = McpHarness::spawn(
        None,
        &[
            (
                "FIDGET_SPINNER_MCP_TEST_WORKER_TRANSIENT_ONCE_KEY",
                "tools/call:project.status".to_owned(),
            ),
            (
                "FIDGET_SPINNER_MCP_TEST_WORKER_TRANSIENT_ONCE_MARKER",
                marker.to_string(),
            ),
        ],
    )?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(12, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let response = harness.call_tool(13, "project.status", json!({}))?;
    assert_eq!(response["result"]["isError"].as_bool(), Some(false));

    let telemetry = harness.call_tool(14, "system.telemetry", json!({}))?;
    assert_eq!(tool_content(&telemetry)["retries"].as_u64(), Some(1));
    assert_eq!(
        tool_content(&telemetry)["worker_restarts"].as_u64(),
        Some(1)
    );
    Ok(())
}

#[test]
fn side_effecting_request_is_not_replayed_after_worker_crash() -> TestResult {
    let project_root = temp_project_root("no_replay")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(
        None,
        &[(
            "FIDGET_SPINNER_MCP_TEST_HOST_CRASH_ONCE_KEY",
            "tools/call:research.record".to_owned(),
        )],
    )?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(6, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let response = harness.call_tool(
        7,
        "research.record",
        json!({
            "title": "should not duplicate",
            "body": "host crash before worker execution",
        }),
    )?;
    assert_eq!(response["result"]["isError"].as_bool(), Some(true));

    let nodes = harness.call_tool(8, "node.list", json!({}))?;
    assert_eq!(
        must_some(tool_content(&nodes).as_array(), "node list")?.len(),
        0
    );

    let telemetry = harness.call_tool(9, "system.telemetry", json!({}))?;
    assert_eq!(tool_content(&telemetry)["retries"].as_u64(), Some(0));
    Ok(())
}

#[test]
fn forced_rollout_preserves_initialized_state() -> TestResult {
    let project_root = temp_project_root("rollout")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(
        None,
        &[(
            "FIDGET_SPINNER_MCP_TEST_FORCE_ROLLOUT_KEY",
            "tools/call:project.status".to_owned(),
        )],
    )?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(9, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let first = harness.call_tool(10, "project.status", json!({}))?;
    assert_eq!(first["result"]["isError"].as_bool(), Some(false));

    let second = harness.call_tool(11, "project.status", json!({}))?;
    assert_eq!(second["result"]["isError"].as_bool(), Some(false));

    let telemetry = harness.call_tool(12, "system.telemetry", json!({}))?;
    assert_eq!(tool_content(&telemetry)["host_rollouts"].as_u64(), Some(1));
    Ok(())
}

#[test]
fn unbound_project_tools_fail_with_bind_hint() -> TestResult {
    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let response = harness.call_tool(20, "project.status", json!({}))?;
    assert_eq!(response["result"]["isError"].as_bool(), Some(true));
    let message = response["result"]["structuredContent"]["message"].as_str();
    assert!(message.is_some_and(|message| message.contains("project.bind")));
    Ok(())
}

#[test]
fn bind_bootstraps_empty_project_root() -> TestResult {
    let project_root = temp_project_root("bind_bootstrap")?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let bind = harness.bind_project(28, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));
    assert_eq!(
        tool_content(&bind)["project_root"].as_str(),
        Some(project_root.as_str())
    );

    let status = harness.call_tool(29, "project.status", json!({}))?;
    assert_eq!(status["result"]["isError"].as_bool(), Some(false));
    assert_eq!(
        tool_content(&status)["project_root"].as_str(),
        Some(project_root.as_str())
    );

    let store = must(ProjectStore::open(&project_root), "open bootstrapped store")?;
    assert_eq!(store.project_root().as_str(), project_root.as_str());
    Ok(())
}

#[test]
fn bind_rejects_nonempty_uninitialized_root() -> TestResult {
    let project_root = temp_project_root("bind_nonempty")?;
    must(
        fs::write(project_root.join("README.txt").as_std_path(), "occupied"),
        "seed nonempty directory",
    )?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let bind = harness.bind_project(30, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(true));
    Ok(())
}

#[test]
fn bind_retargets_writes_to_sibling_project_root() -> TestResult {
    let spinner_root = temp_project_root("spinner_root")?;
    let libgrid_root = temp_project_root("libgrid_root")?;
    init_project(&spinner_root)?;
    init_project(&libgrid_root)?;
    let notes_dir = libgrid_root.join("notes");
    must(
        fs::create_dir_all(notes_dir.as_std_path()),
        "create nested notes dir",
    )?;

    let mut harness = McpHarness::spawn(Some(&spinner_root), &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let initial_status = harness.call_tool(31, "project.status", json!({}))?;
    assert_eq!(
        tool_content(&initial_status)["project_root"].as_str(),
        Some(spinner_root.as_str())
    );

    let rebind = harness.bind_project(32, &notes_dir)?;
    assert_eq!(rebind["result"]["isError"].as_bool(), Some(false));
    assert_eq!(
        tool_content(&rebind)["project_root"].as_str(),
        Some(libgrid_root.as_str())
    );

    let status = harness.call_tool(33, "project.status", json!({}))?;
    assert_eq!(
        tool_content(&status)["project_root"].as_str(),
        Some(libgrid_root.as_str())
    );

    let note = harness.call_tool(
        34,
        "note.quick",
        json!({
            "title": "libgrid dogfood note",
            "body": "rebind should redirect writes",
            "tags": [],
        }),
    )?;
    assert_eq!(note["result"]["isError"].as_bool(), Some(false));

    let spinner_store = must(ProjectStore::open(&spinner_root), "open spinner store")?;
    let libgrid_store = must(ProjectStore::open(&libgrid_root), "open libgrid store")?;
    assert_eq!(
        must(
            spinner_store.list_nodes(ListNodesQuery::default()),
            "list spinner nodes after rebind"
        )?
        .len(),
        0
    );
    assert_eq!(
        must(
            libgrid_store.list_nodes(ListNodesQuery::default()),
            "list libgrid nodes after rebind"
        )?
        .len(),
        1
    );
    Ok(())
}

#[test]
fn tag_registry_drives_note_creation_and_lookup() -> TestResult {
    let project_root = temp_project_root("tag_registry")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(40, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let missing_tags = harness.call_tool(
        41,
        "note.quick",
        json!({
            "title": "untagged",
            "body": "should fail",
        }),
    )?;
    assert_eq!(missing_tags["result"]["isError"].as_bool(), Some(true));

    let tag = harness.call_tool(
        42,
        "tag.add",
        json!({
            "name": "dogfood/mcp",
            "description": "MCP dogfood observations",
        }),
    )?;
    assert_eq!(tag["result"]["isError"].as_bool(), Some(false));

    let tag_list = harness.call_tool(43, "tag.list", json!({}))?;
    let tags = must_some(tool_content(&tag_list).as_array(), "tag list")?;
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["name"].as_str(), Some("dogfood/mcp"));

    let note = harness.call_tool(
        44,
        "note.quick",
        json!({
            "title": "tagged note",
            "body": "tagged lookup should work",
            "tags": ["dogfood/mcp"],
        }),
    )?;
    assert_eq!(note["result"]["isError"].as_bool(), Some(false));

    let filtered = harness.call_tool(45, "node.list", json!({"tags": ["dogfood/mcp"]}))?;
    let nodes = must_some(tool_content(&filtered).as_array(), "filtered nodes")?;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["tags"][0].as_str(), Some("dogfood/mcp"));
    Ok(())
}
