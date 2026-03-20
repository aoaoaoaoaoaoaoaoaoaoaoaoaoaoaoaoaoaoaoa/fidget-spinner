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

fn fault_message(response: &Value) -> Option<&str> {
    response["result"]["structuredContent"]["message"].as_str()
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
            "tools/call:source.record".to_owned(),
        )],
    )?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(6, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let response = harness.call_tool(
        7,
        "source.record",
        json!({
            "title": "should not duplicate",
            "summary": "dedupe check",
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
fn successful_bind_clears_stale_fault_from_health() -> TestResult {
    let bad_root = temp_project_root("bind_fault_bad")?;
    must(
        fs::write(bad_root.join("README.txt").as_std_path(), "occupied"),
        "seed bad bind root",
    )?;
    let good_root = temp_project_root("bind_fault_good")?;
    init_project(&good_root)?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let failed_bind = harness.bind_project(301, &bad_root)?;
    assert_eq!(failed_bind["result"]["isError"].as_bool(), Some(true));

    let failed_health = harness.call_tool(302, "system.health", json!({ "detail": "full" }))?;
    assert_eq!(
        tool_content(&failed_health)["last_fault"]["operation"].as_str(),
        Some("tools/call:project.bind")
    );

    let good_bind = harness.bind_project(303, &good_root)?;
    assert_eq!(good_bind["result"]["isError"].as_bool(), Some(false));

    let recovered_health = harness.call_tool(304, "system.health", json!({}))?;
    assert_eq!(recovered_health["result"]["isError"].as_bool(), Some(false));
    assert!(tool_content(&recovered_health).get("last_fault").is_none());
    assert!(!must_some(tool_text(&recovered_health), "recovered health text")?.contains("fault:"));

    let recovered_health_full =
        harness.call_tool(306, "system.health", json!({ "detail": "full" }))?;
    assert_eq!(
        tool_content(&recovered_health_full)["last_fault"],
        Value::Null,
    );

    let recovered_telemetry = harness.call_tool(305, "system.telemetry", json!({}))?;
    assert_eq!(
        recovered_telemetry["result"]["isError"].as_bool(),
        Some(false)
    );
    assert_eq!(
        tool_content(&recovered_telemetry)["errors"].as_u64(),
        Some(1)
    );
    assert!(tool_content(&recovered_telemetry)["last_fault"].is_null());
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
            "summary": "rebind summary",
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
            "summary": "should fail without explicit tags",
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
            "summary": "tagged lookup summary",
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

#[test]
fn source_record_accepts_tags_and_filtering() -> TestResult {
    let project_root = temp_project_root("research_tags")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(451, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let tag = harness.call_tool(
        452,
        "tag.add",
        json!({
            "name": "campaign/libgrid",
            "description": "libgrid migration campaign",
        }),
    )?;
    assert_eq!(tag["result"]["isError"].as_bool(), Some(false));

    let research = harness.call_tool(
        453,
        "source.record",
        json!({
            "title": "ingest tranche",
            "summary": "Import the next libgrid tranche.",
            "body": "Full import notes live here.",
            "tags": ["campaign/libgrid"],
        }),
    )?;
    assert_eq!(research["result"]["isError"].as_bool(), Some(false));

    let filtered = harness.call_tool(454, "node.list", json!({"tags": ["campaign/libgrid"]}))?;
    let nodes = must_some(tool_content(&filtered).as_array(), "filtered source nodes")?;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["class"].as_str(), Some("source"));
    assert_eq!(nodes[0]["tags"][0].as_str(), Some("campaign/libgrid"));
    Ok(())
}

#[test]
fn prose_tools_reject_invalid_shapes_over_mcp() -> TestResult {
    let project_root = temp_project_root("prose_invalid")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(46, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let missing_note_summary = harness.call_tool(
        47,
        "note.quick",
        json!({
            "title": "untagged",
            "body": "body only",
            "tags": [],
        }),
    )?;
    assert_eq!(
        missing_note_summary["result"]["isError"].as_bool(),
        Some(true)
    );
    assert!(
        fault_message(&missing_note_summary)
            .is_some_and(|message| message.contains("summary") || message.contains("missing field"))
    );

    let missing_source_summary = harness.call_tool(
        48,
        "source.record",
        json!({
            "title": "source only",
            "body": "body only",
        }),
    )?;
    assert_eq!(
        missing_source_summary["result"]["isError"].as_bool(),
        Some(true)
    );
    assert!(
        fault_message(&missing_source_summary)
            .is_some_and(|message| message.contains("summary") || message.contains("missing field"))
    );

    let note_without_body = harness.call_tool(
        49,
        "node.create",
        json!({
            "class": "note",
            "title": "missing body",
            "summary": "triage layer",
            "tags": [],
            "payload": {},
        }),
    )?;
    assert_eq!(note_without_body["result"]["isError"].as_bool(), Some(true));
    assert!(
        fault_message(&note_without_body)
            .is_some_and(|message| message.contains("payload field `body`"))
    );

    let source_without_summary = harness.call_tool(
        50,
        "node.create",
        json!({
            "class": "source",
            "title": "missing summary",
            "payload": { "body": "full research body" },
        }),
    )?;
    assert_eq!(
        source_without_summary["result"]["isError"].as_bool(),
        Some(true)
    );
    assert!(
        fault_message(&source_without_summary)
            .is_some_and(|message| message.contains("non-empty summary"))
    );
    Ok(())
}

#[test]
fn concise_note_reads_do_not_leak_body_text() -> TestResult {
    let project_root = temp_project_root("concise_note_read")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(50, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let note = harness.call_tool(
        51,
        "note.quick",
        json!({
            "title": "tagged note",
            "summary": "triage layer",
            "body": "full note body should stay out of concise reads",
            "tags": [],
        }),
    )?;
    assert_eq!(note["result"]["isError"].as_bool(), Some(false));
    let node_id = must_some(tool_content(&note)["id"].as_str(), "created note id")?.to_owned();

    let concise = harness.call_tool(52, "node.read", json!({ "node_id": node_id }))?;
    let concise_structured = tool_content(&concise);
    assert_eq!(concise_structured["summary"].as_str(), Some("triage layer"));
    assert!(concise_structured["payload_preview"].get("body").is_none());
    assert!(
        !must_some(tool_text(&concise), "concise note.read text")?
            .contains("full note body should stay out of concise reads")
    );

    let full = harness.call_tool(
        53,
        "node.read",
        json!({ "node_id": node_id, "detail": "full" }),
    )?;
    assert_eq!(
        tool_content(&full)["payload"]["fields"]["body"].as_str(),
        Some("full note body should stay out of concise reads")
    );
    Ok(())
}

#[test]
fn concise_prose_reads_only_surface_payload_field_names() -> TestResult {
    let project_root = temp_project_root("concise_prose_field_names")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(531, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let research = harness.call_tool(
        532,
        "node.create",
        json!({
            "class": "source",
            "title": "rich import",
            "summary": "triage layer only",
            "payload": {
                "body": "Body stays out of concise output.",
                "source_excerpt": "This imported excerpt is intentionally long and should never reappear in concise node reads as a value preview.",
                "verbatim_snippet": "Another long snippet that belongs in full payload inspection only, not in triage surfaces."
            }
        }),
    )?;
    assert_eq!(research["result"]["isError"].as_bool(), Some(false));
    let node_id =
        must_some(tool_content(&research)["id"].as_str(), "created source id")?.to_owned();

    let concise = harness.call_tool(533, "node.read", json!({ "node_id": node_id }))?;
    let concise_structured = tool_content(&concise);
    assert_eq!(concise_structured["payload_field_count"].as_u64(), Some(2));
    let payload_fields = must_some(
        concise_structured["payload_fields"].as_array(),
        "concise prose payload fields",
    )?;
    assert!(
        payload_fields
            .iter()
            .any(|field| field.as_str() == Some("source_excerpt"))
    );
    assert!(concise_structured.get("payload_preview").is_none());
    let concise_text = must_some(tool_text(&concise), "concise prose read text")?;
    assert!(!concise_text.contains("This imported excerpt is intentionally long"));
    assert!(concise_text.contains("payload fields: source_excerpt, verbatim_snippet"));
    Ok(())
}

#[test]
fn node_list_does_not_enumerate_full_prose_bodies() -> TestResult {
    let project_root = temp_project_root("node_list_no_body_leak")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(54, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let note = harness.call_tool(
        55,
        "note.quick",
        json!({
            "title": "tagged note",
            "summary": "triage summary",
            "body": "full note body should never appear in list-like surfaces",
            "tags": [],
        }),
    )?;
    assert_eq!(note["result"]["isError"].as_bool(), Some(false));

    let listed = harness.call_tool(56, "node.list", json!({ "class": "note" }))?;
    let listed_rows = must_some(tool_content(&listed).as_array(), "listed note rows")?;
    assert_eq!(listed_rows.len(), 1);
    assert_eq!(listed_rows[0]["summary"].as_str(), Some("triage summary"));
    assert!(listed_rows[0].get("body").is_none());
    assert!(
        !must_some(tool_text(&listed), "node.list text")?
            .contains("full note body should never appear in list-like surfaces")
    );
    Ok(())
}

#[test]
fn metric_tools_are_listed_for_discovery() -> TestResult {
    let project_root = temp_project_root("metric_tool_list")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root), &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let tools = harness.tools_list()?;
    let names = must_some(tools["result"]["tools"].as_array(), "tool list")?
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect::<Vec<_>>();
    assert!(names.contains(&"metric.define"));
    assert!(names.contains(&"metric.keys"));
    assert!(names.contains(&"metric.best"));
    assert!(names.contains(&"metric.migrate"));
    assert!(names.contains(&"run.dimension.define"));
    assert!(names.contains(&"run.dimension.list"));
    assert!(names.contains(&"schema.field.upsert"));
    assert!(names.contains(&"schema.field.remove"));
    Ok(())
}

#[test]
fn schema_field_tools_mutate_project_schema() -> TestResult {
    let project_root = temp_project_root("schema_field_tools")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root), &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let upsert = harness.call_tool(
        861,
        "schema.field.upsert",
        json!({
            "name": "scenario",
            "node_classes": ["hypothesis", "analysis"],
            "presence": "recommended",
            "severity": "warning",
            "role": "projection_gate",
            "inference_policy": "manual_only",
            "value_type": "string"
        }),
    )?;
    assert_eq!(upsert["result"]["isError"].as_bool(), Some(false));
    assert_eq!(
        tool_content(&upsert)["field"]["name"].as_str(),
        Some("scenario")
    );
    assert_eq!(
        tool_content(&upsert)["field"]["node_classes"],
        json!(["hypothesis", "analysis"])
    );

    let schema = harness.call_tool(862, "project.schema", json!({ "detail": "full" }))?;
    assert_eq!(schema["result"]["isError"].as_bool(), Some(false));
    let fields = must_some(tool_content(&schema)["fields"].as_array(), "schema fields")?;
    assert!(fields.iter().any(|field| {
        field["name"].as_str() == Some("scenario") && field["value_type"].as_str() == Some("string")
    }));

    let remove = harness.call_tool(
        863,
        "schema.field.remove",
        json!({
            "name": "scenario",
            "node_classes": ["hypothesis", "analysis"]
        }),
    )?;
    assert_eq!(remove["result"]["isError"].as_bool(), Some(false));
    assert_eq!(tool_content(&remove)["removed_count"].as_u64(), Some(1));

    let schema_after = harness.call_tool(864, "project.schema", json!({ "detail": "full" }))?;
    let fields_after = must_some(
        tool_content(&schema_after)["fields"].as_array(),
        "schema fields after remove",
    )?;
    assert!(
        !fields_after
            .iter()
            .any(|field| field["name"].as_str() == Some("scenario"))
    );
    Ok(())
}

#[test]
fn bind_open_backfills_legacy_missing_summary() -> TestResult {
    let project_root = temp_project_root("bind_backfill")?;
    init_project(&project_root)?;

    let node_id = {
        let mut store = must(ProjectStore::open(&project_root), "open project store")?;
        let node = must(
            store.add_node(fidget_spinner_store_sqlite::CreateNodeRequest {
                class: fidget_spinner_core::NodeClass::Source,
                frontier_id: None,
                title: must(NonEmptyText::new("legacy source"), "legacy title")?,
                summary: Some(must(
                    NonEmptyText::new("temporary summary"),
                    "temporary summary",
                )?),
                tags: None,
                payload: fidget_spinner_core::NodePayload::with_schema(
                    store.schema().schema_ref(),
                    serde_json::from_value(json!({
                        "body": "Derived summary first paragraph.\n\nLonger body follows."
                    }))
                    .map_err(|error| io::Error::other(format!("payload object: {error}")))?,
                ),
                annotations: Vec::new(),
                attachments: Vec::new(),
            }),
            "create legacy source node",
        )?;
        node.id.to_string()
    };

    let database_path = project_root.join(".fidget_spinner").join("state.sqlite");
    let clear_output = must(
        Command::new("sqlite3")
            .current_dir(project_root.as_std_path())
            .arg(database_path.as_str())
            .arg(format!(
                "UPDATE nodes SET summary = NULL WHERE id = '{node_id}';"
            ))
            .output(),
        "spawn sqlite3 for direct summary clear",
    )?;
    if !clear_output.status.success() {
        return Err(io::Error::other(format!(
            "sqlite3 summary clear failed: {}",
            String::from_utf8_lossy(&clear_output.stderr)
        ))
        .into());
    }

    let mut harness = McpHarness::spawn(None, &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;
    let bind = harness.bind_project(60, &project_root)?;
    assert_eq!(bind["result"]["isError"].as_bool(), Some(false));

    let read = harness.call_tool(61, "node.read", json!({ "node_id": node_id }))?;
    assert_eq!(read["result"]["isError"].as_bool(), Some(false));
    assert_eq!(
        tool_content(&read)["summary"].as_str(),
        Some("Derived summary first paragraph.")
    );

    let listed = harness.call_tool(62, "node.list", json!({ "class": "source" }))?;
    let items = must_some(tool_content(&listed).as_array(), "source node list")?;
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0]["summary"].as_str(),
        Some("Derived summary first paragraph.")
    );
    Ok(())
}

#[test]
fn metric_tools_rank_closed_experiments_and_enforce_disambiguation() -> TestResult {
    let project_root = temp_project_root("metric_rank_e2e")?;
    init_project(&project_root)?;

    let mut harness = McpHarness::spawn(Some(&project_root), &[])?;
    let _ = harness.initialize()?;
    harness.notify_initialized()?;

    let frontier = harness.call_tool(
        70,
        "frontier.init",
        json!({
            "label": "metric frontier",
            "objective": "exercise metric ranking",
            "contract_title": "metric contract",
            "benchmark_suites": ["smoke"],
            "promotion_criteria": ["rank by one key"],
            "primary_metric": {
                "key": "wall_clock_s",
                "unit": "seconds",
                "objective": "minimize"
            }
        }),
    )?;
    assert_eq!(frontier["result"]["isError"].as_bool(), Some(false));
    let frontier_id = must_some(
        tool_content(&frontier)["frontier_id"].as_str(),
        "frontier id",
    )?
    .to_owned();
    let metric_define = harness.call_tool(
        701,
        "metric.define",
        json!({
            "key": "wall_clock_s",
            "unit": "seconds",
            "objective": "minimize",
            "description": "elapsed wall time"
        }),
    )?;
    assert_eq!(metric_define["result"]["isError"].as_bool(), Some(false));

    let scenario_dimension = harness.call_tool(
        702,
        "run.dimension.define",
        json!({
            "key": "scenario",
            "value_type": "string",
            "description": "workload family"
        }),
    )?;
    assert_eq!(
        scenario_dimension["result"]["isError"].as_bool(),
        Some(false)
    );

    let duration_dimension = harness.call_tool(
        703,
        "run.dimension.define",
        json!({
            "key": "duration_s",
            "value_type": "numeric",
            "description": "time budget in seconds"
        }),
    )?;
    assert_eq!(
        duration_dimension["result"]["isError"].as_bool(),
        Some(false)
    );

    let dimensions = harness.call_tool(704, "run.dimension.list", json!({}))?;
    assert_eq!(dimensions["result"]["isError"].as_bool(), Some(false));
    let dimension_rows = must_some(tool_content(&dimensions).as_array(), "run dimension rows")?;
    assert!(dimension_rows.iter().any(|row| {
        row["key"].as_str() == Some("benchmark_suite")
            && row["value_type"].as_str() == Some("string")
    }));
    assert!(dimension_rows.iter().any(|row| {
        row["key"].as_str() == Some("scenario")
            && row["description"].as_str() == Some("workload family")
    }));
    assert!(dimension_rows.iter().any(|row| {
        row["key"].as_str() == Some("duration_s") && row["value_type"].as_str() == Some("numeric")
    }));

    let first_change = harness.call_tool(
        71,
        "node.create",
        json!({
            "class": "hypothesis",
            "frontier_id": frontier_id,
            "title": "first change",
            "summary": "first change summary",
            "payload": {
                "body": "first change body",
                "wall_clock_s": 14.0
            }
        }),
    )?;
    assert_eq!(first_change["result"]["isError"].as_bool(), Some(false));
    let first_change_id = must_some(
        tool_content(&first_change)["id"].as_str(),
        "first change id",
    )?;
    let first_experiment = harness.call_tool(
        711,
        "experiment.open",
        json!({
            "frontier_id": frontier_id,
            "hypothesis_node_id": first_change_id,
            "title": "first experiment",
            "summary": "first experiment summary"
        }),
    )?;
    assert_eq!(first_experiment["result"]["isError"].as_bool(), Some(false));
    let first_experiment_id = must_some(
        tool_content(&first_experiment)["experiment_id"].as_str(),
        "first experiment id",
    )?;

    let first_close = harness.call_tool(
        72,
        "experiment.close",
        json!({
            "experiment_id": first_experiment_id,
            "run": {
                "title": "first run",
                "summary": "first run summary",
                "backend": "worktree_process",
                "dimensions": {
                    "benchmark_suite": "smoke",
                    "scenario": "belt_4x5",
                    "duration_s": 20.0
                },
                "command": {
                    "working_directory": project_root.as_str(),
                    "argv": ["true"]
                }
            },
            "primary_metric": {
                "key": "wall_clock_s",
                "value": 10.0
            },
            "note": {
                "summary": "first run note"
            },
            "verdict": "kept",
            "decision_title": "first decision",
            "decision_rationale": "keep first candidate around"
        }),
    )?;
    assert_eq!(first_close["result"]["isError"].as_bool(), Some(false));

    let second_change = harness.call_tool(
        73,
        "node.create",
        json!({
            "class": "hypothesis",
            "frontier_id": frontier_id,
            "title": "second change",
            "summary": "second change summary",
            "payload": {
                "body": "second change body",
                "wall_clock_s": 7.0
            }
        }),
    )?;
    assert_eq!(second_change["result"]["isError"].as_bool(), Some(false));
    let second_change_id = must_some(
        tool_content(&second_change)["id"].as_str(),
        "second change id",
    )?;
    let second_experiment = harness.call_tool(
        712,
        "experiment.open",
        json!({
            "frontier_id": frontier_id,
            "hypothesis_node_id": second_change_id,
            "title": "second experiment",
            "summary": "second experiment summary"
        }),
    )?;
    assert_eq!(
        second_experiment["result"]["isError"].as_bool(),
        Some(false)
    );
    let second_experiment_id = must_some(
        tool_content(&second_experiment)["experiment_id"].as_str(),
        "second experiment id",
    )?;

    let second_close = harness.call_tool(
        74,
        "experiment.close",
        json!({
            "experiment_id": second_experiment_id,
            "run": {
                "title": "second run",
                "summary": "second run summary",
                "backend": "worktree_process",
                "dimensions": {
                    "benchmark_suite": "smoke",
                    "scenario": "belt_4x5",
                    "duration_s": 60.0
                },
                "command": {
                    "working_directory": project_root.as_str(),
                    "argv": ["true"]
                }
            },
            "primary_metric": {
                "key": "wall_clock_s",
                "value": 5.0
            },
            "note": {
                "summary": "second run note"
            },
            "verdict": "kept",
            "decision_title": "second decision",
            "decision_rationale": "second candidate looks stronger"
        }),
    )?;
    assert_eq!(second_close["result"]["isError"].as_bool(), Some(false));

    let second_frontier = harness.call_tool(
        80,
        "frontier.init",
        json!({
            "label": "metric frontier two",
            "objective": "exercise frontier filtering",
            "contract_title": "metric contract two",
            "benchmark_suites": ["smoke"],
            "promotion_criteria": ["frontier filters should isolate rankings"],
            "primary_metric": {
                "key": "wall_clock_s",
                "unit": "seconds",
                "objective": "minimize"
            }
        }),
    )?;
    assert_eq!(second_frontier["result"]["isError"].as_bool(), Some(false));
    let second_frontier_id = must_some(
        tool_content(&second_frontier)["frontier_id"].as_str(),
        "second frontier id",
    )?
    .to_owned();

    let third_change = harness.call_tool(
        81,
        "node.create",
        json!({
            "class": "hypothesis",
            "frontier_id": second_frontier_id,
            "title": "third change",
            "summary": "third change summary",
            "payload": {
                "body": "third change body",
                "wall_clock_s": 3.0
            }
        }),
    )?;
    assert_eq!(third_change["result"]["isError"].as_bool(), Some(false));
    let third_change_id = must_some(
        tool_content(&third_change)["id"].as_str(),
        "third change id",
    )?;
    let third_experiment = harness.call_tool(
        811,
        "experiment.open",
        json!({
            "frontier_id": second_frontier_id,
            "hypothesis_node_id": third_change_id,
            "title": "third experiment",
            "summary": "third experiment summary"
        }),
    )?;
    assert_eq!(third_experiment["result"]["isError"].as_bool(), Some(false));
    let third_experiment_id = must_some(
        tool_content(&third_experiment)["experiment_id"].as_str(),
        "third experiment id",
    )?;

    let third_close = harness.call_tool(
        82,
        "experiment.close",
        json!({
            "experiment_id": third_experiment_id,
            "run": {
                "title": "third run",
                "summary": "third run summary",
                "backend": "worktree_process",
                "dimensions": {
                    "benchmark_suite": "smoke",
                    "scenario": "belt_4x5_alt",
                    "duration_s": 60.0
                },
                "command": {
                    "working_directory": project_root.as_str(),
                    "argv": ["true"]
                }
            },
            "primary_metric": {
                "key": "wall_clock_s",
                "value": 3.0
            },
            "note": {
                "summary": "third run note"
            },
            "verdict": "kept",
            "decision_title": "third decision",
            "decision_rationale": "third candidate is best overall but not in the first frontier"
        }),
    )?;
    assert_eq!(third_close["result"]["isError"].as_bool(), Some(false));

    let keys = harness.call_tool(75, "metric.keys", json!({}))?;
    assert_eq!(keys["result"]["isError"].as_bool(), Some(false));
    let key_rows = must_some(tool_content(&keys).as_array(), "metric keys array")?;
    assert!(key_rows.iter().any(|row| {
        row["key"].as_str() == Some("wall_clock_s") && row["source"].as_str() == Some("run_metric")
    }));
    assert!(key_rows.iter().any(|row| {
        row["key"].as_str() == Some("wall_clock_s")
            && row["source"].as_str() == Some("run_metric")
            && row["description"].as_str() == Some("elapsed wall time")
            && row["requires_order"].as_bool() == Some(false)
    }));
    assert!(key_rows.iter().any(|row| {
        row["key"].as_str() == Some("wall_clock_s")
            && row["source"].as_str() == Some("hypothesis_payload")
    }));

    let filtered_keys = harness.call_tool(
        750,
        "metric.keys",
        json!({
            "source": "run_metric",
            "dimensions": {
                "scenario": "belt_4x5",
                "duration_s": 60.0
            }
        }),
    )?;
    assert_eq!(filtered_keys["result"]["isError"].as_bool(), Some(false));
    let filtered_key_rows = must_some(
        tool_content(&filtered_keys).as_array(),
        "filtered metric keys array",
    )?;
    assert_eq!(filtered_key_rows.len(), 1);
    assert_eq!(filtered_key_rows[0]["key"].as_str(), Some("wall_clock_s"));
    assert_eq!(filtered_key_rows[0]["experiment_count"].as_u64(), Some(1));

    let ambiguous = harness.call_tool(76, "metric.best", json!({ "key": "wall_clock_s" }))?;
    assert_eq!(ambiguous["result"]["isError"].as_bool(), Some(true));
    assert!(
        fault_message(&ambiguous)
            .is_some_and(|message| message.contains("ambiguous across sources"))
    );

    let run_metric_best = harness.call_tool(
        77,
        "metric.best",
        json!({
            "key": "wall_clock_s",
            "source": "run_metric",
            "dimensions": {
                "scenario": "belt_4x5",
                "duration_s": 60.0
            },
            "limit": 5
        }),
    )?;
    assert_eq!(run_metric_best["result"]["isError"].as_bool(), Some(false));
    let run_best_rows = must_some(
        tool_content(&run_metric_best).as_array(),
        "run metric best array",
    )?;
    assert_eq!(run_best_rows[0]["value"].as_f64(), Some(5.0));
    assert_eq!(run_best_rows.len(), 1);
    assert_eq!(
        run_best_rows[0]["experiment_title"].as_str(),
        Some("second experiment")
    );
    assert_eq!(run_best_rows[0]["verdict"].as_str(), Some("kept"));
    assert_eq!(
        run_best_rows[0]["dimensions"]["scenario"].as_str(),
        Some("belt_4x5")
    );
    assert_eq!(
        run_best_rows[0]["dimensions"]["duration_s"].as_f64(),
        Some(60.0)
    );
    assert!(
        must_some(tool_text(&run_metric_best), "run metric best text")?.contains("hypothesis=")
    );
    assert!(must_some(tool_text(&run_metric_best), "run metric best text")?.contains("dims:"));

    let payload_requires_order = harness.call_tool(
        78,
        "metric.best",
        json!({
            "key": "wall_clock_s",
            "source": "hypothesis_payload"
        }),
    )?;
    assert_eq!(
        payload_requires_order["result"]["isError"].as_bool(),
        Some(true)
    );
    assert!(
        fault_message(&payload_requires_order)
            .is_some_and(|message| message.contains("explicit order"))
    );

    let payload_best = harness.call_tool(
        79,
        "metric.best",
        json!({
            "key": "wall_clock_s",
            "source": "hypothesis_payload",
            "dimensions": {
                "scenario": "belt_4x5",
                "duration_s": 60.0
            },
            "order": "asc"
        }),
    )?;
    assert_eq!(payload_best["result"]["isError"].as_bool(), Some(false));
    let payload_best_rows = must_some(
        tool_content(&payload_best).as_array(),
        "payload metric best array",
    )?;
    assert_eq!(payload_best_rows[0]["value"].as_f64(), Some(7.0));
    assert_eq!(payload_best_rows.len(), 1);
    assert_eq!(
        payload_best_rows[0]["experiment_title"].as_str(),
        Some("second experiment")
    );

    let filtered_best = harness.call_tool(
        83,
        "metric.best",
        json!({
            "key": "wall_clock_s",
            "source": "run_metric",
            "frontier_id": frontier_id,
            "dimensions": {
                "scenario": "belt_4x5"
            },
            "limit": 5
        }),
    )?;
    assert_eq!(filtered_best["result"]["isError"].as_bool(), Some(false));
    let filtered_rows = must_some(
        tool_content(&filtered_best).as_array(),
        "filtered metric best array",
    )?;
    assert_eq!(filtered_rows.len(), 2);
    assert_eq!(
        filtered_rows[0]["experiment_title"].as_str(),
        Some("second experiment")
    );
    assert!(
        filtered_rows
            .iter()
            .all(|row| row["frontier_id"].as_str() == Some(frontier_id.as_str()))
    );

    let global_best = harness.call_tool(
        84,
        "metric.best",
        json!({
            "key": "wall_clock_s",
            "source": "run_metric",
            "limit": 5
        }),
    )?;
    assert_eq!(global_best["result"]["isError"].as_bool(), Some(false));
    let global_rows = must_some(
        tool_content(&global_best).as_array(),
        "global metric best array",
    )?;
    assert_eq!(
        global_rows[0]["experiment_title"].as_str(),
        Some("third experiment")
    );
    assert_eq!(
        global_rows[0]["frontier_id"].as_str(),
        Some(second_frontier_id.as_str())
    );

    let migrate = harness.call_tool(85, "metric.migrate", json!({}))?;
    assert_eq!(migrate["result"]["isError"].as_bool(), Some(false));
    assert_eq!(
        tool_content(&migrate)["inserted_metric_definitions"].as_u64(),
        Some(0)
    );
    assert_eq!(
        tool_content(&migrate)["inserted_dimension_definitions"].as_u64(),
        Some(0)
    );
    assert_eq!(
        tool_content(&migrate)["inserted_dimension_values"].as_u64(),
        Some(0)
    );
    Ok(())
}
