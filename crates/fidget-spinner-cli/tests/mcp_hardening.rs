use axum as _;
use clap as _;
use dirs as _;
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::OnceLock;

use camino::Utf8PathBuf;
use fidget_spinner_core::{
    FrontierStatus, MetricUnit, MetricVisibility, NonEmptyText, OptimizationObjective,
    RegistryLockMode, RegistryName, Slug, TagFamilyName, TagName,
};
use fidget_spinner_store_sqlite::{
    AssignTagFamilyRequest, CreateFrontierRequest, CreateHypothesisRequest, CreateKpiRequest,
    CreateTagFamilyRequest, DefineMetricRequest, DeleteTagRequest, ListExperimentsQuery,
    ListFrontiersQuery, MergeTagRequest, OpenExperimentRequest, ProjectStore, RenameTagRequest,
    SetRegistryLockRequest, UpdateFrontierRequest,
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
            "name": "node throughput",
            "objective": "maximize",
            "metric_keys": ["nodes_solved"],
        }),
    )?);
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
    assert!(tool_names.contains(&"hypothesis.record"));
    assert!(tool_names.contains(&"experiment.close"));
    assert!(tool_names.contains(&"experiment.nearest"));
    assert!(tool_names.contains(&"artifact.record"));
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
            objective: None,
            status: Some(FrontierStatus::Archived),
            situation: None,
            roadmap: None,
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
        71,
        "frontier.create",
        json!({
            "label": "Governed Frontier",
            "objective": "Test mandatory family",
            "slug": "governed",
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
            "tags": ["baseline"],
        }),
    )?;
    assert_tool_ok(&accepted);
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
        170,
        "frontier.create",
        json!({
            "label": "Assignment Lock Frontier",
            "objective": "Assignment lock should be inert",
            "slug": "assignment-lock",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        171,
        "hypothesis.record",
        json!({
            "frontier": "assignment-lock",
            "title": "Tagged despite assignment lock",
            "summary": "The retired assignment lock does not block tag sets.",
            "body": "One paragraph body.",
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
    let active_hypotheses = must_some(
        content["active_hypotheses"].as_array(),
        "active hypotheses array",
    )?;
    assert_eq!(active_hypotheses.len(), 1);
    assert_eq!(
        active_hypotheses[0]["hypothesis"]["slug"].as_str(),
        Some("node-local-loop")
    );
    assert!(active_hypotheses[0]["hypothesis"].get("id").is_none());
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
    assert!(
        must_some(
            content["open_experiments"].as_array(),
            "open experiments array",
        )?[0]
            .get("hypothesis_id")
            .is_none()
    );
    assert!(content.get("artifacts").is_none());
    assert!(active_hypotheses[0]["hypothesis"].get("body").is_none());
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
            "unit": "count",
            "objective": "maximize",
            "visibility": "canonical",
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
            "name": "node throughput",
            "objective": "maximize",
            "metric_keys": ["nodes_solved"],
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
        )?["metrics"][0]["key"]
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
            "unit": "count",
            "objective": "maximize",
            "visibility": "canonical",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        81,
        "run.dimension.define",
        json!({"key": "instance", "value_type": "string"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        82,
        "run.dimension.define",
        json!({"key": "profile", "value_type": "string"}),
    )?);
    assert_tool_ok(&harness.call_tool(
        83,
        "run.dimension.define",
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
            "name": "node throughput",
            "objective": "maximize",
            "metric_keys": ["nodes_solved"],
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
                "backend": "manual",
                "command": {"argv": [slug]},
                "dimensions": {
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
            "dimensions": {
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
        "run.dimension.define",
        json!({
            "key": "duration_s",
            "value_type": "numeric",
            "description": "Wallclock timeout in seconds.",
        }),
    )?;
    assert_tool_ok(&dimension);
    assert!(tool_content(&dimension)["record"]["created_at"].is_string());
    assert!(tool_content(&dimension)["record"]["updated_at"].is_string());

    let dimensions = harness.call_tool_full(20, "run.dimension.list", json!({}))?;
    assert_tool_ok(&dimensions);
    let listed = must_some(
        tool_content(&dimensions)["dimensions"]
            .as_array()
            .and_then(|items| items.first()),
        "defined run dimension in list",
    )?;
    assert!(listed["created_at"].is_string());
    assert!(listed["updated_at"].is_string());

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
            "unit": "micros",
            "objective": "minimize",
            "visibility": "canonical",
        }),
    )?;
    assert_tool_ok(&microseconds);
    assert_eq!(
        tool_content(&microseconds)["record"]["unit"].as_str(),
        Some("microseconds")
    );

    let custom = harness.call_tool_full(
        24,
        "metric.define",
        json!({
            "key": "root_lp_objective_last",
            "unit": "objective",
            "objective": "minimize",
            "visibility": "canonical",
        }),
    )?;
    assert_tool_ok(&custom);
    assert_eq!(
        tool_content(&custom)["record"]["unit"].as_str(),
        Some("objective")
    );

    let placeholder = harness.call_tool(
        25,
        "metric.define",
        json!({
            "key": "bad_custom_placeholder",
            "unit": "custom",
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
    assert_no_opaque_ids(content).map_err(|error| io::Error::other(error.to_string()))?;
    assert_eq!(
        content["record"]["locator"].as_str(),
        Some("/tmp/lp-review.md")
    );
    assert_eq!(content["record"]["slug"].as_str(), Some("lp-review-doc"));
    assert!(content["record"].get("body").is_none());
    assert_eq!(
        must_some(content["attachments"].as_array(), "artifact attachments")?[0]["kind"].as_str(),
        Some("hypothesis")
    );
    assert_eq!(
        must_some(content["attachments"].as_array(), "artifact attachments")?[0]["slug"].as_str(),
        Some("sourced-hypothesis")
    );
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
            "unit": "count",
            "objective": "maximize",
            "visibility": "canonical",
        }),
    )?);
    assert_tool_ok(&harness.call_tool(
        51,
        "run.dimension.define",
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
            "backend": "manual",
            "command": {"argv": ["dirty-run"]},
            "dimensions": {"instance": "4x5-braid"},
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
            "unit": "count",
            "objective": "maximize",
            "visibility": "canonical",
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
            unit: must(MetricUnit::new("count"), "metric unit")?,
            aggregation: fidget_spinner_core::MetricAggregation::Point,
            objective: OptimizationObjective::Maximize,
            visibility: MetricVisibility::Canonical,
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
            name: must(NonEmptyText::new("node throughput"), "kpi name")?,
            objective: OptimizationObjective::Maximize,
            description: None,
            metric_keys: vec![must(NonEmptyText::new("nodes_solved"), "kpi metric")?],
        }),
        "create beta KPI",
    )?;

    let beta_list = harness.call_tool_full(63, "frontier.list", json!({}))?;
    assert_tool_ok(&beta_list);
    assert_eq!(frontier_slugs(&beta_list), vec!["beta"]);
    Ok(())
}
