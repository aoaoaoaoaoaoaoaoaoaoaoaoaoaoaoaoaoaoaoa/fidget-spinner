mod bundled_skill;
mod mcp;
mod ui;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Parser, Subcommand, ValueEnum};
use fidget_spinner_core::{
    AnnotationVisibility, CodeSnapshotRef, CommandRecipe, ExecutionBackend, FrontierContract,
    FrontierNote, FrontierVerdict, GitCommitHash, MetricObservation, MetricSpec, MetricUnit,
    NodeAnnotation, NodeClass, NodePayload, NonEmptyText, OptimizationObjective, TagName,
};
use fidget_spinner_store_sqlite::{
    CloseExperimentRequest, CreateFrontierRequest, CreateNodeRequest, EdgeAttachment,
    EdgeAttachmentDirection, ListNodesQuery, ProjectStore, StoreError,
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use uuid::Uuid;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Fidget Spinner CLI, MCP server, and local navigator"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize a project-local `.fidget_spinner/` store.
    Init(InitArgs),
    /// Read the local project payload schema.
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    /// Create and inspect frontiers.
    Frontier {
        #[command(subcommand)]
        command: FrontierCommand,
    },
    /// Create, inspect, and mutate DAG nodes.
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    /// Record terse off-path notes.
    Note(NoteCommand),
    /// Manage the repo-local tag registry.
    Tag {
        #[command(subcommand)]
        command: TagCommand,
    },
    /// Record off-path research and enabling work.
    Research(ResearchCommand),
    /// Close a core-path experiment atomically.
    Experiment {
        #[command(subcommand)]
        command: ExperimentCommand,
    },
    /// Serve the hardened stdio MCP endpoint.
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },
    /// Serve the minimal local web navigator.
    Ui {
        #[command(subcommand)]
        command: UiCommand,
    },
    /// Inspect or install bundled Codex skills.
    Skill {
        #[command(subcommand)]
        command: SkillCommand,
    },
}

#[derive(Args)]
struct InitArgs {
    /// Project root to initialize.
    #[arg(long, default_value = ".")]
    project: PathBuf,
    /// Human-facing project name. Defaults to the directory name.
    #[arg(long)]
    name: Option<String>,
    /// Payload schema namespace written into `.fidget_spinner/schema.json`.
    #[arg(long, default_value = "local.project")]
    namespace: String,
}

#[derive(Subcommand)]
enum SchemaCommand {
    /// Show the current project schema as JSON.
    Show(ProjectArg),
}

#[derive(Subcommand)]
enum FrontierCommand {
    /// Create a frontier and root contract node.
    Init(FrontierInitArgs),
    /// Show one frontier projection or list frontiers when omitted.
    Status(FrontierStatusArgs),
}

#[derive(Args)]
struct FrontierInitArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    label: String,
    #[arg(long)]
    objective: String,
    #[arg(long, default_value = "frontier contract")]
    contract_title: String,
    #[arg(long)]
    contract_summary: Option<String>,
    #[arg(long = "benchmark-suite")]
    benchmark_suites: Vec<String>,
    #[arg(long = "promotion-criterion")]
    promotion_criteria: Vec<String>,
    #[arg(long = "primary-metric-key")]
    primary_metric_key: String,
    #[arg(long = "primary-metric-unit", value_enum)]
    primary_metric_unit: CliMetricUnit,
    #[arg(long = "primary-metric-objective", value_enum)]
    primary_metric_objective: CliOptimizationObjective,
    #[arg(long = "seed-summary", default_value = "initial champion checkpoint")]
    seed_summary: String,
}

#[derive(Args)]
struct FrontierStatusArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: Option<String>,
}

#[derive(Subcommand)]
enum NodeCommand {
    /// Create a generic DAG node.
    Add(NodeAddArgs),
    /// List recent nodes.
    List(NodeListArgs),
    /// Show one node in full.
    Show(NodeShowArgs),
    /// Attach an annotation to a node.
    Annotate(NodeAnnotateArgs),
    /// Archive a node without deleting it.
    Archive(NodeArchiveArgs),
}

#[derive(Args)]
struct NodeAddArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long, value_enum)]
    class: CliNodeClass,
    #[arg(long)]
    frontier: Option<String>,
    #[arg(long)]
    title: String,
    #[arg(long)]
    summary: Option<String>,
    #[arg(long = "payload-json")]
    payload_json: Option<String>,
    #[arg(long = "payload-file")]
    payload_file: Option<PathBuf>,
    #[command(flatten)]
    tag_selection: ExplicitTagSelectionArgs,
    #[arg(long = "field")]
    fields: Vec<String>,
    #[arg(long = "annotation")]
    annotations: Vec<String>,
    #[arg(long = "parent")]
    parents: Vec<String>,
}

#[derive(Args)]
struct NodeListArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: Option<String>,
    #[arg(long, value_enum)]
    class: Option<CliNodeClass>,
    #[arg(long = "tag")]
    tags: Vec<String>,
    #[arg(long)]
    include_archived: bool,
    #[arg(long, default_value_t = 20)]
    limit: u32,
}

#[derive(Args, Default)]
struct ExplicitTagSelectionArgs {
    #[arg(long = "tag")]
    tags: Vec<String>,
    #[arg(long, conflicts_with = "tags")]
    no_tags: bool,
}

#[derive(Args)]
struct NodeShowArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    node: String,
}

#[derive(Args)]
struct NodeAnnotateArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    node: String,
    #[arg(long)]
    body: String,
    #[arg(long)]
    label: Option<String>,
    #[arg(long)]
    visible: bool,
}

#[derive(Args)]
struct NodeArchiveArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    node: String,
}

#[derive(Args)]
struct NoteCommand {
    #[command(subcommand)]
    command: NoteSubcommand,
}

#[derive(Subcommand)]
enum NoteSubcommand {
    /// Record a quick off-path note.
    Quick(QuickNoteArgs),
}

#[derive(Subcommand)]
enum TagCommand {
    /// Register a new repo-local tag.
    Add(TagAddArgs),
    /// List registered repo-local tags.
    List(ProjectArg),
}

#[derive(Args)]
struct ResearchCommand {
    #[command(subcommand)]
    command: ResearchSubcommand,
}

#[derive(Subcommand)]
enum ResearchSubcommand {
    /// Record off-path research or enabling work.
    Add(QuickResearchArgs),
}

#[derive(Args)]
struct QuickNoteArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: Option<String>,
    #[arg(long)]
    title: String,
    #[arg(long)]
    body: String,
    #[command(flatten)]
    tag_selection: ExplicitTagSelectionArgs,
    #[arg(long = "parent")]
    parents: Vec<String>,
}

#[derive(Args)]
struct TagAddArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    name: String,
    #[arg(long)]
    description: String,
}

#[derive(Args)]
struct QuickResearchArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: Option<String>,
    #[arg(long)]
    title: String,
    #[arg(long)]
    body: String,
    #[arg(long)]
    summary: Option<String>,
    #[arg(long = "parent")]
    parents: Vec<String>,
}

#[derive(Subcommand)]
enum ExperimentCommand {
    /// Close a core-path experiment with checkpoint, run, note, and verdict.
    Close(ExperimentCloseArgs),
}

#[derive(Subcommand)]
enum McpCommand {
    /// Serve the public stdio MCP host. If `--project` is omitted, the host starts unbound.
    Serve(McpServeArgs),
    #[command(hide = true)]
    Worker(McpWorkerArgs),
}

#[derive(Subcommand)]
enum UiCommand {
    /// Serve the local read-only navigator.
    Serve(UiServeArgs),
}

#[derive(Args)]
struct ExperimentCloseArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: String,
    #[arg(long = "base-checkpoint")]
    base_checkpoint: String,
    #[arg(long = "change-node")]
    change_node: String,
    #[arg(long = "candidate-summary")]
    candidate_summary: String,
    #[arg(long = "run-title")]
    run_title: String,
    #[arg(long = "run-summary")]
    run_summary: Option<String>,
    #[arg(long = "benchmark-suite")]
    benchmark_suite: String,
    #[arg(long = "backend", value_enum, default_value_t = CliExecutionBackend::Worktree)]
    backend: CliExecutionBackend,
    #[arg(long = "cwd")]
    working_directory: Option<PathBuf>,
    #[arg(long = "argv")]
    argv: Vec<String>,
    #[arg(long = "env")]
    env: Vec<String>,
    #[arg(long = "primary-metric-key")]
    primary_metric_key: String,
    #[arg(long = "primary-metric-unit", value_enum)]
    primary_metric_unit: CliMetricUnit,
    #[arg(long = "primary-metric-objective", value_enum)]
    primary_metric_objective: CliOptimizationObjective,
    #[arg(long = "primary-metric-value")]
    primary_metric_value: f64,
    #[arg(long = "metric")]
    metrics: Vec<String>,
    #[arg(long)]
    note: String,
    #[arg(long = "next-hypothesis")]
    next_hypotheses: Vec<String>,
    #[arg(long = "verdict", value_enum)]
    verdict: CliFrontierVerdict,
    #[arg(long = "decision-title")]
    decision_title: String,
    #[arg(long = "decision-rationale")]
    decision_rationale: String,
}

#[derive(Subcommand)]
enum SkillCommand {
    /// List bundled skills.
    List,
    /// Install bundled skills into a Codex skill directory.
    Install(SkillInstallArgs),
    /// Print one bundled skill body.
    Show(SkillShowArgs),
}

#[derive(Args)]
struct SkillInstallArgs {
    /// Bundled skill name. Defaults to all bundled skills.
    #[arg(long)]
    name: Option<String>,
    /// Destination root. Defaults to `~/.codex/skills`.
    #[arg(long)]
    destination: Option<PathBuf>,
}

#[derive(Args)]
struct SkillShowArgs {
    /// Bundled skill name. Defaults to `fidget-spinner`.
    #[arg(long)]
    name: Option<String>,
}

#[derive(Args)]
struct ProjectArg {
    /// Project root or any nested path inside a project containing `.fidget_spinner/`.
    #[arg(long, default_value = ".")]
    project: PathBuf,
}

#[derive(Args)]
struct McpServeArgs {
    /// Optional initial project binding. When omitted, the MCP starts unbound.
    #[arg(long)]
    project: Option<PathBuf>,
}

#[derive(Args)]
struct McpWorkerArgs {
    #[arg(long)]
    project: PathBuf,
}

#[derive(Args)]
struct UiServeArgs {
    #[command(flatten)]
    project: ProjectArg,
    /// Bind address for the local navigator.
    #[arg(long, default_value = "127.0.0.1:8913")]
    bind: SocketAddr,
    /// Maximum rows rendered in list views.
    #[arg(long, default_value_t = 200)]
    limit: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliNodeClass {
    Contract,
    Change,
    Run,
    Analysis,
    Decision,
    Research,
    Enabling,
    Note,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliMetricUnit {
    Seconds,
    Bytes,
    Count,
    Ratio,
    Custom,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliOptimizationObjective {
    Minimize,
    Maximize,
    Target,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliExecutionBackend {
    Local,
    Worktree,
    Ssh,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliFrontierVerdict {
    PromoteToChampion,
    KeepOnFrontier,
    RevertToChampion,
    ArchiveDeadEnd,
    NeedsMoreEvidence,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), StoreError> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init(args) => run_init(args),
        Command::Schema { command } => match command {
            SchemaCommand::Show(project) => {
                let store = open_store(&project.project)?;
                print_json(store.schema())
            }
        },
        Command::Frontier { command } => match command {
            FrontierCommand::Init(args) => run_frontier_init(args),
            FrontierCommand::Status(args) => run_frontier_status(args),
        },
        Command::Node { command } => match command {
            NodeCommand::Add(args) => run_node_add(args),
            NodeCommand::List(args) => run_node_list(args),
            NodeCommand::Show(args) => run_node_show(args),
            NodeCommand::Annotate(args) => run_node_annotate(args),
            NodeCommand::Archive(args) => run_node_archive(args),
        },
        Command::Note(command) => match command.command {
            NoteSubcommand::Quick(args) => run_quick_note(args),
        },
        Command::Tag { command } => match command {
            TagCommand::Add(args) => run_tag_add(args),
            TagCommand::List(project) => run_tag_list(project),
        },
        Command::Research(command) => match command.command {
            ResearchSubcommand::Add(args) => run_quick_research(args),
        },
        Command::Experiment { command } => match command {
            ExperimentCommand::Close(args) => run_experiment_close(args),
        },
        Command::Mcp { command } => match command {
            McpCommand::Serve(args) => mcp::serve(args.project),
            McpCommand::Worker(args) => mcp::serve_worker(args.project),
        },
        Command::Ui { command } => match command {
            UiCommand::Serve(args) => run_ui_serve(args),
        },
        Command::Skill { command } => match command {
            SkillCommand::List => print_json(&bundled_skill::bundled_skill_summaries()),
            SkillCommand::Install(args) => run_skill_install(args),
            SkillCommand::Show(args) => {
                println!("{}", resolve_bundled_skill(args.name.as_deref())?.body);
                Ok(())
            }
        },
    }
}

fn run_init(args: InitArgs) -> Result<(), StoreError> {
    let project_root = utf8_path(args.project);
    let display_name = args
        .name
        .map(NonEmptyText::new)
        .transpose()?
        .unwrap_or(default_display_name_for_root(&project_root)?);
    let namespace = NonEmptyText::new(args.namespace)?;
    let store = ProjectStore::init(&project_root, display_name, namespace)?;
    println!("initialized {}", store.state_root());
    println!("project: {}", store.config().display_name);
    println!("schema: {}", store.state_root().join("schema.json"));
    maybe_print_gitignore_hint(&project_root)?;
    Ok(())
}

fn run_frontier_init(args: FrontierInitArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let initial_checkpoint =
        store.auto_capture_checkpoint(NonEmptyText::new(args.seed_summary)?)?;
    let projection = store.create_frontier(CreateFrontierRequest {
        label: NonEmptyText::new(args.label)?,
        contract_title: NonEmptyText::new(args.contract_title)?,
        contract_summary: args.contract_summary.map(NonEmptyText::new).transpose()?,
        contract: FrontierContract {
            objective: NonEmptyText::new(args.objective)?,
            evaluation: fidget_spinner_core::EvaluationProtocol {
                benchmark_suites: to_text_set(args.benchmark_suites)?,
                primary_metric: MetricSpec {
                    metric_key: NonEmptyText::new(args.primary_metric_key)?,
                    unit: args.primary_metric_unit.into(),
                    objective: args.primary_metric_objective.into(),
                },
                supporting_metrics: BTreeSet::new(),
            },
            promotion_criteria: to_text_vec(args.promotion_criteria)?,
        },
        initial_checkpoint,
    })?;
    print_json(&projection)
}

fn run_frontier_status(args: FrontierStatusArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    if let Some(frontier) = args.frontier {
        let projection = store.frontier_projection(parse_frontier_id(&frontier)?)?;
        return print_json(&projection);
    }
    let frontiers = store.list_frontiers()?;
    if frontiers.len() == 1 {
        return print_json(&store.frontier_projection(frontiers[0].id)?);
    }
    print_json(&frontiers)
}

fn run_node_add(args: NodeAddArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let frontier_id = args
        .frontier
        .as_deref()
        .map(parse_frontier_id)
        .transpose()?;
    let tags = optional_cli_tags(args.tag_selection, args.class == CliNodeClass::Note)?;
    let payload = load_payload(
        store.schema().schema_ref(),
        args.payload_json,
        args.payload_file,
        args.fields,
    )?;
    let annotations = args
        .annotations
        .into_iter()
        .map(|body| Ok(NodeAnnotation::hidden(NonEmptyText::new(body)?)))
        .collect::<Result<Vec<_>, StoreError>>()?;
    let node = store.add_node(CreateNodeRequest {
        class: args.class.into(),
        frontier_id,
        title: NonEmptyText::new(args.title)?,
        summary: args.summary.map(NonEmptyText::new).transpose()?,
        tags,
        payload,
        annotations,
        attachments: lineage_attachments(args.parents)?,
    })?;
    print_json(&node)
}

fn run_node_list(args: NodeListArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    let items = store.list_nodes(ListNodesQuery {
        frontier_id: args
            .frontier
            .as_deref()
            .map(parse_frontier_id)
            .transpose()?,
        class: args.class.map(Into::into),
        tags: parse_tag_set(args.tags)?,
        include_archived: args.include_archived,
        limit: args.limit,
    })?;
    print_json(&items)
}

fn run_node_show(args: NodeShowArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    let node_id = parse_node_id(&args.node)?;
    let node = store
        .get_node(node_id)?
        .ok_or(StoreError::NodeNotFound(node_id))?;
    print_json(&node)
}

fn run_node_annotate(args: NodeAnnotateArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let annotation = NodeAnnotation {
        id: fidget_spinner_core::AnnotationId::fresh(),
        visibility: if args.visible {
            AnnotationVisibility::Visible
        } else {
            AnnotationVisibility::HiddenByDefault
        },
        label: args.label.map(NonEmptyText::new).transpose()?,
        body: NonEmptyText::new(args.body)?,
        created_at: time::OffsetDateTime::now_utc(),
    };
    store.annotate_node(parse_node_id(&args.node)?, annotation)?;
    println!("annotated {}", args.node);
    Ok(())
}

fn run_node_archive(args: NodeArchiveArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    store.archive_node(parse_node_id(&args.node)?)?;
    println!("archived {}", args.node);
    Ok(())
}

fn run_quick_note(args: QuickNoteArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let payload = NodePayload::with_schema(
        store.schema().schema_ref(),
        json_object(json!({ "body": args.body }))?,
    );
    let node = store.add_node(CreateNodeRequest {
        class: NodeClass::Note,
        frontier_id: args
            .frontier
            .as_deref()
            .map(parse_frontier_id)
            .transpose()?,
        title: NonEmptyText::new(args.title)?,
        summary: None,
        tags: Some(explicit_cli_tags(args.tag_selection)?),
        payload,
        annotations: Vec::new(),
        attachments: lineage_attachments(args.parents)?,
    })?;
    print_json(&node)
}

fn run_tag_add(args: TagAddArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let tag = store.add_tag(
        TagName::new(args.name)?,
        NonEmptyText::new(args.description)?,
    )?;
    print_json(&tag)
}

fn run_tag_list(args: ProjectArg) -> Result<(), StoreError> {
    let store = open_store(&args.project)?;
    print_json(&store.list_tags()?)
}

fn run_quick_research(args: QuickResearchArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let payload = NodePayload::with_schema(
        store.schema().schema_ref(),
        json_object(json!({ "body": args.body }))?,
    );
    let node = store.add_node(CreateNodeRequest {
        class: NodeClass::Research,
        frontier_id: args
            .frontier
            .as_deref()
            .map(parse_frontier_id)
            .transpose()?,
        title: NonEmptyText::new(args.title)?,
        summary: args.summary.map(NonEmptyText::new).transpose()?,
        tags: None,
        payload,
        annotations: Vec::new(),
        attachments: lineage_attachments(args.parents)?,
    })?;
    print_json(&node)
}

fn run_experiment_close(args: ExperimentCloseArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let frontier_id = parse_frontier_id(&args.frontier)?;
    let snapshot = store
        .auto_capture_checkpoint(NonEmptyText::new(args.candidate_summary.clone())?)?
        .map(|seed| seed.snapshot)
        .ok_or(StoreError::GitInspectionFailed(
            store.project_root().to_path_buf(),
        ))?;
    let command = CommandRecipe::new(
        args.working_directory
            .map(utf8_path)
            .unwrap_or_else(|| store.project_root().to_path_buf()),
        to_text_vec(args.argv)?,
        parse_env(args.env),
    )?;
    let receipt = store.close_experiment(CloseExperimentRequest {
        frontier_id,
        base_checkpoint_id: parse_checkpoint_id(&args.base_checkpoint)?,
        change_node_id: parse_node_id(&args.change_node)?,
        candidate_summary: NonEmptyText::new(args.candidate_summary)?,
        candidate_snapshot: snapshot,
        run_title: NonEmptyText::new(args.run_title)?,
        run_summary: args.run_summary.map(NonEmptyText::new).transpose()?,
        backend: args.backend.into(),
        benchmark_suite: NonEmptyText::new(args.benchmark_suite)?,
        command,
        code_snapshot: Some(capture_code_snapshot(store.project_root())?),
        primary_metric: MetricObservation {
            metric_key: NonEmptyText::new(args.primary_metric_key)?,
            unit: args.primary_metric_unit.into(),
            objective: args.primary_metric_objective.into(),
            value: args.primary_metric_value,
        },
        supporting_metrics: args
            .metrics
            .into_iter()
            .map(parse_metric_observation)
            .collect::<Result<Vec<_>, _>>()?,
        note: FrontierNote {
            summary: NonEmptyText::new(args.note)?,
            next_hypotheses: to_text_vec(args.next_hypotheses)?,
        },
        verdict: args.verdict.into(),
        decision_title: NonEmptyText::new(args.decision_title)?,
        decision_rationale: NonEmptyText::new(args.decision_rationale)?,
        analysis_node_id: None,
    })?;
    print_json(&receipt)
}

fn run_skill_install(args: SkillInstallArgs) -> Result<(), StoreError> {
    if let Some(name) = args.name.as_deref() {
        let skill = resolve_bundled_skill(Some(name))?;
        let destination = args
            .destination
            .unwrap_or(default_skill_root()?.join(skill.name));
        install_skill(skill, &destination)?;
        println!("{}", destination.display());
    } else {
        let destination_root = args.destination.unwrap_or(default_skill_root()?);
        for skill in bundled_skill::bundled_skill_summaries() {
            let destination = destination_root.join(skill.name);
            install_skill(resolve_bundled_skill(Some(skill.name))?, &destination)?;
            println!("{}", destination.display());
        }
    }
    Ok(())
}

fn run_ui_serve(args: UiServeArgs) -> Result<(), StoreError> {
    ui::serve(utf8_path(args.project.project), args.bind, args.limit)
}

fn resolve_bundled_skill(
    requested_name: Option<&str>,
) -> Result<bundled_skill::BundledSkill, StoreError> {
    requested_name.map_or_else(
        || Ok(bundled_skill::default_bundled_skill()),
        |name| {
            bundled_skill::bundled_skill(name)
                .ok_or_else(|| invalid_input(format!("unknown bundled skill `{name}`")))
        },
    )
}

fn default_skill_root() -> Result<PathBuf, StoreError> {
    dirs::home_dir()
        .map(|home| home.join(".codex/skills"))
        .ok_or_else(|| invalid_input("home directory not found"))
}

fn install_skill(skill: bundled_skill::BundledSkill, destination: &Path) -> Result<(), StoreError> {
    fs::create_dir_all(destination)?;
    fs::write(destination.join("SKILL.md"), skill.body)?;
    Ok(())
}

fn open_store(path: &Path) -> Result<ProjectStore, StoreError> {
    ProjectStore::open(utf8_path(path.to_path_buf()))
}

fn open_or_init_store_for_binding(path: &Path) -> Result<ProjectStore, StoreError> {
    let requested_root = utf8_path(path.to_path_buf());
    match ProjectStore::open(requested_root.clone()) {
        Ok(store) => Ok(store),
        Err(StoreError::MissingProjectStore(_)) => {
            let project_root = binding_bootstrap_root(&requested_root)?;
            if !is_empty_directory(&project_root)? {
                return Err(StoreError::MissingProjectStore(requested_root));
            }
            ProjectStore::init(
                &project_root,
                default_display_name_for_root(&project_root)?,
                default_namespace_for_root(&project_root)?,
            )
        }
        Err(error) => Err(error),
    }
}

fn utf8_path(path: impl Into<PathBuf>) -> Utf8PathBuf {
    Utf8PathBuf::from(path.into().to_string_lossy().into_owned())
}

fn binding_bootstrap_root(path: &Utf8Path) -> Result<Utf8PathBuf, StoreError> {
    match fs::metadata(path.as_std_path()) {
        Ok(metadata) if metadata.is_file() => Ok(path
            .parent()
            .map_or_else(|| path.to_path_buf(), Utf8Path::to_path_buf)),
        Ok(_) => Ok(path.to_path_buf()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(path.to_path_buf()),
        Err(error) => Err(StoreError::from(error)),
    }
}

fn is_empty_directory(path: &Utf8Path) -> Result<bool, StoreError> {
    match fs::metadata(path.as_std_path()) {
        Ok(metadata) if metadata.is_dir() => {
            let mut entries = fs::read_dir(path.as_std_path())?;
            Ok(entries.next().transpose()?.is_none())
        }
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(StoreError::from(error)),
    }
}

fn default_display_name_for_root(project_root: &Utf8Path) -> Result<NonEmptyText, StoreError> {
    NonEmptyText::new(
        project_root
            .file_name()
            .map_or_else(|| "fidget-spinner-project".to_owned(), ToOwned::to_owned),
    )
    .map_err(StoreError::from)
}

fn default_namespace_for_root(project_root: &Utf8Path) -> Result<NonEmptyText, StoreError> {
    let slug = slugify_namespace_component(project_root.file_name().unwrap_or("project"));
    NonEmptyText::new(format!("local.{slug}")).map_err(StoreError::from)
}

fn slugify_namespace_component(raw: &str) -> String {
    let mut slug = String::new();
    let mut previous_was_separator = false;
    for character in raw.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            slug.push(character);
            previous_was_separator = false;
            continue;
        }
        if !previous_was_separator {
            slug.push('_');
            previous_was_separator = true;
        }
    }
    let slug = slug.trim_matches('_').to_owned();
    if slug.is_empty() {
        "project".to_owned()
    } else {
        slug
    }
}

fn to_text_vec(values: Vec<String>) -> Result<Vec<NonEmptyText>, StoreError> {
    values
        .into_iter()
        .map(NonEmptyText::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::from)
}

fn to_text_set(values: Vec<String>) -> Result<BTreeSet<NonEmptyText>, StoreError> {
    to_text_vec(values).map(BTreeSet::from_iter)
}

fn parse_tag_set(values: Vec<String>) -> Result<BTreeSet<TagName>, StoreError> {
    values
        .into_iter()
        .map(TagName::new)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(StoreError::from)
}

fn explicit_cli_tags(selection: ExplicitTagSelectionArgs) -> Result<BTreeSet<TagName>, StoreError> {
    optional_cli_tags(selection, true)?.ok_or(StoreError::NoteTagsRequired)
}

fn optional_cli_tags(
    selection: ExplicitTagSelectionArgs,
    required: bool,
) -> Result<Option<BTreeSet<TagName>>, StoreError> {
    if selection.no_tags {
        return Ok(Some(BTreeSet::new()));
    }
    if selection.tags.is_empty() {
        return if required {
            Err(StoreError::NoteTagsRequired)
        } else {
            Ok(None)
        };
    }
    Ok(Some(parse_tag_set(selection.tags)?))
}

fn parse_env(values: Vec<String>) -> BTreeMap<String, String> {
    values
        .into_iter()
        .filter_map(|entry| {
            let (key, value) = entry.split_once('=')?;
            Some((key.to_owned(), value.to_owned()))
        })
        .collect()
}

fn lineage_attachments(parents: Vec<String>) -> Result<Vec<EdgeAttachment>, StoreError> {
    parents
        .into_iter()
        .map(|parent| {
            Ok(EdgeAttachment {
                node_id: parse_node_id(&parent)?,
                kind: fidget_spinner_core::EdgeKind::Lineage,
                direction: EdgeAttachmentDirection::ExistingToNew,
            })
        })
        .collect()
}

fn load_payload(
    schema: fidget_spinner_core::PayloadSchemaRef,
    payload_json: Option<String>,
    payload_file: Option<PathBuf>,
    fields: Vec<String>,
) -> Result<NodePayload, StoreError> {
    let mut map = Map::new();
    if let Some(text) = payload_json {
        map.extend(json_object(serde_json::from_str::<Value>(&text)?)?);
    }
    if let Some(path) = payload_file {
        let text = fs::read_to_string(path)?;
        map.extend(json_object(serde_json::from_str::<Value>(&text)?)?);
    }
    for field in fields {
        let Some((key, raw_value)) = field.split_once('=') else {
            continue;
        };
        let value = serde_json::from_str::<Value>(raw_value).unwrap_or_else(|_| json!(raw_value));
        let _ = map.insert(key.to_owned(), value);
    }
    Ok(NodePayload::with_schema(schema, map))
}

fn json_object(value: Value) -> Result<Map<String, Value>, StoreError> {
    match value {
        Value::Object(map) => Ok(map),
        other => Err(invalid_input(format!(
            "expected JSON object, got {other:?}"
        ))),
    }
}

fn capture_code_snapshot(project_root: &Utf8Path) -> Result<CodeSnapshotRef, StoreError> {
    let head_commit = run_git(project_root, &["rev-parse", "HEAD"])?;
    let dirty_paths = run_git(project_root, &["status", "--porcelain"])?
        .map(|status| {
            status
                .lines()
                .filter_map(|line| line.get(3..).map(str::trim))
                .filter(|line| !line.is_empty())
                .map(Utf8PathBuf::from)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    Ok(CodeSnapshotRef {
        repo_root: run_git(project_root, &["rev-parse", "--show-toplevel"])?
            .map(Utf8PathBuf::from)
            .unwrap_or_else(|| project_root.to_path_buf()),
        worktree_root: project_root.to_path_buf(),
        worktree_name: run_git(project_root, &["rev-parse", "--abbrev-ref", "HEAD"])?
            .map(NonEmptyText::new)
            .transpose()?,
        head_commit: head_commit.map(GitCommitHash::new).transpose()?,
        dirty_paths,
    })
}

fn run_git(project_root: &Utf8Path, args: &[&str]) -> Result<Option<String>, StoreError> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(project_root.as_str())
        .args(args)
        .output()?;
    if !output.status.success() {
        return Ok(None);
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if text.is_empty() {
        return Ok(None);
    }
    Ok(Some(text))
}

fn maybe_print_gitignore_hint(project_root: &Utf8Path) -> Result<(), StoreError> {
    if run_git(project_root, &["rev-parse", "--show-toplevel"])?.is_none() {
        return Ok(());
    }

    let status = std::process::Command::new("git")
        .arg("-C")
        .arg(project_root.as_str())
        .args(["check-ignore", "-q", ".fidget_spinner"])
        .status()?;

    match status.code() {
        Some(0) => Ok(()),
        Some(1) => {
            println!(
                "note: add `.fidget_spinner/` to `.gitignore` or `.git/info/exclude` if you do not want local state in `git status`"
            );
            Ok(())
        }
        _ => Ok(()),
    }
}

fn parse_metric_observation(raw: String) -> Result<MetricObservation, StoreError> {
    let parts = raw.split(':').collect::<Vec<_>>();
    if parts.len() != 4 {
        return Err(invalid_input(
            "metrics must look like key:unit:objective:value",
        ));
    }
    Ok(MetricObservation {
        metric_key: NonEmptyText::new(parts[0])?,
        unit: parse_metric_unit(parts[1])?,
        objective: parse_optimization_objective(parts[2])?,
        value: parts[3]
            .parse::<f64>()
            .map_err(|error| invalid_input(format!("invalid metric value: {error}")))?,
    })
}

fn parse_metric_unit(raw: &str) -> Result<MetricUnit, StoreError> {
    match raw {
        "seconds" => Ok(MetricUnit::Seconds),
        "bytes" => Ok(MetricUnit::Bytes),
        "count" => Ok(MetricUnit::Count),
        "ratio" => Ok(MetricUnit::Ratio),
        "custom" => Ok(MetricUnit::Custom),
        other => Err(invalid_input(format!("unknown metric unit `{other}`"))),
    }
}

fn parse_optimization_objective(raw: &str) -> Result<OptimizationObjective, StoreError> {
    match raw {
        "minimize" => Ok(OptimizationObjective::Minimize),
        "maximize" => Ok(OptimizationObjective::Maximize),
        "target" => Ok(OptimizationObjective::Target),
        other => Err(invalid_input(format!(
            "unknown optimization objective `{other}`"
        ))),
    }
}

fn parse_node_id(raw: &str) -> Result<fidget_spinner_core::NodeId, StoreError> {
    Ok(fidget_spinner_core::NodeId::from_uuid(Uuid::parse_str(
        raw,
    )?))
}

fn parse_frontier_id(raw: &str) -> Result<fidget_spinner_core::FrontierId, StoreError> {
    Ok(fidget_spinner_core::FrontierId::from_uuid(Uuid::parse_str(
        raw,
    )?))
}

fn parse_checkpoint_id(raw: &str) -> Result<fidget_spinner_core::CheckpointId, StoreError> {
    Ok(fidget_spinner_core::CheckpointId::from_uuid(
        Uuid::parse_str(raw)?,
    ))
}

fn print_json<T: Serialize>(value: &T) -> Result<(), StoreError> {
    println!("{}", to_pretty_json(value)?);
    Ok(())
}

fn to_pretty_json<T: Serialize>(value: &T) -> Result<String, StoreError> {
    serde_json::to_string_pretty(value).map_err(StoreError::from)
}

fn invalid_input(message: impl Into<String>) -> StoreError {
    StoreError::Json(serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    )))
}

impl From<CliNodeClass> for NodeClass {
    fn from(value: CliNodeClass) -> Self {
        match value {
            CliNodeClass::Contract => Self::Contract,
            CliNodeClass::Change => Self::Change,
            CliNodeClass::Run => Self::Run,
            CliNodeClass::Analysis => Self::Analysis,
            CliNodeClass::Decision => Self::Decision,
            CliNodeClass::Research => Self::Research,
            CliNodeClass::Enabling => Self::Enabling,
            CliNodeClass::Note => Self::Note,
        }
    }
}

impl From<CliMetricUnit> for MetricUnit {
    fn from(value: CliMetricUnit) -> Self {
        match value {
            CliMetricUnit::Seconds => Self::Seconds,
            CliMetricUnit::Bytes => Self::Bytes,
            CliMetricUnit::Count => Self::Count,
            CliMetricUnit::Ratio => Self::Ratio,
            CliMetricUnit::Custom => Self::Custom,
        }
    }
}

impl From<CliOptimizationObjective> for OptimizationObjective {
    fn from(value: CliOptimizationObjective) -> Self {
        match value {
            CliOptimizationObjective::Minimize => Self::Minimize,
            CliOptimizationObjective::Maximize => Self::Maximize,
            CliOptimizationObjective::Target => Self::Target,
        }
    }
}

impl From<CliExecutionBackend> for ExecutionBackend {
    fn from(value: CliExecutionBackend) -> Self {
        match value {
            CliExecutionBackend::Local => Self::LocalProcess,
            CliExecutionBackend::Worktree => Self::WorktreeProcess,
            CliExecutionBackend::Ssh => Self::SshProcess,
        }
    }
}

impl From<CliFrontierVerdict> for FrontierVerdict {
    fn from(value: CliFrontierVerdict) -> Self {
        match value {
            CliFrontierVerdict::PromoteToChampion => Self::PromoteToChampion,
            CliFrontierVerdict::KeepOnFrontier => Self::KeepOnFrontier,
            CliFrontierVerdict::RevertToChampion => Self::RevertToChampion,
            CliFrontierVerdict::ArchiveDeadEnd => Self::ArchiveDeadEnd,
            CliFrontierVerdict::NeedsMoreEvidence => Self::NeedsMoreEvidence,
        }
    }
}
