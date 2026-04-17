mod bundled_skill;
mod mcp;
mod ui;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Parser, Subcommand, ValueEnum};
use fidget_spinner_core::{
    ArtifactKind, CommandRecipe, ExecutionBackend, ExperimentAnalysis, ExperimentStatus,
    FieldValueType, FrontierStatus, FrontierVerdict, MetricAggregation, MetricUnit, NonEmptyText,
    OptimizationObjective, RunDimensionValue, Slug, TagName,
};
use fidget_spinner_store_sqlite::{
    AttachmentSelector, CloseExperimentRequest, CreateArtifactRequest, CreateFrontierRequest,
    CreateHypothesisRequest, CreateKpiRequest, DefineMetricRequest, DefineRunDimensionRequest,
    DeleteMetricRequest, ExperimentOutcomePatch, FrontierRoadmapItemDraft, KpiBestQuery,
    KpiListQuery, ListArtifactsQuery, ListExperimentsQuery, ListFrontiersQuery,
    ListHypothesesQuery, MergeMetricRequest, MetricBestQuery, MetricKeysQuery, MetricRankOrder,
    MetricScope, OpenExperimentRequest, ProjectStore, RenameMetricRequest, StoreError, TextPatch,
    UpdateArtifactRequest, UpdateExperimentRequest, UpdateFrontierRequest, UpdateHypothesisRequest,
    VertexSelector,
};
#[cfg(test)]
use libmcp_testkit as _;
use serde::Serialize;
use serde_json::Value;

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
    /// Initialize a centralized Fidget Spinner store for one project root.
    Init(InitArgs),
    /// Inspect project metadata and coarse counts.
    Project {
        #[command(subcommand)]
        command: ProjectCommand,
    },
    /// Manage the repo-local tag registry.
    Tag {
        #[command(subcommand)]
        command: TagCommand,
    },
    /// Create and inspect frontier scopes.
    Frontier {
        #[command(subcommand)]
        command: FrontierCommand,
    },
    /// Record and inspect hypotheses.
    Hypothesis {
        #[command(subcommand)]
        command: HypothesisCommand,
    },
    /// Open, inspect, update, and close experiments.
    Experiment {
        #[command(subcommand)]
        command: ExperimentCommand,
    },
    /// Register external references and attach them to the ledger.
    Artifact {
        #[command(subcommand)]
        command: ArtifactCommand,
    },
    /// Manage project-level metric definitions and rankings.
    Metric {
        #[command(subcommand)]
        command: MetricCommand,
    },
    /// Manage frontier KPI definitions and rankings.
    Kpi {
        #[command(subcommand)]
        command: KpiCommand,
    },
    /// Define the typed dimension vocabulary used to slice experiments.
    Dimension {
        #[command(subcommand)]
        command: DimensionCommand,
    },
    /// Serve the hardened stdio MCP endpoint.
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },
    /// Serve the local navigator.
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
    #[arg(long, default_value = ".")]
    project: PathBuf,
    #[arg(long)]
    name: Option<String>,
}

#[derive(Subcommand)]
enum ProjectCommand {
    Status(ProjectArg),
}

#[derive(Subcommand)]
enum TagCommand {
    Add(TagAddArgs),
    List(TagListArgs),
}

#[derive(Subcommand)]
enum FrontierCommand {
    Create(FrontierCreateArgs),
    List(FrontierListArgs),
    Read(FrontierSelectorArgs),
    Open(FrontierSelectorArgs),
    Update(FrontierUpdateArgs),
    History(FrontierSelectorArgs),
}

#[derive(Subcommand)]
enum HypothesisCommand {
    Record(HypothesisRecordArgs),
    List(HypothesisListArgs),
    Read(HypothesisSelectorArgs),
    Update(HypothesisUpdateArgs),
    History(HypothesisSelectorArgs),
}

#[derive(Subcommand)]
enum ExperimentCommand {
    Open(ExperimentOpenArgs),
    List(ExperimentListArgs),
    Read(ExperimentSelectorArgs),
    Update(ExperimentUpdateArgs),
    Close(ExperimentCloseArgs),
    Nearest(ExperimentNearestArgs),
    History(ExperimentSelectorArgs),
}

#[derive(Subcommand)]
enum ArtifactCommand {
    Record(ArtifactRecordArgs),
    List(ArtifactListArgs),
    Read(ArtifactSelectorArgs),
    Update(ArtifactUpdateArgs),
    History(ArtifactSelectorArgs),
}

#[derive(Subcommand)]
enum MetricCommand {
    Define(MetricDefineArgs),
    Keys(MetricKeysArgs),
    Best(MetricBestArgs),
    Rename(MetricRenameArgs),
    Merge(MetricMergeArgs),
    Delete(MetricDeleteArgs),
}

#[derive(Subcommand)]
enum KpiCommand {
    Create(KpiCreateArgs),
    List(KpiListArgs),
    Best(KpiBestArgs),
}

#[derive(Subcommand)]
enum DimensionCommand {
    Define(DimensionDefineArgs),
    List(ProjectArg),
}

#[derive(Subcommand)]
enum McpCommand {
    Serve(McpServeArgs),
    Worker(McpWorkerArgs),
}

#[derive(Subcommand)]
enum UiCommand {
    Serve(UiServeArgs),
}

#[derive(Subcommand)]
enum SkillCommand {
    List,
    Install(SkillInstallArgs),
    Show(SkillShowArgs),
}

#[derive(Args, Clone)]
struct ProjectArg {
    #[arg(long, default_value = ".")]
    project: PathBuf,
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
struct TagListArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    include_hidden: bool,
}

#[derive(Args)]
struct FrontierCreateArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    label: String,
    #[arg(long)]
    objective: String,
    #[arg(long)]
    slug: Option<String>,
}

#[derive(Args)]
struct FrontierListArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    include_archived: bool,
}

#[derive(Args)]
struct FrontierSelectorArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: String,
}

#[derive(Args)]
struct FrontierUpdateArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: String,
    #[arg(long)]
    expected_revision: Option<u64>,
    #[arg(long)]
    objective: Option<String>,
    #[arg(long, value_enum)]
    status: Option<CliFrontierStatus>,
    #[command(flatten)]
    situation: FrontierSituationPatchArgs,
    #[command(flatten)]
    unknowns: FrontierUnknownsPatchArgs,
    #[command(flatten)]
    roadmap: FrontierRoadmapPatchArgs,
}

#[derive(Args)]
struct FrontierSituationPatchArgs {
    #[arg(long)]
    situation: Option<String>,
    #[arg(long)]
    clear_situation: bool,
}

#[derive(Args)]
struct FrontierUnknownsPatchArgs {
    #[arg(long = "unknown")]
    unknowns: Vec<String>,
    #[arg(long = "clear-unknowns")]
    clear_unknowns: bool,
}

#[derive(Args)]
struct FrontierRoadmapPatchArgs {
    #[arg(long = "roadmap")]
    roadmap: Vec<String>,
    #[arg(long = "clear-roadmap")]
    clear_roadmap: bool,
}

#[derive(Args)]
struct HypothesisRecordArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: String,
    #[arg(long)]
    title: String,
    #[arg(long)]
    summary: String,
    #[arg(long)]
    body: String,
    #[arg(long)]
    slug: Option<String>,
    #[arg(long = "tag")]
    tags: Vec<String>,
    #[arg(long = "parent")]
    parents: Vec<String>,
}

#[derive(Args)]
struct HypothesisListArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: Option<String>,
    #[arg(long = "tag")]
    tags: Vec<String>,
    #[arg(long)]
    include_archived: bool,
    #[arg(long)]
    limit: Option<u32>,
}

#[derive(Args)]
struct HypothesisSelectorArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    hypothesis: String,
}

#[derive(Args)]
struct HypothesisUpdateArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    hypothesis: String,
    #[arg(long)]
    expected_revision: Option<u64>,
    #[arg(long)]
    title: Option<String>,
    #[arg(long)]
    summary: Option<String>,
    #[arg(long)]
    body: Option<String>,
    #[arg(long = "tag")]
    tags: Vec<String>,
    #[arg(long = "replace-tags")]
    replace_tags: bool,
    #[arg(long = "parent")]
    parents: Vec<String>,
    #[arg(long = "replace-parents")]
    replace_parents: bool,
    #[arg(long, value_enum)]
    state: Option<CliArchivePatch>,
}

#[derive(Args)]
struct ExperimentOpenArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    hypothesis: String,
    #[arg(long)]
    title: String,
    #[arg(long)]
    summary: Option<String>,
    #[arg(long)]
    slug: Option<String>,
    #[arg(long = "tag")]
    tags: Vec<String>,
    #[arg(long = "parent")]
    parents: Vec<String>,
}

#[derive(Args)]
struct ExperimentListArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: Option<String>,
    #[arg(long)]
    hypothesis: Option<String>,
    #[arg(long, value_enum)]
    status: Option<CliExperimentStatus>,
    #[arg(long = "tag")]
    tags: Vec<String>,
    #[arg(long)]
    include_archived: bool,
    #[arg(long)]
    limit: Option<u32>,
}

#[derive(Args)]
struct ExperimentSelectorArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    experiment: String,
}

#[derive(Args)]
struct ExperimentUpdateArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    experiment: String,
    #[arg(long)]
    expected_revision: Option<u64>,
    #[arg(long)]
    title: Option<String>,
    #[arg(long)]
    summary: Option<String>,
    #[arg(long)]
    clear_summary: bool,
    #[arg(long = "tag")]
    tags: Vec<String>,
    #[arg(long = "replace-tags")]
    replace_tags: bool,
    #[arg(long = "parent")]
    parents: Vec<String>,
    #[arg(long = "replace-parents")]
    replace_parents: bool,
    #[arg(long, value_enum)]
    state: Option<CliArchivePatch>,
    #[arg(long = "outcome-json")]
    outcome_json: Option<String>,
    #[arg(long = "outcome-file")]
    outcome_file: Option<PathBuf>,
}

#[derive(Args)]
struct ExperimentCloseArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    experiment: String,
    #[arg(long)]
    expected_revision: Option<u64>,
    #[arg(long, value_enum)]
    backend: CliExecutionBackend,
    #[arg(long = "argv")]
    argv: Vec<String>,
    #[arg(long)]
    working_directory: Option<PathBuf>,
    #[arg(long = "env")]
    env: Vec<String>,
    #[arg(long = "dimension")]
    dimensions: Vec<String>,
    #[arg(long = "primary-metric")]
    primary_metric: String,
    #[arg(long = "metric")]
    supporting_metrics: Vec<String>,
    #[arg(long, value_enum)]
    verdict: CliFrontierVerdict,
    #[arg(long)]
    rationale: String,
    #[arg(long)]
    analysis_summary: Option<String>,
    #[arg(long)]
    analysis_body: Option<String>,
}

#[derive(Args)]
struct ExperimentNearestArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: Option<String>,
    #[arg(long)]
    hypothesis: Option<String>,
    #[arg(long)]
    experiment: Option<String>,
    #[arg(long)]
    metric: Option<String>,
    #[arg(long = "dimension")]
    dimensions: Vec<String>,
    #[arg(long = "tag")]
    tags: Vec<String>,
    #[arg(long, value_enum)]
    order: Option<CliMetricRankOrder>,
}

#[derive(Args)]
struct ArtifactRecordArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    kind: CliArtifactKind,
    #[arg(long)]
    label: String,
    #[arg(long)]
    summary: Option<String>,
    #[arg(long)]
    locator: String,
    #[arg(long)]
    media_type: Option<String>,
    #[arg(long)]
    slug: Option<String>,
    #[arg(long = "attach")]
    attachments: Vec<String>,
}

#[derive(Args)]
struct ArtifactListArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: Option<String>,
    #[arg(long)]
    kind: Option<CliArtifactKind>,
    #[arg(long)]
    attached_to: Option<String>,
    #[arg(long)]
    limit: Option<u32>,
}

#[derive(Args)]
struct ArtifactSelectorArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    artifact: String,
}

#[derive(Args)]
struct ArtifactUpdateArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    artifact: String,
    #[arg(long)]
    expected_revision: Option<u64>,
    #[arg(long)]
    kind: Option<CliArtifactKind>,
    #[arg(long)]
    label: Option<String>,
    #[arg(long)]
    summary: Option<String>,
    #[arg(long)]
    clear_summary: bool,
    #[arg(long)]
    locator: Option<String>,
    #[arg(long)]
    media_type: Option<String>,
    #[arg(long)]
    clear_media_type: bool,
    #[arg(long = "attach")]
    attachments: Vec<String>,
    #[arg(long = "replace-attachments")]
    replace_attachments: bool,
}

#[derive(Args)]
struct MetricDefineArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    key: String,
    #[arg(long)]
    unit: String,
    #[arg(long, value_enum, default_value_t = CliMetricAggregation::Point)]
    aggregation: CliMetricAggregation,
    #[arg(long, value_enum)]
    objective: CliOptimizationObjective,
    #[arg(long)]
    description: Option<String>,
}

#[derive(Args)]
struct MetricKeysArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: Option<String>,
    #[arg(long, value_enum, default_value_t = CliMetricScope::Live)]
    scope: CliMetricScope,
}

#[derive(Args)]
struct MetricBestArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: Option<String>,
    #[arg(long)]
    hypothesis: Option<String>,
    #[arg(long)]
    key: String,
    #[arg(long = "dimension")]
    dimensions: Vec<String>,
    #[arg(long)]
    include_rejected: bool,
    #[arg(long)]
    limit: Option<u32>,
    #[arg(long, value_enum)]
    order: Option<CliMetricRankOrder>,
}

#[derive(Args)]
struct MetricRenameArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    metric: String,
    #[arg(long)]
    new_key: String,
}

#[derive(Args)]
struct MetricMergeArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    source: String,
    #[arg(long)]
    target: String,
}

#[derive(Args)]
struct MetricDeleteArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    metric: String,
}

#[derive(Args)]
struct KpiCreateArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: String,
    #[arg(long)]
    name: String,
    #[arg(long, value_enum)]
    objective: CliOptimizationObjective,
    #[arg(long)]
    description: Option<String>,
    #[arg(long = "metric")]
    metric_keys: Vec<String>,
}

#[derive(Args)]
struct KpiListArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: String,
}

#[derive(Args)]
struct KpiBestArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    frontier: String,
    #[arg(long)]
    kpi: Option<String>,
    #[arg(long = "dimension")]
    dimensions: Vec<String>,
    #[arg(long)]
    include_rejected: bool,
    #[arg(long)]
    limit: Option<u32>,
    #[arg(long)]
    strict: bool,
}

#[derive(Args)]
struct DimensionDefineArgs {
    #[command(flatten)]
    project: ProjectArg,
    #[arg(long)]
    key: String,
    #[arg(long, value_enum)]
    value_type: CliFieldValueType,
    #[arg(long)]
    description: Option<String>,
}

#[derive(Args)]
struct McpServeArgs {
    #[arg(long)]
    project: Option<PathBuf>,
}

#[derive(Args)]
struct McpWorkerArgs {
    #[arg(long, default_value = ".")]
    project: PathBuf,
}

#[derive(Args)]
struct UiServeArgs {
    #[arg(long, default_value = ".")]
    path: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8913")]
    bind: SocketAddr,
    #[arg(long)]
    limit: Option<u32>,
}

#[derive(Args)]
struct SkillInstallArgs {
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    destination: Option<PathBuf>,
}

#[derive(Args)]
struct SkillShowArgs {
    #[arg(long)]
    name: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliOptimizationObjective {
    Minimize,
    Maximize,
    Target,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliMetricScope {
    Kpi,
    Live,
    Default,
    All,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliMetricAggregation {
    Point,
    Mean,
    Geomean,
    Median,
    P95,
    Min,
    Max,
    Sum,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliMetricRankOrder {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliFieldValueType {
    String,
    Numeric,
    Boolean,
    Timestamp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliArtifactKind {
    Document,
    Link,
    Log,
    Table,
    Plot,
    Dump,
    Binary,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliExecutionBackend {
    Manual,
    LocalProcess,
    WorktreeProcess,
    SshProcess,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliFrontierVerdict {
    Accepted,
    Kept,
    Parked,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliExperimentStatus {
    Open,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliFrontierStatus {
    Exploring,
    Paused,
    Archived,
}

impl From<CliFrontierStatus> for FrontierStatus {
    fn from(value: CliFrontierStatus) -> Self {
        match value {
            CliFrontierStatus::Exploring => Self::Exploring,
            CliFrontierStatus::Paused => Self::Paused,
            CliFrontierStatus::Archived => Self::Archived,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliArchivePatch {
    Archive,
    Restore,
}

fn main() -> Result<(), StoreError> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init(args) => run_init(args),
        Command::Project { command } => match command {
            ProjectCommand::Status(args) => print_json(&open_store(&args.project)?.status()?),
        },
        Command::Tag { command } => match command {
            TagCommand::Add(args) => run_tag_add(args),
            TagCommand::List(args) => {
                print_json(&open_store(&args.project.project)?.tag_registry(
                    fidget_spinner_store_sqlite::TagRegistryQuery {
                        include_hidden: args.include_hidden,
                    },
                )?)
            }
        },
        Command::Frontier { command } => match command {
            FrontierCommand::Create(args) => run_frontier_create(args),
            FrontierCommand::List(args) => print_json(
                &open_store(&args.project.project)?.list_frontiers(ListFrontiersQuery {
                    include_archived: args.include_archived,
                })?,
            ),
            FrontierCommand::Read(args) => {
                print_json(&open_store(&args.project.project)?.read_frontier(&args.frontier)?)
            }
            FrontierCommand::Open(args) => {
                print_json(&open_store(&args.project.project)?.frontier_open(&args.frontier)?)
            }
            FrontierCommand::Update(args) => run_frontier_update(args),
            FrontierCommand::History(args) => {
                print_json(&open_store(&args.project.project)?.frontier_history(&args.frontier)?)
            }
        },
        Command::Hypothesis { command } => match command {
            HypothesisCommand::Record(args) => run_hypothesis_record(args),
            HypothesisCommand::List(args) => run_hypothesis_list(args),
            HypothesisCommand::Read(args) => {
                print_json(&open_store(&args.project.project)?.read_hypothesis(&args.hypothesis)?)
            }
            HypothesisCommand::Update(args) => run_hypothesis_update(args),
            HypothesisCommand::History(args) => print_json(
                &open_store(&args.project.project)?.hypothesis_history(&args.hypothesis)?,
            ),
        },
        Command::Experiment { command } => match command {
            ExperimentCommand::Open(args) => run_experiment_open(args),
            ExperimentCommand::List(args) => run_experiment_list(args),
            ExperimentCommand::Read(args) => {
                print_json(&open_store(&args.project.project)?.read_experiment(&args.experiment)?)
            }
            ExperimentCommand::Update(args) => run_experiment_update(args),
            ExperimentCommand::Close(args) => run_experiment_close(args),
            ExperimentCommand::Nearest(args) => run_experiment_nearest(args),
            ExperimentCommand::History(args) => print_json(
                &open_store(&args.project.project)?.experiment_history(&args.experiment)?,
            ),
        },
        Command::Artifact { command } => match command {
            ArtifactCommand::Record(args) => run_artifact_record(args),
            ArtifactCommand::List(args) => run_artifact_list(args),
            ArtifactCommand::Read(args) => {
                print_json(&open_store(&args.project.project)?.read_artifact(&args.artifact)?)
            }
            ArtifactCommand::Update(args) => run_artifact_update(args),
            ArtifactCommand::History(args) => {
                print_json(&open_store(&args.project.project)?.artifact_history(&args.artifact)?)
            }
        },
        Command::Metric { command } => match command {
            MetricCommand::Define(args) => run_metric_define(args),
            MetricCommand::Keys(args) => run_metric_keys(args),
            MetricCommand::Best(args) => run_metric_best(args),
            MetricCommand::Rename(args) => run_metric_rename(args),
            MetricCommand::Merge(args) => run_metric_merge(args),
            MetricCommand::Delete(args) => run_metric_delete(args),
        },
        Command::Kpi { command } => match command {
            KpiCommand::Create(args) => run_kpi_create(args),
            KpiCommand::List(args) => run_kpi_list(args),
            KpiCommand::Best(args) => run_kpi_best(args),
        },
        Command::Dimension { command } => match command {
            DimensionCommand::Define(args) => run_dimension_define(args),
            DimensionCommand::List(args) => {
                print_json(&open_store(&args.project)?.list_run_dimensions()?)
            }
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
    let store = ProjectStore::init(
        &project_root,
        args.name
            .map(NonEmptyText::new)
            .transpose()?
            .unwrap_or(default_display_name_for_root(&project_root)?),
    )?;
    print_json(&store.status()?)
}

fn run_tag_add(args: TagAddArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    print_json(&store.register_tag(
        TagName::new(args.name)?,
        NonEmptyText::new(args.description)?,
    )?)
}

fn run_frontier_create(args: FrontierCreateArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    print_json(&store.create_frontier(CreateFrontierRequest {
        label: NonEmptyText::new(args.label)?,
        objective: NonEmptyText::new(args.objective)?,
        slug: args.slug.map(Slug::new).transpose()?,
    })?)
}

fn run_frontier_update(args: FrontierUpdateArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let roadmap = if args.roadmap.clear_roadmap {
        Some(Vec::new())
    } else if args.roadmap.roadmap.is_empty() {
        None
    } else {
        Some(
            args.roadmap
                .roadmap
                .into_iter()
                .map(parse_roadmap_item)
                .collect::<Result<Vec<_>, _>>()?,
        )
    };
    let unknowns = if args.unknowns.clear_unknowns {
        Some(Vec::new())
    } else if args.unknowns.unknowns.is_empty() {
        None
    } else {
        Some(to_non_empty_texts(args.unknowns.unknowns)?)
    };
    print_json(&store.update_frontier(UpdateFrontierRequest {
        frontier: args.frontier,
        expected_revision: args.expected_revision,
        objective: args.objective.map(NonEmptyText::new).transpose()?,
        status: args.status.map(FrontierStatus::from),
        situation: cli_text_patch(args.situation.situation, args.situation.clear_situation)?,
        roadmap,
        unknowns,
    })?)
}

fn run_hypothesis_record(args: HypothesisRecordArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    print_json(&store.create_hypothesis(CreateHypothesisRequest {
        frontier: args.frontier,
        slug: args.slug.map(Slug::new).transpose()?,
        title: NonEmptyText::new(args.title)?,
        summary: NonEmptyText::new(args.summary)?,
        body: NonEmptyText::new(args.body)?,
        tags: parse_tag_set(args.tags)?,
        parents: parse_vertex_selectors(args.parents)?,
    })?)
}

fn run_hypothesis_list(args: HypothesisListArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    print_json(&store.list_hypotheses(ListHypothesesQuery {
        frontier: args.frontier,
        tags: parse_tag_set(args.tags)?,
        include_archived: args.include_archived,
        limit: args.limit,
    })?)
}

fn run_hypothesis_update(args: HypothesisUpdateArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let tags = if args.replace_tags {
        Some(parse_tag_set(args.tags)?)
    } else {
        None
    };
    let parents = if args.replace_parents {
        Some(parse_vertex_selectors(args.parents)?)
    } else {
        None
    };
    print_json(&store.update_hypothesis(UpdateHypothesisRequest {
        hypothesis: args.hypothesis,
        expected_revision: args.expected_revision,
        title: args.title.map(NonEmptyText::new).transpose()?,
        summary: args.summary.map(NonEmptyText::new).transpose()?,
        body: args.body.map(NonEmptyText::new).transpose()?,
        tags,
        parents,
        archived: archive_patch(args.state),
    })?)
}

fn run_experiment_open(args: ExperimentOpenArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    print_json(&store.open_experiment(OpenExperimentRequest {
        hypothesis: args.hypothesis,
        slug: args.slug.map(Slug::new).transpose()?,
        title: NonEmptyText::new(args.title)?,
        summary: args.summary.map(NonEmptyText::new).transpose()?,
        tags: parse_tag_set(args.tags)?,
        parents: parse_vertex_selectors(args.parents)?,
    })?)
}

fn run_experiment_list(args: ExperimentListArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    print_json(&store.list_experiments(ListExperimentsQuery {
        frontier: args.frontier,
        hypothesis: args.hypothesis,
        tags: parse_tag_set(args.tags)?,
        include_archived: args.include_archived,
        status: args.status.map(Into::into),
        limit: args.limit,
    })?)
}

fn run_experiment_update(args: ExperimentUpdateArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let outcome =
        load_optional_json::<ExperimentOutcomePatch>(args.outcome_json, args.outcome_file)?;
    print_json(&store.update_experiment(UpdateExperimentRequest {
        experiment: args.experiment,
        expected_revision: args.expected_revision,
        title: args.title.map(NonEmptyText::new).transpose()?,
        summary: cli_text_patch(args.summary, args.clear_summary)?,
        tags: if args.replace_tags {
            Some(parse_tag_set(args.tags)?)
        } else {
            None
        },
        parents: if args.replace_parents {
            Some(parse_vertex_selectors(args.parents)?)
        } else {
            None
        },
        archived: archive_patch(args.state),
        outcome,
    })?)
}

fn run_experiment_close(args: ExperimentCloseArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    let analysis = match (args.analysis_summary, args.analysis_body) {
        (Some(summary), Some(body)) => Some(ExperimentAnalysis {
            summary: NonEmptyText::new(summary)?,
            body: NonEmptyText::new(body)?,
        }),
        (None, None) => None,
        _ => {
            return Err(invalid_input(
                "analysis requires both --analysis-summary and --analysis-body",
            ));
        }
    };
    print_json(
        &store.close_experiment(CloseExperimentRequest {
            experiment: args.experiment,
            expected_revision: args.expected_revision,
            backend: args.backend.into(),
            command: CommandRecipe::new(
                args.working_directory.map(utf8_path),
                to_non_empty_texts(args.argv)?,
                parse_env(args.env),
            )?,
            dimensions: parse_dimension_assignments(args.dimensions)?,
            primary_metric: parse_metric_value_assignment(&args.primary_metric)?,
            supporting_metrics: args
                .supporting_metrics
                .into_iter()
                .map(|raw| parse_metric_value_assignment(&raw))
                .collect::<Result<Vec<_>, _>>()?,
            verdict: args.verdict.into(),
            rationale: NonEmptyText::new(args.rationale)?,
            analysis,
        })?,
    )
}

fn run_experiment_nearest(args: ExperimentNearestArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    print_json(
        &store.experiment_nearest(fidget_spinner_store_sqlite::ExperimentNearestQuery {
            frontier: args.frontier,
            hypothesis: args.hypothesis,
            experiment: args.experiment,
            metric: args.metric.map(NonEmptyText::new).transpose()?,
            dimensions: parse_dimension_assignments(args.dimensions)?,
            tags: parse_tag_set(args.tags)?,
            order: args.order.map(Into::into),
        })?,
    )
}

fn run_artifact_record(args: ArtifactRecordArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    print_json(&store.create_artifact(CreateArtifactRequest {
        slug: args.slug.map(Slug::new).transpose()?,
        kind: args.kind.into(),
        label: NonEmptyText::new(args.label)?,
        summary: args.summary.map(NonEmptyText::new).transpose()?,
        locator: NonEmptyText::new(args.locator)?,
        media_type: args.media_type.map(NonEmptyText::new).transpose()?,
        attachments: parse_attachment_selectors(args.attachments)?,
    })?)
}

fn run_artifact_list(args: ArtifactListArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    print_json(
        &store.list_artifacts(ListArtifactsQuery {
            frontier: args.frontier,
            kind: args.kind.map(Into::into),
            attached_to: args
                .attached_to
                .as_deref()
                .map(parse_attachment_selector)
                .transpose()?,
            limit: args.limit,
        })?,
    )
}

fn run_artifact_update(args: ArtifactUpdateArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    print_json(&store.update_artifact(UpdateArtifactRequest {
        artifact: args.artifact,
        expected_revision: args.expected_revision,
        kind: args.kind.map(Into::into),
        label: args.label.map(NonEmptyText::new).transpose()?,
        summary: cli_text_patch(args.summary, args.clear_summary)?,
        locator: args.locator.map(NonEmptyText::new).transpose()?,
        media_type: cli_text_patch(args.media_type, args.clear_media_type)?,
        attachments: if args.replace_attachments {
            Some(parse_attachment_selectors(args.attachments)?)
        } else {
            None
        },
    })?)
}

fn run_metric_define(args: MetricDefineArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    print_json(&store.define_metric(DefineMetricRequest {
        key: NonEmptyText::new(args.key)?,
        unit: MetricUnit::new(args.unit)?,
        aggregation: args.aggregation.into(),
        objective: args.objective.into(),
        description: args.description.map(NonEmptyText::new).transpose()?,
    })?)
}

fn run_metric_keys(args: MetricKeysArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    print_json(&store.metric_keys(MetricKeysQuery {
        frontier: args.frontier,
        scope: args.scope.into(),
    })?)
}

fn run_metric_best(args: MetricBestArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    print_json(&store.metric_best(MetricBestQuery {
        frontier: args.frontier,
        hypothesis: args.hypothesis,
        key: NonEmptyText::new(args.key)?,
        dimensions: parse_dimension_assignments(args.dimensions)?,
        include_rejected: args.include_rejected,
        limit: args.limit,
        order: args.order.map(Into::into),
    })?)
}

fn run_metric_rename(args: MetricRenameArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    print_json(&store.rename_metric(RenameMetricRequest {
        metric: NonEmptyText::new(args.metric)?,
        new_key: NonEmptyText::new(args.new_key)?,
    })?)
}

fn run_metric_merge(args: MetricMergeArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    store.merge_metric(MergeMetricRequest {
        source: NonEmptyText::new(args.source)?,
        target: NonEmptyText::new(args.target)?,
    })?;
    print_json(&serde_json::json!({"merged": true}))
}

fn run_metric_delete(args: MetricDeleteArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    store.delete_metric(DeleteMetricRequest {
        metric: NonEmptyText::new(args.metric)?,
    })?;
    print_json(&serde_json::json!({"deleted": true}))
}

fn run_kpi_create(args: KpiCreateArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    print_json(
        &store.create_kpi(CreateKpiRequest {
            frontier: args.frontier,
            name: NonEmptyText::new(args.name)?,
            objective: args.objective.into(),
            description: args.description.map(NonEmptyText::new).transpose()?,
            metric_keys: args
                .metric_keys
                .into_iter()
                .map(NonEmptyText::new)
                .collect::<Result<Vec<_>, _>>()?,
        })?,
    )
}

fn run_kpi_list(args: KpiListArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    print_json(&store.list_kpis(KpiListQuery {
        frontier: args.frontier,
    })?)
}

fn run_kpi_best(args: KpiBestArgs) -> Result<(), StoreError> {
    let store = open_store(&args.project.project)?;
    print_json(&store.kpi_best(KpiBestQuery {
        frontier: args.frontier,
        kpi: args.kpi,
        dimensions: parse_dimension_assignments(args.dimensions)?,
        include_rejected: args.include_rejected,
        limit: args.limit,
        strict: args.strict,
    })?)
}

fn run_dimension_define(args: DimensionDefineArgs) -> Result<(), StoreError> {
    let mut store = open_store(&args.project.project)?;
    print_json(&store.define_run_dimension(DefineRunDimensionRequest {
        key: NonEmptyText::new(args.key)?,
        value_type: args.value_type.into(),
        description: args.description.map(NonEmptyText::new).transpose()?,
    })?)
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
    let scope = resolve_ui_scope(&utf8_path(args.path))?;
    ui::serve(scope, args.bind, args.limit)
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

pub(crate) fn open_store(path: &Path) -> Result<ProjectStore, StoreError> {
    ProjectStore::open(utf8_path(path.to_path_buf()))
}

pub(crate) fn resolve_ui_scope(path: &Utf8Path) -> Result<ui::NavigatorScope, StoreError> {
    if let Some(project_root) = fidget_spinner_store_sqlite::discover_project_root(path)? {
        return Ok(ui::NavigatorScope::Single(project_root));
    }
    let scan_root = binding_bootstrap_root(path)?;
    let candidates = discover_descendant_project_roots(path)?;
    match candidates.len() {
        0 => Err(StoreError::MissingProjectStore(path.to_path_buf())),
        _ => Ok(ui::NavigatorScope::Multi {
            scan_root,
            project_roots: candidates,
        }),
    }
}

pub(crate) fn open_or_init_store_for_binding(path: &Path) -> Result<ProjectStore, StoreError> {
    let requested_root = utf8_path(path.to_path_buf());
    let project_root = if let Some(project_root) =
        fidget_spinner_store_sqlite::discover_project_root(&requested_root)?
    {
        project_root
    } else {
        fidget_spinner_store_sqlite::preferred_project_root(&requested_root)?
    };
    match ProjectStore::open(project_root.clone()) {
        Ok(store) => Ok(store),
        Err(StoreError::MissingProjectStore(_)) => {
            ProjectStore::init(&project_root, default_display_name_for_root(&project_root)?)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn utf8_path(path: impl Into<PathBuf>) -> Utf8PathBuf {
    Utf8PathBuf::from(path.into().to_string_lossy().into_owned())
}

fn binding_bootstrap_root(path: &Utf8Path) -> Result<Utf8PathBuf, StoreError> {
    if matches!(
        path.file_name(),
        Some(fidget_spinner_store_sqlite::STORE_DIR_NAME)
            | Some(fidget_spinner_store_sqlite::GIT_DIR_NAME)
    ) {
        return Ok(path
            .parent()
            .map_or_else(|| path.to_path_buf(), Utf8Path::to_path_buf));
    }
    match fs::metadata(path.as_std_path()) {
        Ok(metadata) if metadata.is_file() => Ok(path
            .parent()
            .map_or_else(|| path.to_path_buf(), Utf8Path::to_path_buf)),
        Ok(_) => Ok(path.to_path_buf()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(path.to_path_buf()),
        Err(error) => Err(StoreError::from(error)),
    }
}

fn discover_descendant_project_roots(path: &Utf8Path) -> Result<BTreeSet<Utf8PathBuf>, StoreError> {
    let start = binding_bootstrap_root(path)?;
    let mut candidates = BTreeSet::new();
    collect_descendant_project_roots(&start, &mut candidates)?;
    Ok(candidates)
}

fn collect_descendant_project_roots(
    path: &Utf8Path,
    candidates: &mut BTreeSet<Utf8PathBuf>,
) -> Result<(), StoreError> {
    let metadata = match fs::metadata(path.as_std_path()) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(StoreError::from(error)),
    };
    if metadata.is_file() {
        return Ok(());
    }
    if let Some(project_root) = fidget_spinner_store_sqlite::discover_project_root(path)? {
        let _ = candidates.insert(project_root);
        return Ok(());
    }
    for entry in fs::read_dir(path.as_std_path())? {
        let entry = entry?;
        let entry_type = entry.file_type()?;
        if !entry_type.is_dir() {
            continue;
        }
        let child = utf8_path(entry.path());
        collect_descendant_project_roots(&child, candidates)?;
    }
    Ok(())
}

fn default_display_name_for_root(project_root: &Utf8Path) -> Result<NonEmptyText, StoreError> {
    NonEmptyText::new(
        project_root
            .file_name()
            .map_or_else(|| "fidget-spinner-project".to_owned(), ToOwned::to_owned),
    )
    .map_err(StoreError::from)
}

fn parse_tag_set(values: Vec<String>) -> Result<BTreeSet<TagName>, StoreError> {
    values
        .into_iter()
        .map(TagName::new)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(StoreError::from)
}

pub(crate) fn parse_vertex_selectors(
    values: Vec<String>,
) -> Result<Vec<VertexSelector>, StoreError> {
    values
        .into_iter()
        .map(|raw| {
            let (kind, selector) = raw
                .split_once(':')
                .ok_or_else(|| invalid_input("expected parent selector in the form `hypothesis:<selector>` or `experiment:<selector>`"))?;
            match kind {
                "hypothesis" => Ok(VertexSelector::Hypothesis(selector.to_owned())),
                "experiment" => Ok(VertexSelector::Experiment(selector.to_owned())),
                _ => Err(invalid_input(format!("unknown parent kind `{kind}`"))),
            }
        })
        .collect()
}

pub(crate) fn parse_attachment_selectors(
    values: Vec<String>,
) -> Result<Vec<AttachmentSelector>, StoreError> {
    values
        .into_iter()
        .map(|raw| parse_attachment_selector(&raw))
        .collect()
}

pub(crate) fn parse_attachment_selector(raw: &str) -> Result<AttachmentSelector, StoreError> {
    let (kind, selector) = raw
        .split_once(':')
        .ok_or_else(|| invalid_input("expected attachment selector in the form `frontier:<selector>`, `hypothesis:<selector>`, or `experiment:<selector>`"))?;
    match kind {
        "frontier" => Ok(AttachmentSelector::Frontier(selector.to_owned())),
        "hypothesis" => Ok(AttachmentSelector::Hypothesis(selector.to_owned())),
        "experiment" => Ok(AttachmentSelector::Experiment(selector.to_owned())),
        _ => Err(invalid_input(format!("unknown attachment kind `{kind}`"))),
    }
}

fn parse_roadmap_item(raw: String) -> Result<FrontierRoadmapItemDraft, StoreError> {
    let mut parts = raw.splitn(3, ':');
    let rank = parts
        .next()
        .ok_or_else(|| invalid_input("roadmap items must look like `rank:hypothesis[:summary]`"))?
        .parse::<u32>()
        .map_err(|error| invalid_input(format!("invalid roadmap rank: {error}")))?;
    let hypothesis = parts
        .next()
        .ok_or_else(|| invalid_input("roadmap items must include a hypothesis selector"))?
        .to_owned();
    let summary = parts
        .next()
        .map(NonEmptyText::new)
        .transpose()
        .map_err(StoreError::from)?;
    Ok(FrontierRoadmapItemDraft {
        rank,
        hypothesis,
        summary,
    })
}

pub(crate) fn parse_env(values: Vec<String>) -> BTreeMap<String, String> {
    values
        .into_iter()
        .filter_map(|entry| {
            let (key, value) = entry.split_once('=')?;
            Some((key.to_owned(), value.to_owned()))
        })
        .collect()
}

fn parse_metric_value_assignment(
    raw: &str,
) -> Result<fidget_spinner_core::MetricValue, StoreError> {
    let (key, value) = raw
        .split_once('=')
        .ok_or_else(|| invalid_input("expected metric assignment in the form `key=value`"))?;
    let value = value
        .parse::<f64>()
        .map_err(|error| invalid_input(format!("invalid metric value `{value}`: {error}")))?;
    Ok(fidget_spinner_core::MetricValue {
        key: NonEmptyText::new(key.to_owned())?,
        value,
    })
}

pub(crate) fn parse_dimension_assignments(
    values: Vec<String>,
) -> Result<BTreeMap<NonEmptyText, RunDimensionValue>, StoreError> {
    values
        .into_iter()
        .map(|entry| {
            let (key, raw_value) = entry.split_once('=').ok_or_else(|| {
                invalid_input("expected dimension assignment in the form `key=value`")
            })?;
            let json_value = serde_json::from_str::<Value>(raw_value)
                .unwrap_or_else(|_| Value::String(raw_value.to_owned()));
            Ok((
                NonEmptyText::new(key.to_owned())?,
                json_to_dimension_value(json_value)?,
            ))
        })
        .collect()
}

fn json_to_dimension_value(value: Value) -> Result<RunDimensionValue, StoreError> {
    match value {
        Value::String(raw) => {
            if time::OffsetDateTime::parse(&raw, &time::format_description::well_known::Rfc3339)
                .is_ok()
            {
                Ok(RunDimensionValue::Timestamp(NonEmptyText::new(raw)?))
            } else {
                Ok(RunDimensionValue::String(NonEmptyText::new(raw)?))
            }
        }
        Value::Number(number) => number
            .as_f64()
            .map(RunDimensionValue::Numeric)
            .ok_or_else(|| invalid_input("numeric dimension values must fit into f64")),
        Value::Bool(value) => Ok(RunDimensionValue::Boolean(value)),
        _ => Err(invalid_input(
            "dimension values must be string, number, boolean, or RFC3339 timestamp",
        )),
    }
}

fn to_non_empty_texts(values: Vec<String>) -> Result<Vec<NonEmptyText>, StoreError> {
    values
        .into_iter()
        .map(NonEmptyText::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::from)
}

fn load_optional_json<T: for<'de> serde::Deserialize<'de>>(
    inline: Option<String>,
    file: Option<PathBuf>,
) -> Result<Option<T>, StoreError> {
    match (inline, file) {
        (Some(raw), None) => serde_json::from_str(&raw)
            .map(Some)
            .map_err(StoreError::from),
        (None, Some(path)) => serde_json::from_slice(&fs::read(path)?)
            .map(Some)
            .map_err(StoreError::from),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(invalid_input(
            "use only one of --outcome-json or --outcome-file",
        )),
    }
}

const fn archive_patch(state: Option<CliArchivePatch>) -> Option<bool> {
    match state {
        None => None,
        Some(CliArchivePatch::Archive) => Some(true),
        Some(CliArchivePatch::Restore) => Some(false),
    }
}

fn cli_text_patch(
    value: Option<String>,
    clear: bool,
) -> Result<Option<TextPatch<NonEmptyText>>, StoreError> {
    if clear {
        if value.is_some() {
            return Err(invalid_input("cannot set and clear the same field"));
        }
        return Ok(Some(TextPatch::Clear));
    }
    value
        .map(NonEmptyText::new)
        .transpose()
        .map(|value| value.map(TextPatch::Set))
        .map_err(StoreError::from)
}

fn invalid_input(message: impl Into<String>) -> StoreError {
    StoreError::InvalidInput(message.into())
}

pub(crate) fn to_pretty_json(value: &impl Serialize) -> Result<String, StoreError> {
    serde_json::to_string_pretty(value).map_err(StoreError::from)
}

fn print_json(value: &impl Serialize) -> Result<(), StoreError> {
    println!("{}", to_pretty_json(value)?);
    Ok(())
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

impl From<CliMetricScope> for MetricScope {
    fn from(value: CliMetricScope) -> Self {
        match value {
            CliMetricScope::Kpi => Self::Kpi,
            CliMetricScope::Live => Self::Live,
            CliMetricScope::Default => Self::Default,
            CliMetricScope::All => Self::All,
        }
    }
}

impl From<CliMetricAggregation> for MetricAggregation {
    fn from(value: CliMetricAggregation) -> Self {
        match value {
            CliMetricAggregation::Point => Self::Point,
            CliMetricAggregation::Mean => Self::Mean,
            CliMetricAggregation::Geomean => Self::Geomean,
            CliMetricAggregation::Median => Self::Median,
            CliMetricAggregation::P95 => Self::P95,
            CliMetricAggregation::Min => Self::Min,
            CliMetricAggregation::Max => Self::Max,
            CliMetricAggregation::Sum => Self::Sum,
        }
    }
}

impl From<CliMetricRankOrder> for MetricRankOrder {
    fn from(value: CliMetricRankOrder) -> Self {
        match value {
            CliMetricRankOrder::Asc => Self::Asc,
            CliMetricRankOrder::Desc => Self::Desc,
        }
    }
}

impl From<CliFieldValueType> for FieldValueType {
    fn from(value: CliFieldValueType) -> Self {
        match value {
            CliFieldValueType::String => Self::String,
            CliFieldValueType::Numeric => Self::Numeric,
            CliFieldValueType::Boolean => Self::Boolean,
            CliFieldValueType::Timestamp => Self::Timestamp,
        }
    }
}

impl From<CliArtifactKind> for ArtifactKind {
    fn from(value: CliArtifactKind) -> Self {
        match value {
            CliArtifactKind::Document => Self::Document,
            CliArtifactKind::Link => Self::Link,
            CliArtifactKind::Log => Self::Log,
            CliArtifactKind::Table => Self::Table,
            CliArtifactKind::Plot => Self::Plot,
            CliArtifactKind::Dump => Self::Dump,
            CliArtifactKind::Binary => Self::Binary,
            CliArtifactKind::Other => Self::Other,
        }
    }
}

impl From<CliExecutionBackend> for ExecutionBackend {
    fn from(value: CliExecutionBackend) -> Self {
        match value {
            CliExecutionBackend::Manual => Self::Manual,
            CliExecutionBackend::LocalProcess => Self::LocalProcess,
            CliExecutionBackend::WorktreeProcess => Self::WorktreeProcess,
            CliExecutionBackend::SshProcess => Self::SshProcess,
        }
    }
}

impl From<CliFrontierVerdict> for FrontierVerdict {
    fn from(value: CliFrontierVerdict) -> Self {
        match value {
            CliFrontierVerdict::Accepted => Self::Accepted,
            CliFrontierVerdict::Kept => Self::Kept,
            CliFrontierVerdict::Parked => Self::Parked,
            CliFrontierVerdict::Rejected => Self::Rejected,
        }
    }
}

impl From<CliExperimentStatus> for ExperimentStatus {
    fn from(value: CliExperimentStatus) -> Self {
        match value {
            CliExperimentStatus::Open => Self::Open,
            CliExperimentStatus::Closed => Self::Closed,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn fresh_temp_root(label: &str) -> Result<Utf8PathBuf, Box<dyn Error>> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        let root = std::env::temp_dir().join(format!(
            "fidget-spinner-cli-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&root)?;
        Ok(utf8_path(root))
    }

    #[test]
    fn scan_root_with_single_descendant_project_stays_multi() -> Result<(), Box<dyn Error>> {
        let root = fresh_temp_root("ui-scope-single-descendant")?;
        let project_root = root.join("alpha");
        drop(ProjectStore::init(
            &project_root,
            NonEmptyText::new("Alpha".to_owned())?,
        )?);

        let scope = resolve_ui_scope(&root)?;
        match scope {
            ui::NavigatorScope::Multi {
                scan_root,
                project_roots,
            } => {
                assert_eq!(scan_root, root);
                assert_eq!(project_roots, BTreeSet::from([project_root]));
                Ok(())
            }
            ui::NavigatorScope::Single(project_root) => Err(format!(
                "expected multi-project scan root, got single project {project_root}"
            )
            .into()),
        }
    }

    #[test]
    fn explicit_project_root_stays_single() -> Result<(), Box<dyn Error>> {
        let project_root = fresh_temp_root("ui-scope-explicit-project")?;
        drop(ProjectStore::init(
            &project_root,
            NonEmptyText::new("Explicit".to_owned())?,
        )?);

        let scope = resolve_ui_scope(&project_root)?;
        match scope {
            ui::NavigatorScope::Single(observed_root) => {
                assert_eq!(observed_root, project_root);
                Ok(())
            }
            ui::NavigatorScope::Multi { .. } => {
                Err("expected single-project scope for explicit project root".into())
            }
        }
    }
}
