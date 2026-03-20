use std::io;
use std::net::SocketAddr;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use camino::Utf8PathBuf;
use fidget_spinner_core::{
    AttachmentTargetRef, ExperimentAnalysis, ExperimentOutcome, ExperimentStatus, FrontierRecord,
    FrontierVerdict, MetricUnit, RunDimensionValue, Slug, VertexRef,
};
use fidget_spinner_store_sqlite::{
    ExperimentDetail, ExperimentSummary, FrontierOpenProjection, FrontierSummary,
    HypothesisCurrentState, HypothesisDetail, ProjectStatus, StoreError, VertexSummary,
};
use maud::{DOCTYPE, Markup, PreEscaped, html};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use crate::open_store;

#[derive(Clone)]
struct NavigatorState {
    project_root: Utf8PathBuf,
    limit: Option<u32>,
}

struct AttachmentDisplay {
    kind: &'static str,
    href: String,
    title: String,
    summary: Option<String>,
}

pub(crate) fn serve(
    project_root: Utf8PathBuf,
    bind: SocketAddr,
    limit: Option<u32>,
) -> Result<(), StoreError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()
        .map_err(StoreError::from)?;
    runtime.block_on(async move {
        let state = NavigatorState {
            project_root,
            limit,
        };
        let app = Router::new()
            .route("/", get(project_home))
            .route("/frontier/{selector}", get(frontier_detail))
            .route("/hypothesis/{selector}", get(hypothesis_detail))
            .route("/experiment/{selector}", get(experiment_detail))
            .route("/artifact/{selector}", get(artifact_detail))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .map_err(StoreError::from)?;
        println!("navigator: http://{bind}/");
        axum::serve(listener, app)
            .await
            .map_err(|error| StoreError::Io(io::Error::other(error.to_string())))
    })
}

async fn project_home(State(state): State<NavigatorState>) -> Response {
    render_response(render_project_home(state))
}

async fn frontier_detail(
    State(state): State<NavigatorState>,
    Path(selector): Path<String>,
) -> Response {
    render_response(render_frontier_detail(state, selector))
}

async fn hypothesis_detail(
    State(state): State<NavigatorState>,
    Path(selector): Path<String>,
) -> Response {
    render_response(render_hypothesis_detail(state, selector))
}

async fn experiment_detail(
    State(state): State<NavigatorState>,
    Path(selector): Path<String>,
) -> Response {
    render_response(render_experiment_detail(state, selector))
}

async fn artifact_detail(
    State(state): State<NavigatorState>,
    Path(selector): Path<String>,
) -> Response {
    render_response(render_artifact_detail(state, selector))
}

fn render_response(result: Result<Markup, StoreError>) -> Response {
    match result {
        Ok(markup) => Html(markup.into_string()).into_response(),
        Err(StoreError::UnknownFrontierSelector(_))
        | Err(StoreError::UnknownHypothesisSelector(_))
        | Err(StoreError::UnknownExperimentSelector(_))
        | Err(StoreError::UnknownArtifactSelector(_)) => {
            (StatusCode::NOT_FOUND, "not found".to_owned()).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("navigator render failed: {error}"),
        )
            .into_response(),
    }
}

fn render_project_home(state: NavigatorState) -> Result<Markup, StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let project_status = store.status()?;
    let frontiers = store.list_frontiers()?;
    let title = format!("{} navigator", project_status.display_name);
    let content = html! {
        (render_project_status(&project_status))
        (render_frontier_grid(&frontiers, state.limit))
    };
    Ok(render_shell(
        &title,
        Some(&project_status.display_name.to_string()),
        None,
        content,
    ))
}

fn render_frontier_detail(state: NavigatorState, selector: String) -> Result<Markup, StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let projection = store.frontier_open(&selector)?;
    let title = format!("{} · frontier", projection.frontier.label);
    let subtitle = format!(
        "{} hypotheses active · {} experiments open",
        projection.active_hypotheses.len(),
        projection.open_experiments.len()
    );
    let content = html! {
        (render_frontier_header(&projection.frontier))
        (render_frontier_brief(&projection))
        (render_frontier_active_sets(&projection))
        (render_hypothesis_current_state_grid(
            &projection.active_hypotheses,
            state.limit,
        ))
        (render_open_experiment_grid(
            &projection.open_experiments,
            state.limit,
        ))
    };
    Ok(render_shell(&title, Some(&subtitle), None, content))
}

fn render_hypothesis_detail(state: NavigatorState, selector: String) -> Result<Markup, StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let detail = store.read_hypothesis(&selector)?;
    let frontier = store.read_frontier(&detail.record.frontier_id.to_string())?;
    let title = format!("{} · hypothesis", detail.record.title);
    let subtitle = detail.record.summary.to_string();
    let content = html! {
        (render_hypothesis_header(&detail, &frontier))
        (render_prose_block("Body", detail.record.body.as_str()))
        (render_vertex_relation_sections(&detail.parents, &detail.children, state.limit))
        (render_artifact_section(&detail.artifacts, state.limit))
        (render_experiment_section(
            "Open Experiments",
            &detail.open_experiments,
            state.limit,
        ))
        (render_experiment_section(
            "Closed Experiments",
            &detail.closed_experiments,
            state.limit,
        ))
    };
    Ok(render_shell(
        &title,
        Some(&subtitle),
        Some((frontier.label.as_str(), frontier_href(&frontier.slug))),
        content,
    ))
}

fn render_experiment_detail(state: NavigatorState, selector: String) -> Result<Markup, StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let detail = store.read_experiment(&selector)?;
    let frontier = store.read_frontier(&detail.record.frontier_id.to_string())?;
    let title = format!("{} · experiment", detail.record.title);
    let subtitle = detail.record.summary.as_ref().map_or_else(
        || detail.record.status.as_str().to_owned(),
        ToString::to_string,
    );
    let content = html! {
        (render_experiment_header(&detail, &frontier))
        (render_vertex_relation_sections(&detail.parents, &detail.children, state.limit))
        (render_artifact_section(&detail.artifacts, state.limit))
        @if let Some(outcome) = detail.record.outcome.as_ref() {
            (render_experiment_outcome(outcome))
        } @else {
            section.card {
                h2 { "Outcome" }
                p.muted { "Open experiment. No outcome recorded yet." }
            }
        }
    };
    Ok(render_shell(
        &title,
        Some(&subtitle),
        Some((frontier.label.as_str(), frontier_href(&frontier.slug))),
        content,
    ))
}

fn render_artifact_detail(state: NavigatorState, selector: String) -> Result<Markup, StoreError> {
    let store = open_store(state.project_root.as_std_path())?;
    let detail = store.read_artifact(&selector)?;
    let attachments = detail
        .attachments
        .iter()
        .map(|target| resolve_attachment_display(&store, *target))
        .collect::<Result<Vec<_>, StoreError>>()?;
    let title = format!("{} · artifact", detail.record.label);
    let subtitle = detail.record.summary.as_ref().map_or_else(
        || detail.record.kind.as_str().to_owned(),
        ToString::to_string,
    );
    let content = html! {
        section.card {
            h2 { "Artifact" }
            div.kv-grid {
                (render_kv("Kind", detail.record.kind.as_str()))
                (render_kv("Slug", detail.record.slug.as_str()))
                (render_kv("Locator", detail.record.locator.as_str()))
                @if let Some(media_type) = detail.record.media_type.as_ref() {
                    (render_kv("Media type", media_type.as_str()))
                }
                (render_kv("Updated", &format_timestamp(detail.record.updated_at)))
            }
            @if let Some(summary) = detail.record.summary.as_ref() {
                p.prose { (summary) }
            }
            p.muted {
                "Artifact bodies are intentionally out of band. Spinner only preserves references."
            }
        }
        section.card {
            h2 { "Attachments" }
            @if attachments.is_empty() {
                p.muted { "No attachments." }
            } @else {
                div.link-list {
                    @for attachment in &attachments {
                        (render_attachment_chip(attachment))
                    }
                }
            }
        }
    };
    Ok(render_shell(&title, Some(&subtitle), None, content))
}

fn render_frontier_grid(frontiers: &[FrontierSummary], limit: Option<u32>) -> Markup {
    html! {
    section.card {
        h2 { "Frontiers" }
        @if frontiers.is_empty() {
            p.muted { "No frontiers yet." }
        } @else {
            div.card-grid {
                @for frontier in limit_items(frontiers, limit) {
                    article.mini-card {
                        div.card-header {
                            a.title-link href=(frontier_href(&frontier.slug)) { (frontier.label) }
                            span.status-chip class=(frontier_status_class(frontier.status.as_str())) {
                                (frontier.status.as_str())
                            }
                        }
                        p.prose { (frontier.objective) }
                        div.meta-row {
                            span { (format!("{} active hypotheses", frontier.active_hypothesis_count)) }
                            span { (format!("{} open experiments", frontier.open_experiment_count)) }
                        }
                        div.meta-row.muted {
                            span { "updated " (format_timestamp(frontier.updated_at)) }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_project_status(status: &ProjectStatus) -> Markup {
    html! {
    section.card {
        h1 { (status.display_name) }
        p.prose {
            "Austere experimental ledger. Frontier overview is the only sanctioned dump; everything else is deliberate traversal."
        }
        div.kv-grid {
            (render_kv("Project root", status.project_root.as_str()))
            (render_kv("Store format", &status.store_format_version.to_string()))
            (render_kv("Frontiers", &status.frontier_count.to_string()))
            (render_kv("Hypotheses", &status.hypothesis_count.to_string()))
            (render_kv("Experiments", &status.experiment_count.to_string()))
            (render_kv("Open experiments", &status.open_experiment_count.to_string()))
            (render_kv("Artifacts", &status.artifact_count.to_string()))
        }
    }
    }
}

fn render_frontier_header(frontier: &FrontierRecord) -> Markup {
    html! {
    section.card {
        h1 { (frontier.label) }
        p.prose { (frontier.objective) }
        div.meta-row {
            span { "slug " code { (frontier.slug) } }
            span.status-chip class=(frontier_status_class(frontier.status.as_str())) {
                (frontier.status.as_str())
            }
            span.muted { "updated " (format_timestamp(frontier.updated_at)) }
        }
    }
    }
}

fn render_frontier_brief(projection: &FrontierOpenProjection) -> Markup {
    let frontier = &projection.frontier;
    html! {
    section.card {
        h2 { "Frontier Brief" }
        @if let Some(situation) = frontier.brief.situation.as_ref() {
            div.block {
                h3 { "Situation" }
                p.prose { (situation) }
            }
        } @else {
            p.muted { "No situation summary recorded." }
        }
        div.split {
            div.subcard {
                h3 { "Roadmap" }
                @if frontier.brief.roadmap.is_empty() {
                    p.muted { "No roadmap ordering recorded." }
                } @else {
                    ol.roadmap-list {
                        @for item in &frontier.brief.roadmap {
                            @let title = hypothesis_title_for_roadmap_item(projection, item.hypothesis_id);
                            li {
                                a href=(hypothesis_href_from_id(item.hypothesis_id)) {
                                    (format!("{}.", item.rank)) " "
                                    (title)
                                }
                                @if let Some(summary) = item.summary.as_ref() {
                                    span.muted { " · " (summary) }
                                }
                            }
                        }
                    }
                }
            }
            div.subcard {
                h3 { "Unknowns" }
                @if frontier.brief.unknowns.is_empty() {
                    p.muted { "No explicit unknowns." }
                } @else {
                    ul.simple-list {
                        @for unknown in &frontier.brief.unknowns {
                            li { (unknown) }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_frontier_active_sets(projection: &FrontierOpenProjection) -> Markup {
    html! {
    section.card {
        h2 { "Active Surface" }
        div.split {
            div.subcard {
                h3 { "Active Tags" }
                @if projection.active_tags.is_empty() {
                    p.muted { "No active tags." }
                } @else {
                    div.chip-row {
                        @for tag in &projection.active_tags {
                            span.tag-chip { (tag) }
                        }
                    }
                }
            }
            div.subcard {
                h3 { "Live Metrics" }
                @if projection.active_metric_keys.is_empty() {
                    p.muted { "No live metrics." }
                } @else {
                    table.metric-table {
                        thead {
                            tr {
                                th { "Key" }
                                th { "Unit" }
                                th { "Objective" }
                                th { "Refs" }
                            }
                        }
                        tbody {
                            @for metric in &projection.active_metric_keys {
                                tr {
                                    td { (metric.key) }
                                    td { (metric.unit.as_str()) }
                                    td { (metric.objective.as_str()) }
                                    td { (metric.reference_count) }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_hypothesis_current_state_grid(
    states: &[HypothesisCurrentState],
    limit: Option<u32>,
) -> Markup {
    html! {
    section.card {
        h2 { "Active Hypotheses" }
        @if states.is_empty() {
            p.muted { "No active hypotheses." }
        } @else {
            div.card-grid {
                @for state in limit_items(states, limit) {
                    article.mini-card {
                        div.card-header {
                            a.title-link href=(hypothesis_href(&state.hypothesis.slug)) {
                                (state.hypothesis.title)
                            }
                            @if let Some(verdict) = state.hypothesis.latest_verdict {
                                span.status-chip class=(verdict_class(verdict)) {
                                    (verdict.as_str())
                                }
                            }
                        }
                        p.prose { (state.hypothesis.summary) }
                        @if !state.hypothesis.tags.is_empty() {
                            div.chip-row {
                                @for tag in &state.hypothesis.tags {
                                    span.tag-chip { (tag) }
                                }
                            }
                        }
                        div.meta-row {
                            span { (format!("{} open", state.open_experiments.len())) }
                            @if let Some(latest) = state.latest_closed_experiment.as_ref() {
                                span {
                                    "latest "
                                    a href=(experiment_href(&latest.slug)) { (latest.title) }
                                }
                            } @else {
                                span.muted { "no closed experiments" }
                            }
                        }
                        @if !state.open_experiments.is_empty() {
                            div.related-block {
                                h3 { "Open" }
                                div.link-list {
                                    @for experiment in &state.open_experiments {
                                        (render_experiment_link_chip(experiment))
                                    }
                                }
                            }
                        }
                        @if let Some(latest) = state.latest_closed_experiment.as_ref() {
                            div.related-block {
                                h3 { "Latest Closed" }
                                (render_experiment_summary_line(latest))
                            }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_open_experiment_grid(experiments: &[ExperimentSummary], limit: Option<u32>) -> Markup {
    html! {
    section.card {
        h2 { "Open Experiments" }
        @if experiments.is_empty() {
            p.muted { "No open experiments." }
        } @else {
            div.card-grid {
                @for experiment in limit_items(experiments, limit) {
                    (render_experiment_card(experiment))
                }
            }
        }
    }
    }
}

fn render_hypothesis_header(detail: &HypothesisDetail, frontier: &FrontierRecord) -> Markup {
    html! {
    section.card {
        h1 { (detail.record.title) }
        p.prose { (detail.record.summary) }
        div.meta-row {
            span { "frontier " a href=(frontier_href(&frontier.slug)) { (frontier.label) } }
            span { "slug " code { (detail.record.slug) } }
            @if detail.record.archived {
                span.status-chip.archived { "archived" }
            }
            span.muted { "updated " (format_timestamp(detail.record.updated_at)) }
        }
        @if !detail.record.tags.is_empty() {
            div.chip-row {
                @for tag in &detail.record.tags {
                    span.tag-chip { (tag) }
                }
            }
        }
    }
    }
}

fn render_experiment_header(detail: &ExperimentDetail, frontier: &FrontierRecord) -> Markup {
    html! {
    section.card {
        h1 { (detail.record.title) }
        @if let Some(summary) = detail.record.summary.as_ref() {
            p.prose { (summary) }
        }
        div.meta-row {
            span {
                "frontier "
                a href=(frontier_href(&frontier.slug)) { (frontier.label) }
            }
            span {
                "hypothesis "
                a href=(hypothesis_href(&detail.owning_hypothesis.slug)) {
                    (detail.owning_hypothesis.title)
                }
            }
            span.status-chip class=(experiment_status_class(detail.record.status)) {
                (detail.record.status.as_str())
            }
            @if let Some(verdict) = detail
                .record
                .outcome
                .as_ref()
                .map(|outcome| outcome.verdict)
            {
                span.status-chip class=(verdict_class(verdict)) { (verdict.as_str()) }
            }
            span.muted { "updated " (format_timestamp(detail.record.updated_at)) }
        }
        @if !detail.record.tags.is_empty() {
            div.chip-row {
                @for tag in &detail.record.tags {
                    span.tag-chip { (tag) }
                }
            }
        }
    }
    }
}

fn render_experiment_outcome(outcome: &ExperimentOutcome) -> Markup {
    html! {
    section.card {
        h2 { "Outcome" }
        div.kv-grid {
            (render_kv("Verdict", outcome.verdict.as_str()))
            (render_kv("Backend", outcome.backend.as_str()))
            (render_kv("Closed", &format_timestamp(outcome.closed_at)))
        }
        (render_command_recipe(&outcome.command))
        (render_metric_panel("Primary metric", std::slice::from_ref(&outcome.primary_metric), outcome))
        @if !outcome.supporting_metrics.is_empty() {
            (render_metric_panel("Supporting metrics", &outcome.supporting_metrics, outcome))
        }
        @if !outcome.dimensions.is_empty() {
            section.subcard {
                h3 { "Dimensions" }
                table.metric-table {
                    thead { tr { th { "Key" } th { "Value" } } }
                    tbody {
                        @for (key, value) in &outcome.dimensions {
                            tr {
                                td { (key) }
                                td { (render_dimension_value(value)) }
                            }
                        }
                    }
                }
            }
        }
        section.subcard {
            h3 { "Rationale" }
            p.prose { (outcome.rationale) }
        }
        @if let Some(analysis) = outcome.analysis.as_ref() {
            (render_experiment_analysis(analysis))
        }
    }
    }
}

fn render_experiment_analysis(analysis: &ExperimentAnalysis) -> Markup {
    html! {
    section.subcard {
        h3 { "Analysis" }
        p.prose { (analysis.summary) }
        div.code-block {
            (analysis.body)
        }
    }
    }
}

fn render_command_recipe(command: &fidget_spinner_core::CommandRecipe) -> Markup {
    html! {
    section.subcard {
        h3 { "Command" }
        div.kv-grid {
            (render_kv(
                "argv",
                &command
                    .argv
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" "),
            ))
            @if let Some(working_directory) = command.working_directory.as_ref() {
                (render_kv("cwd", working_directory.as_str()))
            }
        }
        @if !command.env.is_empty() {
            table.metric-table {
                thead { tr { th { "Env" } th { "Value" } } }
                tbody {
                    @for (key, value) in &command.env {
                        tr {
                            td { (key) }
                            td { (value) }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_metric_panel(
    title: &str,
    metrics: &[fidget_spinner_core::MetricValue],
    outcome: &ExperimentOutcome,
) -> Markup {
    html! {
    section.subcard {
        h3 { (title) }
        table.metric-table {
            thead {
                tr {
                    th { "Key" }
                    th { "Value" }
                }
            }
            tbody {
                @for metric in metrics {
                    tr {
                        td { (metric.key) }
                        td { (format_metric_value(metric.value, metric_unit_for(metric, outcome))) }
                    }
                }
            }
        }
    }
    }
}

fn metric_unit_for(
    metric: &fidget_spinner_core::MetricValue,
    outcome: &ExperimentOutcome,
) -> MetricUnit {
    if metric.key == outcome.primary_metric.key {
        return MetricUnit::Custom;
    }
    MetricUnit::Custom
}

fn render_vertex_relation_sections(
    parents: &[VertexSummary],
    children: &[VertexSummary],
    limit: Option<u32>,
) -> Markup {
    html! {
        section.card {
            h2 { "Influence Network" }
            div.split {
                div.subcard {
                    h3 { "Parents" }
                    @if parents.is_empty() {
                        p.muted { "No parent influences." }
                    } @else {
                        div.link-list {
                            @for parent in limit_items(parents, limit) {
                                (render_vertex_chip(parent))
                            }
                        }
                    }
                }
                div.subcard {
                    h3 { "Children" }
                    @if children.is_empty() {
                        p.muted { "No downstream influences." }
                    } @else {
                        div.link-list {
                            @for child in limit_items(children, limit) {
                                (render_vertex_chip(child))
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_artifact_section(
    artifacts: &[fidget_spinner_store_sqlite::ArtifactSummary],
    limit: Option<u32>,
) -> Markup {
    html! {
    section.card {
        h2 { "Artifacts" }
        @if artifacts.is_empty() {
            p.muted { "No attached artifacts." }
        } @else {
            div.card-grid {
                @for artifact in limit_items(artifacts, limit) {
                    article.mini-card {
                        div.card-header {
                            a.title-link href=(artifact_href(&artifact.slug)) { (artifact.label) }
                            span.status-chip.classless { (artifact.kind.as_str()) }
                        }
                        @if let Some(summary) = artifact.summary.as_ref() {
                            p.prose { (summary) }
                        }
                        div.meta-row {
                            span.muted { (artifact.locator) }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_experiment_section(
    title: &str,
    experiments: &[ExperimentSummary],
    limit: Option<u32>,
) -> Markup {
    html! {
    section.card {
        h2 { (title) }
        @if experiments.is_empty() {
            p.muted { "None." }
        } @else {
            div.card-grid {
                @for experiment in limit_items(experiments, limit) {
                    (render_experiment_card(experiment))
                }
            }
        }
    }
    }
}

fn render_experiment_card(experiment: &ExperimentSummary) -> Markup {
    html! {
    article.mini-card {
        div.card-header {
            a.title-link href=(experiment_href(&experiment.slug)) { (experiment.title) }
            span.status-chip class=(experiment_status_class(experiment.status)) {
                (experiment.status.as_str())
            }
            @if let Some(verdict) = experiment.verdict {
                span.status-chip class=(verdict_class(verdict)) { (verdict.as_str()) }
            }
        }
        @if let Some(summary) = experiment.summary.as_ref() {
            p.prose { (summary) }
        }
        @if let Some(metric) = experiment.primary_metric.as_ref() {
            div.meta-row {
                span.metric-pill {
                    (metric.key) ": "
                    (format_metric_value(metric.value, metric.unit))
                }
            }
        }
        @if !experiment.tags.is_empty() {
            div.chip-row {
                @for tag in &experiment.tags {
                    span.tag-chip { (tag) }
                }
            }
        }
        div.meta-row.muted {
            span { "updated " (format_timestamp(experiment.updated_at)) }
        }
    }
    }
}

fn render_experiment_summary_line(experiment: &ExperimentSummary) -> Markup {
    html! {
    div.link-list {
        (render_experiment_link_chip(experiment))
        @if let Some(metric) = experiment.primary_metric.as_ref() {
            span.metric-pill {
                (metric.key) ": "
                (format_metric_value(metric.value, metric.unit))
            }
        }
    }
    }
}

fn render_experiment_link_chip(experiment: &ExperimentSummary) -> Markup {
    html! {
        a.link-chip href=(experiment_href(&experiment.slug)) {
            span { (experiment.title) }
            @if let Some(verdict) = experiment.verdict {
                span.status-chip class=(verdict_class(verdict)) { (verdict.as_str()) }
            }
        }
    }
}

fn render_vertex_chip(summary: &VertexSummary) -> Markup {
    let href = match summary.vertex {
        VertexRef::Hypothesis(_) => hypothesis_href(&summary.slug),
        VertexRef::Experiment(_) => experiment_href(&summary.slug),
    };
    let kind = match summary.vertex {
        VertexRef::Hypothesis(_) => "hypothesis",
        VertexRef::Experiment(_) => "experiment",
    };
    html! {
        a.link-chip href=(href) {
            span.kind-chip { (kind) }
            span { (summary.title) }
            @if let Some(summary_text) = summary.summary.as_ref() {
                span.muted { " — " (summary_text) }
            }
        }
    }
}

fn render_attachment_chip(attachment: &AttachmentDisplay) -> Markup {
    html! {
        a.link-chip href=(&attachment.href) {
            span.kind-chip { (attachment.kind) }
            span { (&attachment.title) }
            @if let Some(summary) = attachment.summary.as_ref() {
                span.muted { " — " (summary) }
            }
        }
    }
}

fn render_prose_block(title: &str, body: &str) -> Markup {
    html! {
    section.card {
        h2 { (title) }
        p.prose { (body) }
    }
    }
}

fn render_shell(
    title: &str,
    subtitle: Option<&str>,
    breadcrumb: Option<(&str, String)>,
    content: Markup,
) -> Markup {
    html! {
        (DOCTYPE)
        html {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) }
                style { (PreEscaped(styles())) }
            }
            body {
                main.shell {
                    header.page-header {
                        div.eyebrow {
                            a href="/" { "home" }
                            @if let Some((label, href)) = breadcrumb {
                                span.sep { "/" }
                                a href=(href) { (label) }
                            }
                        }
                        h1.page-title { (title) }
                        @if let Some(subtitle) = subtitle {
                            p.page-subtitle { (subtitle) }
                        }
                    }
                    (content)
                }
            }
        }
    }
}

fn render_kv(label: &str, value: &str) -> Markup {
    html! {
        div.kv {
            div.kv-label { (label) }
            div.kv-value { (value) }
        }
    }
}

fn render_dimension_value(value: &RunDimensionValue) -> String {
    match value {
        RunDimensionValue::String(value) => value.to_string(),
        RunDimensionValue::Numeric(value) => format_float(*value),
        RunDimensionValue::Boolean(value) => value.to_string(),
        RunDimensionValue::Timestamp(value) => value.to_string(),
    }
}

fn format_metric_value(value: f64, unit: MetricUnit) -> String {
    match unit {
        MetricUnit::Bytes => format!("{} B", format_integerish(value)),
        MetricUnit::Seconds => format!("{value:.3} s"),
        MetricUnit::Count => format_integerish(value),
        MetricUnit::Ratio => format!("{value:.4}"),
        MetricUnit::Custom => format_float(value),
    }
}

fn format_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format_integerish(value)
    } else {
        format!("{value:.4}")
    }
}

fn format_integerish(value: f64) -> String {
    let negative = value.is_sign_negative();
    let digits = format!("{:.0}", value.abs());
    let mut grouped = String::with_capacity(digits.len() + (digits.len() / 3));
    for (index, ch) in digits.chars().rev().enumerate() {
        if index != 0 && index % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    let grouped: String = grouped.chars().rev().collect();
    if negative {
        format!("-{grouped}")
    } else {
        grouped
    }
}

fn format_timestamp(value: OffsetDateTime) -> String {
    const TIMESTAMP: &[time::format_description::FormatItem<'static>] =
        format_description!("[year]-[month]-[day] [hour]:[minute]");
    value.format(TIMESTAMP).unwrap_or_else(|_| {
        value
            .format(&Rfc3339)
            .unwrap_or_else(|_| value.unix_timestamp().to_string())
    })
}

fn frontier_href(slug: &Slug) -> String {
    format!("/frontier/{}", encode_path_segment(slug.as_str()))
}

fn hypothesis_href(slug: &Slug) -> String {
    format!("/hypothesis/{}", encode_path_segment(slug.as_str()))
}

fn hypothesis_href_from_id(id: fidget_spinner_core::HypothesisId) -> String {
    format!("/hypothesis/{}", encode_path_segment(&id.to_string()))
}

fn hypothesis_title_for_roadmap_item(
    projection: &FrontierOpenProjection,
    hypothesis_id: fidget_spinner_core::HypothesisId,
) -> String {
    projection
        .active_hypotheses
        .iter()
        .find(|state| state.hypothesis.id == hypothesis_id)
        .map(|state| state.hypothesis.title.to_string())
        .unwrap_or_else(|| hypothesis_id.to_string())
}

fn experiment_href(slug: &Slug) -> String {
    format!("/experiment/{}", encode_path_segment(slug.as_str()))
}

fn artifact_href(slug: &Slug) -> String {
    format!("/artifact/{}", encode_path_segment(slug.as_str()))
}

fn resolve_attachment_display(
    store: &fidget_spinner_store_sqlite::ProjectStore,
    target: AttachmentTargetRef,
) -> Result<AttachmentDisplay, StoreError> {
    match target {
        AttachmentTargetRef::Frontier(id) => {
            let frontier = store.read_frontier(&id.to_string())?;
            Ok(AttachmentDisplay {
                kind: "frontier",
                href: frontier_href(&frontier.slug),
                title: frontier.label.to_string(),
                summary: Some(frontier.objective.to_string()),
            })
        }
        AttachmentTargetRef::Hypothesis(id) => {
            let detail = store.read_hypothesis(&id.to_string())?;
            Ok(AttachmentDisplay {
                kind: "hypothesis",
                href: hypothesis_href(&detail.record.slug),
                title: detail.record.title.to_string(),
                summary: Some(detail.record.summary.to_string()),
            })
        }
        AttachmentTargetRef::Experiment(id) => {
            let detail = store.read_experiment(&id.to_string())?;
            Ok(AttachmentDisplay {
                kind: "experiment",
                href: experiment_href(&detail.record.slug),
                title: detail.record.title.to_string(),
                summary: detail.record.summary.as_ref().map(ToString::to_string),
            })
        }
    }
}

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn frontier_status_class(status: &str) -> &'static str {
    match status {
        "exploring" => "status-exploring",
        "paused" => "status-parked",
        "archived" => "status-archived",
        _ => "status-neutral",
    }
}

fn experiment_status_class(status: ExperimentStatus) -> &'static str {
    match status {
        ExperimentStatus::Open => "status-open",
        ExperimentStatus::Closed => "status-neutral",
    }
}

fn verdict_class(verdict: FrontierVerdict) -> &'static str {
    match verdict {
        FrontierVerdict::Accepted => "status-accepted",
        FrontierVerdict::Kept => "status-kept",
        FrontierVerdict::Parked => "status-parked",
        FrontierVerdict::Rejected => "status-rejected",
    }
}

fn limit_items<T>(items: &[T], limit: Option<u32>) -> &[T] {
    let Some(limit) = limit else {
        return items;
    };
    let Ok(limit) = usize::try_from(limit) else {
        return items;
    };
    let end = items.len().min(limit);
    &items[..end]
}

fn styles() -> &'static str {
    r#"
    :root {
        color-scheme: dark;
        --bg: #091019;
        --panel: #0f1823;
        --panel-2: #131f2d;
        --border: #1e3850;
        --text: #d8e6f3;
        --muted: #87a0b8;
        --accent: #6dc7ff;
        --accepted: #7ce38b;
        --kept: #8de0c0;
        --parked: #d9c17d;
        --rejected: #ee7a7a;
    }
    * { box-sizing: border-box; }
    body {
        margin: 0;
        background: var(--bg);
        color: var(--text);
        font: 15px/1.5 "Iosevka Web", "Iosevka", "JetBrains Mono", monospace;
    }
    a {
        color: var(--accent);
        text-decoration: none;
    }
    a:hover { text-decoration: underline; }
    .shell {
        width: min(1500px, 100%);
        margin: 0 auto;
        padding: 20px;
        display: grid;
        gap: 16px;
    }
    .page-header {
        display: grid;
        gap: 8px;
        padding: 16px 18px;
        border: 1px solid var(--border);
        background: var(--panel);
    }
    .eyebrow {
        display: flex;
        gap: 10px;
        color: var(--muted);
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .sep { color: #4d6478; }
    .page-title {
        margin: 0;
        font-size: clamp(22px, 3.8vw, 34px);
        line-height: 1.1;
    }
    .page-subtitle {
        margin: 0;
        color: var(--muted);
        max-width: 90ch;
    }
    .card {
        border: 1px solid var(--border);
        background: var(--panel);
        padding: 16px 18px;
        display: grid;
        gap: 12px;
    }
    .subcard {
        border: 1px solid #1a2b3c;
        background: var(--panel-2);
        padding: 12px 14px;
        display: grid;
        gap: 10px;
        min-width: 0;
    }
    .block { display: grid; gap: 10px; }
    .split {
        display: grid;
        gap: 16px;
        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    }
    .card-grid {
        display: grid;
        gap: 12px;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }
    .mini-card {
        border: 1px solid #1a2b3c;
        background: var(--panel-2);
        padding: 12px 14px;
        display: grid;
        gap: 9px;
        min-width: 0;
    }
    .card-header {
        display: flex;
        gap: 10px;
        align-items: center;
        flex-wrap: wrap;
    }
    .title-link {
        font-size: 16px;
        font-weight: 700;
        color: #f2f8ff;
    }
    h1, h2, h3 {
        margin: 0;
        line-height: 1.15;
    }
    h2 { font-size: 19px; }
    h3 { font-size: 14px; color: #c9d8e6; }
    .prose {
        margin: 0;
        color: #dce9f6;
        max-width: 92ch;
        white-space: pre-wrap;
    }
    .muted { color: var(--muted); }
    .meta-row {
        display: flex;
        flex-wrap: wrap;
        gap: 14px;
        align-items: center;
        font-size: 13px;
    }
    .kv-grid {
        display: grid;
        gap: 10px 14px;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }
    .kv {
        display: grid;
        gap: 4px;
        min-width: 0;
    }
    .kv-label {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .kv-value {
        overflow-wrap: anywhere;
    }
    .chip-row, .link-list {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
    }
    .tag-chip, .kind-chip, .status-chip, .metric-pill, .link-chip {
        border: 1px solid #24425b;
        background: rgba(109, 199, 255, 0.06);
        padding: 4px 8px;
        font-size: 12px;
        line-height: 1.2;
    }
    .link-chip {
        display: inline-flex;
        gap: 8px;
        align-items: center;
    }
    .kind-chip {
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .status-chip {
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 700;
    }
    .status-accepted { color: var(--accepted); border-color: rgba(124, 227, 139, 0.35); }
    .status-kept { color: var(--kept); border-color: rgba(141, 224, 192, 0.35); }
    .status-parked { color: var(--parked); border-color: rgba(217, 193, 125, 0.35); }
    .status-rejected { color: var(--rejected); border-color: rgba(238, 122, 122, 0.35); }
    .status-open { color: var(--accent); border-color: rgba(109, 199, 255, 0.35); }
    .status-exploring { color: var(--accent); border-color: rgba(109, 199, 255, 0.35); }
    .status-neutral, .classless { color: #a7c0d4; border-color: #2a4358; }
    .status-archived { color: #7f8da0; border-color: #2b3540; }
    .metric-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
    }
    .metric-table th,
    .metric-table td {
        padding: 7px 8px;
        border-top: 1px solid #1b2d3e;
        text-align: left;
        vertical-align: top;
    }
    .metric-table th {
        color: var(--muted);
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-size: 12px;
    }
    .related-block {
        display: grid;
        gap: 8px;
    }
    .roadmap-list, .simple-list {
        margin: 0;
        padding-left: 18px;
        display: grid;
        gap: 6px;
    }
    .code-block {
        white-space: pre-wrap;
        overflow-wrap: anywhere;
        border: 1px solid #1a2b3c;
        background: #0b131c;
        padding: 12px 14px;
    }
    code {
        font-family: inherit;
        font-size: 0.95em;
    }
    @media (max-width: 720px) {
        .shell { padding: 12px; }
        .card, .page-header { padding: 14px; }
        .subcard, .mini-card { padding: 12px; }
        .card-grid, .split, .kv-grid { grid-template-columns: 1fr; }
    }
    "#
}
