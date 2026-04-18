use super::assets::{interaction_script, styles};
use super::results::{
    load_other_metric_keys, render_frontier_tab_bar, render_frontier_tab_content,
    requested_or_kpi_metric_keys, resolve_selected_metric_keys, visible_metric_catalog,
};
use super::{
    BTreeMap, DOCTYPE, ExperimentAnalysis, ExperimentDetail, ExperimentOutcome, ExperimentStatus,
    ExperimentSummary, FrontierOpenProjection, FrontierPageQuery, FrontierRecord, FrontierTab,
    HypothesisDetail, Markup, MetricKeysQuery, MetricScope, NonEmptyText, PreEscaped,
    ProjectRenderContext, RunDimensionValue, ShellFrame, StoreError, VertexRef, VertexSummary,
    experiment_href, experiment_status_class, format_metric_value, format_timestamp, frontier_href,
    frontier_status_class, frontier_tab_href, html, hypothesis_href, hypothesis_href_from_id,
    hypothesis_title_for_roadmap_item, limit_items, load_shell_frame, open_store, pencil_icon,
    render_dimension_value, render_fact, render_kv, render_sidebar, short_commit_hash,
    status_chip_classes, verdict_class,
};

pub(super) fn render_frontier_detail(
    context: ProjectRenderContext,
    selector: String,
    query: FrontierPageQuery,
) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let projection = store.frontier_open(&selector)?;
    let shell = load_shell_frame(&store, Some(projection.frontier.slug.clone()), &context)?;
    let kpi_metric_keys_for_tab_bar = store.metric_keys(MetricKeysQuery {
        frontier: Some(projection.frontier.slug.to_string()),
        scope: MetricScope::Kpi,
    })?;
    let other_metric_keys_for_tab_bar = load_other_metric_keys(&store, &projection)?;
    let requested_metrics_for_tab_bar =
        requested_or_kpi_metric_keys(&query.metric, &kpi_metric_keys_for_tab_bar);
    let tab = FrontierTab::from_query(query.tab.as_deref());
    let title = format!("{} · frontier", projection.frontier.label);
    let content = render_frontier_tab_content(&store, &projection, tab, &query, context.limit)?;
    Ok(render_shell(
        &title,
        &shell,
        Some(render_frontier_tab_bar(
            &projection.frontier.slug,
            tab,
            &resolve_selected_metric_keys(
                &requested_metrics_for_tab_bar,
                &visible_metric_catalog(
                    &kpi_metric_keys_for_tab_bar,
                    &other_metric_keys_for_tab_bar,
                ),
            ),
            query.log_y_requested(),
            &query.condition_filters(),
            query.table_metric.as_deref(),
        )),
        content,
    ))
}

pub(super) fn render_hypothesis_detail(
    context: ProjectRenderContext,
    selector: String,
) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let detail = store.read_hypothesis(&selector)?;
    let frontier = store.read_frontier(&detail.record.frontier_id.to_string())?;
    let shell = load_shell_frame(&store, Some(frontier.slug.clone()), &context)?;
    let title = format!("{} · hypothesis", detail.record.title);
    let content = html! {
        (render_hypothesis_header(&detail, &frontier))
        (render_prose_block("Body", detail.record.body.as_str()))
        (render_vertex_relation_sections(&detail.parents, &detail.children, context.limit))
        (render_experiment_section(
            "Open Experiments",
            &detail.open_experiments,
            context.limit,
        ))
        (render_experiment_section(
            "Closed Experiments",
            &detail.closed_experiments,
            context.limit,
        ))
    };
    Ok(render_shell(&title, &shell, None, content))
}

pub(super) fn render_experiment_detail(
    context: ProjectRenderContext,
    selector: String,
) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let detail = store.read_experiment(&selector)?;
    let frontier = store.read_frontier(&detail.record.frontier_id.to_string())?;
    let shell = load_shell_frame(&store, Some(frontier.slug.clone()), &context)?;
    let title = format!("{} · experiment", detail.record.title);
    let content = html! {
        (render_experiment_header(&detail, &frontier))
        @if let Some(outcome) = detail.record.outcome.as_ref() {
            (render_experiment_outcome(outcome))
        } @else {
            (render_open_experiment_outcome())
        }
        (render_vertex_relation_sections(&detail.parents, &detail.children, context.limit))
    };
    Ok(render_shell(&title, &shell, None, content))
}

pub(super) fn render_frontier_header(frontier: &FrontierRecord) -> Markup {
    html! {
    section.card.frontier-heading {
        div.frontier-title-row {
            h1 { (frontier.label) }
            details.control-popout.frontier-summary-editor {
                summary.inline-icon-button.frontier-edit-toggle aria-label="Edit frontier title and description" title="Edit title and description" {
                    (pencil_icon())
                }
                div.control-popout-panel.frontier-summary-panel {
                    form.frontier-summary-form method="post" action=(format!("{}/summary", frontier_href(&frontier.slug))) data-preserve-viewport="true" {
                        input type="hidden" name="expected_revision" value=(frontier.revision);
                        label.filter-control {
                            span.filter-label { "Title" }
                            input.compact-input.frontier-title-input
                                type="text"
                                name="label"
                                value=(frontier.label.as_str())
                                required;
                        }
                        label.filter-control {
                            span.filter-label { "Description" }
                            textarea.compact-textarea.frontier-description-input
                                name="objective"
                                rows="4"
                                required
                            { (frontier.objective.as_str()) }
                        }
                        div.filter-actions {
                            button.form-button type="submit" { "Save" }
                        }
                    }
                }
            }
        }
        div.meta-row {
            span { "slug " code { (frontier.slug) } }
            span class=(status_chip_classes(frontier_status_class(frontier.status.as_str()))) {
                (frontier.status.as_str())
            }
            span.muted { "updated " (format_timestamp(frontier.updated_at)) }
        }
    }
    }
}

pub(super) fn render_frontier_brief(projection: &FrontierOpenProjection) -> Markup {
    let frontier = &projection.frontier;
    html! {
    section.card {
        h2 { "Brief" }
        div.block {
            h3 { "Description" }
            p.prose { (frontier.objective) }
        }
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

pub(super) fn render_frontier_active_sets(projection: &FrontierOpenProjection) -> Markup {
    html! {
    section.card {
        h2 { "Active Surface" }
        div.stack {
            div.subcard.compact-subcard {
                h3 { "Active Tags" }
                @if projection.active_tags.is_empty() {
                    p.muted { "No active tags." }
                } @else {
                    div.chip-row.tag-cloud {
                        @for tag in &projection.active_tags {
                            span.tag-chip { (tag) }
                        }
                    }
                }
            }
            div.subcard {
                h3 { "KPI Metrics" }
                @if projection.kpis.is_empty() {
                    p.muted { "No frontier KPI metrics configured." }
                } @else {
                    div.table-scroll {
                        table.metric-table {
                            thead {
                                tr {
                                    th { "Metric" }
                                    th { "Unit" }
                                    th { "Objective" }
                                }
                            }
                            tbody {
                                @for kpi in &projection.kpis {
                                    tr {
                                        td { (kpi.metric.key) }
                                        td { (kpi.metric.display_unit.as_str()) }
                                        td { (kpi.metric.objective.as_str()) }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div.subcard {
                h3 { "Live Metrics" }
                @if projection.active_metric_keys.is_empty() {
                    p.muted { "No live metrics." }
                } @else {
                    div.table-scroll {
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
                                        td {
                                            a href=(frontier_tab_href(
                                                &projection.frontier.slug,
                                                FrontierTab::Results,
                                                std::slice::from_ref(metric),
                                                false,
                                                Some(metric.key.as_str()),
                                            )) {
                                                (metric.key)
                                            }
                                        }
                                        td { (metric.display_unit.as_str()) }
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
}

fn render_hypothesis_header(detail: &HypothesisDetail, frontier: &FrontierRecord) -> Markup {
    html! {
    section.card {
        h1 { (detail.record.title) }
        p.prose { (detail.record.summary) }
        div.meta-row {
            span { "frontier " a href=(frontier_href(&frontier.slug)) { (frontier.label) } }
            span { "slug " code { (detail.record.slug) } }
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
            span class=(status_chip_classes(experiment_status_class(detail.record.status))) {
                (detail.record.status.as_str())
            }
            @if let Some(verdict) = detail
                .record
                .outcome
                .as_ref()
                .map(|outcome| outcome.verdict)
            {
                span class=(status_chip_classes(verdict_class(verdict))) { (verdict.as_str()) }
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
    section.card.experiment-outcome {
        div.card-header.outcome-header {
            h2 { "Outcome" }
            div.fact-strip.outcome-verdict-strip {
                span.fact {
                    span.fact-label { "verdict" }
                    span class=(status_chip_classes(verdict_class(outcome.verdict))) {
                        (outcome.verdict.as_str())
                    }
                }
            }
        }
        section.subcard.narrative-block {
            h3 { "Rationale" }
            p.prose { (outcome.rationale) }
        }
        @if let Some(analysis) = outcome.analysis.as_ref() {
            (render_experiment_analysis(analysis))
        }
        (render_metric_panel("Primary metric", std::slice::from_ref(&outcome.primary_metric), outcome))
        @if !outcome.supporting_metrics.is_empty() {
            (render_metric_panel("Supporting metrics", &outcome.supporting_metrics, outcome))
        }
        (render_experiment_provenance(outcome))
    }
    }
}

fn render_open_experiment_outcome() -> Markup {
    html! {
    section.card.experiment-outcome {
        div.card-header.outcome-header {
            h2 { "Outcome" }
            div.fact-strip.outcome-verdict-strip {
                span.fact {
                    span.fact-label { "state" }
                    span class=(status_chip_classes(experiment_status_class(ExperimentStatus::Open))) {
                        "open"
                    }
                }
            }
        }
        p.muted { "No outcome recorded yet." }
    }
    }
}

fn render_experiment_analysis(analysis: &ExperimentAnalysis) -> Markup {
    html! {
    section.subcard.narrative-block {
        h3 { "Analysis" }
        p.prose { (analysis.summary) }
        div.code-block {
            (analysis.body)
        }
    }
    }
}

fn render_experiment_provenance(outcome: &ExperimentOutcome) -> Markup {
    html! {
    details.subcard.provenance-disclosure {
        summary.provenance-summary {
            span { "Provenance" }
            span.provenance-summary-facts {
                span { (outcome.backend.as_str()) }
                @if let Some(commit_hash) = outcome.commit_hash.as_ref() {
                    span { (short_commit_hash(commit_hash.as_str())) }
                }
                span { (format_timestamp(outcome.closed_at)) }
            }
        }
        div.provenance-body {
            div.fact-strip {
                (render_fact("backend", outcome.backend.as_str()))
                @if let Some(commit_hash) = outcome.commit_hash.as_ref() {
                    (render_fact("commit", commit_hash.as_str()))
                }
                (render_fact("closed", &format_timestamp(outcome.closed_at)))
            }
            (render_command_recipe(&outcome.command))
            @if !outcome.dimensions.is_empty() {
                (render_dimension_ledger("Conditions", &outcome.dimensions))
            }
        }
    }
    }
}

fn render_command_recipe(command: &fidget_spinner_core::CommandRecipe) -> Markup {
    html! {
    div.provenance-block {
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
            div.table-scroll {
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
}

fn render_dimension_ledger(
    title: &str,
    dimensions: &BTreeMap<NonEmptyText, RunDimensionValue>,
) -> Markup {
    html! {
    div.provenance-block {
        h3 { (title) }
        div.table-scroll {
            table.metric-table {
                thead { tr { th { "Key" } th { "Value" } } }
                tbody {
                    @for (key, value) in dimensions {
                        tr {
                            td { (key) }
                            td { (render_dimension_value(value)) }
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
    _outcome: &ExperimentOutcome,
) -> Markup {
    html! {
    section.subcard {
        h3 { (title) }
        div.table-scroll {
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
                            td { (format_metric_value(metric.value, &metric.unit)) }
                        }
                    }
                }
            }
        }
    }
    }
}

fn render_vertex_relation_sections(
    parents: &[VertexSummary],
    children: &[VertexSummary],
    limit: Option<u32>,
) -> Markup {
    if parents.is_empty() && children.is_empty() {
        return html! {};
    }
    html! {
        section.card {
            h2 { "Influence Network" }
            div.split {
                @if !parents.is_empty() {
                    div.subcard {
                        h3 { "Parents" }
                        div.link-list {
                            @for parent in limit_items(parents, limit) {
                                (render_vertex_chip(parent))
                            }
                        }
                    }
                }
                @if !children.is_empty() {
                    div.subcard {
                        h3 { "Children" }
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

pub(super) fn render_experiment_section(
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

pub(super) fn render_experiment_card(experiment: &ExperimentSummary) -> Markup {
    html! {
    article.mini-card {
        div.card-header {
            a.title-link href=(experiment_href(&experiment.slug)) { (experiment.title) }
            span class=(status_chip_classes(experiment_status_class(experiment.status))) {
                (experiment.status.as_str())
            }
            @if let Some(verdict) = experiment.verdict {
                span class=(status_chip_classes(verdict_class(verdict))) { (verdict.as_str()) }
            }
        }
        @if let Some(summary) = experiment.summary.as_ref() {
            p.prose { (summary) }
        }
        @if let Some(metric) = experiment.primary_metric.as_ref() {
            div.meta-row {
                span.metric-pill {
                    (metric.key) ": "
                    (format_metric_value(metric.value, &metric.display_unit))
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

pub(super) fn render_experiment_summary_line(experiment: &ExperimentSummary) -> Markup {
    html! {
    div.link-list {
        (render_experiment_link_chip(experiment))
        @if let Some(metric) = experiment.primary_metric.as_ref() {
            span.metric-pill {
                (metric.key) ": "
                (format_metric_value(metric.value, &metric.display_unit))
            }
        }
    }
    }
}

pub(super) fn render_experiment_link_chip(experiment: &ExperimentSummary) -> Markup {
    html! {
        a.link-chip href=(experiment_href(&experiment.slug)) {
            span.link-chip-main {
                span.link-chip-title { (experiment.title) }
                @if let Some(verdict) = experiment.verdict {
                    span class=(status_chip_classes(verdict_class(verdict))) { (verdict.as_str()) }
                }
            }
            @if experiment.verdict.is_none() && experiment.status == ExperimentStatus::Open {
                span.link-chip-summary { "open experiment" }
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
            span.link-chip-main {
                span.kind-chip { (kind) }
                span.link-chip-title { (summary.title) }
            }
            @if let Some(summary_text) = summary.summary.as_ref() {
                span.link-chip-summary { (summary_text) }
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

pub(super) fn render_shell(
    title: &str,
    shell: &ShellFrame,
    tab_bar: Option<Markup>,
    content: Markup,
) -> Markup {
    html! {
        (DOCTYPE)
        html {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                (render_favicon_links())
                base href=(&shell.base_href);
                title { (title) }
                style { (PreEscaped(styles())) }
            }
            body {
                main.shell data-refresh-token-url=(&shell.refresh_token_href) {
                    aside.sidebar {
                        (render_sidebar(shell))
                    }
                    div.main-column {
                        @if let Some(tab_bar) = tab_bar {
                            (tab_bar)
                        }
                        (content)
                    }
                }
                script { (PreEscaped(interaction_script())) }
            }
        }
    }
}

pub(super) fn render_favicon_links() -> Markup {
    html! {
        link rel="icon" type="image/svg+xml" href="/favicon.svg";
        link rel="shortcut icon" href="/favicon.svg";
    }
}
