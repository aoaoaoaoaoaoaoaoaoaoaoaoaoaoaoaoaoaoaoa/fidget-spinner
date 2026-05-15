use super::assets::styles;
use super::detail::{render_favicon_links, render_shell};
use super::{
    BTreeMap, BTreeSet, DOCTYPE, FrontierSummary, KpiSummary, ListExperimentsQuery,
    ListHypothesesQuery, Markup, MetricKeysQuery, MetricScope, MoveKpiDirection, NavigatorState,
    NonEmptyText, PreEscaped, ProjectIndexItem, ProjectMetricsQuery, ProjectRenderContext,
    ProjectStatus, RegistryLockMode, RegistryName, StoreError, TagName, TagUsage, arrow_down_icon,
    arrow_up_icon, chevron_down_icon, chevron_up_icon, format_metric_value, format_timestamp,
    frontier_href, frontier_results_href, frontier_status_class, html, limit_items,
    list_project_manifests, load_shell_frame, metric_choice_detail, open_store, pencil_icon,
    plus_icon, project_root_href, project_state_home, render_fact, render_kv,
    render_markdown_prose, render_metric_choice_option, render_metric_kind_chip,
    status_chip_classes, trash_icon,
};

pub(super) fn render_project_index(state: NavigatorState) -> Result<Markup, StoreError> {
    let state_home = project_state_home()?;
    let projects = list_project_manifests()?
        .into_iter()
        .map(|manifest| {
            let store = open_store(manifest.project_root.as_std_path())?;
            Ok(ProjectIndexItem {
                project_root: manifest.project_root,
                project_status: store.status()?,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;

    Ok(html! {
        (DOCTYPE)
        html {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                (render_favicon_links())
                title { "Fidget Spinner navigator" }
                style { (PreEscaped(styles())) }
            }
            body {
                main.index-shell {
                    header.page-header {
                        div.eyebrow { "home" }
                        h1.page-title { "Fidget Spinner navigator" }
                        p.page-subtitle {
                            "Central project index from "
                            code { (state_home.as_str()) }
                        }
                    }
                    section.card {
                        h2 { "Projects" }
                        @if projects.is_empty() {
                            p.muted { "No self-described Spinner project stores are indexed yet." }
                        } @else {
                            div.card-grid {
                                @for project in limit_items(&projects, state.limit) {
                                    article.mini-card {
                                        div.card-header {
                                            a.title-link href=(project_root_href(&project.project_root)) {
                                                (&project.project_status.display_name)
                                            }
                                        }
                                        p.prose { (project.project_root.as_str()) }
                                        div.meta-row {
                                            span { (format!("{} frontiers", project.project_status.frontier_count)) }
                                            span { (format!("{} hypotheses", project.project_status.hypothesis_count)) }
                                        }
                                        div.meta-row {
                                            span { (format!("{} experiments", project.project_status.experiment_count)) }
                                            span { (format!("{} open", project.project_status.open_experiment_count)) }
                                        }
                                        div.meta-row.muted {
                                            span { (project.project_status.state_root.as_str()) }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

pub(super) fn render_project_home(context: ProjectRenderContext) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let shell = load_shell_frame(&store, None, &context)?;
    let title = format!("{} navigator", shell.project_status.display_name);
    let content = html! {
        (render_project_status(&shell.project_status, &context.base_href))
        (render_frontier_grid(&shell.frontiers, context.limit))
    };
    Ok(render_shell(&title, &shell, None, content))
}

pub(super) fn render_project_tags(context: ProjectRenderContext) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let shell = load_shell_frame(&store, None, &context)?;
    let registry = store.tag_registry(fidget_spinner_store_sqlite::TagRegistryQuery {
        include_hidden: true,
    })?;
    let usage = load_tag_usage(&store)?;
    let title = format!("{} · tags", shell.project_status.display_name);
    let mandatory_count = registry
        .families
        .iter()
        .filter(|family| family.mandatory)
        .count();
    let lock_state = TagLockState::from_locks(&registry.locks);
    let orphan_count = registry
        .tags
        .iter()
        .filter(|tag| {
            usage
                .get(&tag.name)
                .is_none_or(|usage| usage.hypotheses + usage.experiments == 0)
        })
        .count();
    let content = html! {
        section.card.tag-state-card {
            div.tag-state-band {
                div.fact-strip {
                    (render_fact("active tags", &registry.tags.len().to_string()))
                    (render_fact("families", &registry.families.len().to_string()))
                    (render_fact("mandatory", &mandatory_count.to_string()))
                    (render_fact("orphans", &orphan_count.to_string()))
                }
                div.tag-state-controls {
                    (render_tag_lock_switch(
                        "new tags",
                        "add",
                        lock_state.add_locked,
                        "When locked, MCP cannot create new tags. Supervisor UI can still curate the registry.",
                    ))
                    (render_tag_lock_switch(
                        "registry edits",
                        "edit",
                        lock_state.edit_locked,
                        "When locked, MCP-origin registry editing is forbidden. Supervisor UI remains authoritative; model assignment of existing tags stays open.",
                    ))
                }
            }
        }
        (render_tag_families(&registry.families))
        (render_tag_table(&registry.tags, &registry.families, &usage))
        @if !registry.name_history.is_empty() {
            section.card {
                h2 { "Name History" }
                div.tag-history-list {
                    @for history in &registry.name_history {
                        div.tag-history-row {
                            span.tag-chip { (history.name) }
                            span.muted { (history.disposition.as_str()) }
                            span { (history.message) }
                        }
                    }
                }
            }
        }
    };
    Ok(render_shell(&title, &shell, None, content))
}

pub(super) fn render_project_metrics(
    context: ProjectRenderContext,
    query: ProjectMetricsQuery,
) -> Result<Markup, StoreError> {
    let store = open_store(context.project_root.as_std_path())?;
    let shell = load_shell_frame(&store, None, &context)?;
    let metrics = store.metric_keys(MetricKeysQuery {
        frontier: None,
        scope: MetricScope::All,
    })?;
    let active_frontiers = &shell.frontiers;
    let selected_frontier = selected_kpi_frontier(active_frontiers, query.frontier.as_deref());
    let selected_kpis = selected_frontier
        .as_ref()
        .map(|frontier| {
            store.list_kpis(fidget_spinner_store_sqlite::KpiListQuery {
                frontier: frontier.slug.to_string(),
            })
        })
        .transpose()?
        .unwrap_or_default();
    let kpi_creation_locked = selected_frontier
        .as_ref()
        .map(|frontier| {
            store
                .frontier_registry_lock(
                    &RegistryName::kpis(),
                    RegistryLockMode::Assignment,
                    frontier.slug.as_str(),
                )
                .map(|lock| lock.is_some())
        })
        .transpose()?
        .unwrap_or(false);
    let kpi_count = active_frontiers
        .iter()
        .map(|frontier| {
            store
                .list_kpis(fidget_spinner_store_sqlite::KpiListQuery {
                    frontier: frontier.slug.to_string(),
                })
                .map(|kpis| kpis.len())
        })
        .collect::<Result<Vec<_>, StoreError>>()?
        .into_iter()
        .sum::<usize>();
    let hidden_count = metrics
        .iter()
        .filter(|metric| !metric.default_visibility.is_default_visible())
        .count();
    let orphan_count = metrics
        .iter()
        .filter(|metric| metric.reference_count == 0)
        .count();
    let title = format!("{} · metrics", shell.project_status.display_name);
    let content = html! {
        section.card.tag-state-card {
            div.tag-state-band {
                div.fact-strip {
                    (render_fact("metrics", &metrics.len().to_string()))
                    (render_fact("hidden", &hidden_count.to_string()))
                    (render_fact("KPIs", &kpi_count.to_string()))
                    (render_fact("orphans", &orphan_count.to_string()))
                }
            }
        }
        (render_kpi_manager(
            active_frontiers,
            selected_frontier.as_ref(),
            &selected_kpis,
            &metrics,
            kpi_creation_locked,
        ))
        (render_metric_registry_table(
            &metrics,
            selected_frontier.as_ref(),
            &selected_kpis,
        ))
    };
    Ok(render_shell(&title, &shell, None, content))
}

fn load_tag_usage(
    store: &fidget_spinner_store_sqlite::ProjectStore,
) -> Result<BTreeMap<TagName, TagUsage>, StoreError> {
    let mut usage = BTreeMap::<TagName, TagUsage>::new();
    for hypothesis in store.list_hypotheses(ListHypothesesQuery {
        limit: None,
        ..ListHypothesesQuery::default()
    })? {
        for tag in hypothesis.tags {
            usage.entry(tag).or_default().hypotheses += 1;
        }
    }
    for experiment in store.list_experiments(ListExperimentsQuery {
        limit: None,
        ..ListExperimentsQuery::default()
    })? {
        for tag in experiment.tags {
            usage.entry(tag).or_default().experiments += 1;
        }
    }
    Ok(usage)
}

#[derive(Clone, Copy, Default)]
struct TagLockState {
    add_locked: bool,
    edit_locked: bool,
}

impl TagLockState {
    fn from_locks(locks: &[fidget_spinner_core::RegistryLockRecord]) -> Self {
        Self {
            add_locked: locks
                .iter()
                .any(|lock| lock.mode == RegistryLockMode::Definition),
            edit_locked: locks
                .iter()
                .any(|lock| lock.mode == RegistryLockMode::Family),
        }
    }
}

fn render_tag_lock_switch(label: &str, mode: &str, locked: bool, help: &str) -> Markup {
    html! {
        form.tag-lock-switch-form method="post" action="tags/lock" data-preserve-viewport="true" {
            input type="hidden" name="mode" value=(mode);
            input type="hidden" name="locked" value=(if locked { "unlock" } else { "lock" });
            button
                type="submit"
                class=(if locked { "tag-lock-switch locked" } else { "tag-lock-switch" })
                aria-pressed=(if locked { "true" } else { "false" })
                title=(help)
            {
                span.switch-track aria-hidden="true" {
                    span.switch-thumb {}
                }
                span.switch-label { (label) }
                span.switch-state { (if locked { "locked" } else { "open" }) }
            }
        }
    }
}

fn render_tag_families(families: &[fidget_spinner_core::TagFamilyRecord]) -> Markup {
    html! {
        section.card {
            div.card-header {
                h2 { "Families" }
            }
            form.tag-create-form method="post" action="tags/families/create" data-preserve-viewport="true" {
                input.compact-input type="text" name="name" placeholder="family name";
                input.compact-input type="text" name="description" placeholder="description";
                label.inline-check {
                    input type="checkbox" name="mandatory" value="1";
                    "mandatory"
                }
                button.form-button type="submit" { "Create Family" }
            }
            @if families.is_empty() {
                p.muted { "No families yet." }
            } @else {
                div.tag-family-grid {
                    @for family in families {
                        article.mini-card {
                            div.card-header {
                                strong { (family.name) }
                                div.family-policy-row {
                                    span.status-chip { (if family.mandatory { "mandatory" } else { "optional" }) }
                                    form.tag-inline-form method="post" action="tags/family-mandatory" data-preserve-viewport="true" {
                                        input type="hidden" name="family" value=(family.name.as_str());
                                        input type="hidden" name="expected_revision" value=(family.revision);
                                        input type="hidden" name="mandatory" value=(if family.mandatory { "optional" } else { "mandatory" });
                                        button.form-button type="submit" {
                                            (if family.mandatory { "Make Optional" } else { "Make Mandatory" })
                                        }
                                    }
                                }
                            }
                            p.prose { (family.description) }
                        }
                    }
                }
            }
        }
    }
}

fn render_tag_table(
    tags: &[fidget_spinner_core::TagRecord],
    families: &[fidget_spinner_core::TagFamilyRecord],
    usage: &BTreeMap<TagName, TagUsage>,
) -> Markup {
    html! {
        section.card {
            div.card-header {
                h2 { "Tag Registry" }
                (render_create_tag_form(families))
            }
            @if tags.is_empty() {
                p.muted { "No tags yet." }
            } @else {
                div.table-wrap {
                    table.dense-table.tag-registry-table {
                        thead {
                            tr {
                                th { "Tag" }
                                th { "Family" }
                                th { "Description" }
                                th { "Use" }
                                th { "Merge" }
                            }
                        }
                        tbody {
                            @for tag in tags {
                                @let tag_usage = usage.get(&tag.name).copied().unwrap_or_default();
                                tr {
                                    td.no-truncate {
                                        div.tag-identity-row {
                                            form.tag-icon-form method="post" action="tags/delete" data-preserve-viewport="true" {
                                                input type="hidden" name="tag" value=(tag.name.as_str());
                                                input type="hidden" name="expected_revision" value=(tag.revision);
                                                button.inline-icon-button.danger-icon-button type="submit" aria-label=(format!("Delete {}", tag.name)) title="Delete tag" {
                                                    (trash_icon())
                                                }
                                            }
                                            form.tag-inline-rename-form method="post" action="tags/rename" data-preserve-viewport="true" data-inline-edit-form="true" data-original-value=(tag.name.as_str()) {
                                                input type="hidden" name="tag" value=(tag.name.as_str());
                                                input type="hidden" name="expected_revision" value=(tag.revision);
                                                span.tag-chip data-inline-edit-label="true" { (tag.name) }
                                                button.inline-icon-button type="button" data-inline-edit-trigger="true" aria-label=(format!("Rename {}", tag.name)) title="Rename tag" {
                                                    (pencil_icon())
                                                }
                                                input.inline-rename-input type="text" name="new_name" value=(tag.name.as_str()) aria-label=(format!("New name for {}", tag.name)) data-inline-edit-input="true";
                                            }
                                        }
                                    }
                                    td.no-truncate {
                                        form.tag-inline-form method="post" action="tags/tag-family" data-preserve-viewport="true" {
                                            input type="hidden" name="tag" value=(tag.name.as_str());
                                            input type="hidden" name="expected_revision" value=(tag.revision);
                                            select.compact-select name="family" data-auto-submit="true" aria-label=(format!("Family for {}", tag.name)) {
                                                option value="" selected[tag.family.is_none()] { "none" }
                                                @for family in families {
                                                    option
                                                        value=(family.name.as_str())
                                                        selected[tag.family.as_ref() == Some(&family.name)]
                                                    {
                                                        (family.name)
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    td {
                                        (tag.description)
                                    }
                                    td.no-truncate {
                                        (tag_usage.hypotheses) " H · " (tag_usage.experiments) " E"
                                    }
                                    td.no-truncate {
                                        form.tag-inline-form method="post" action="tags/merge" data-preserve-viewport="true" {
                                            input type="hidden" name="source" value=(tag.name.as_str());
                                            input type="hidden" name="expected_revision" value=(tag.revision);
                                            select.compact-select name="target" {
                                                @for target in tags {
                                                    @if target.name != tag.name {
                                                        option value=(target.name.as_str()) { (target.name) }
                                                    }
                                                }
                                            }
                                            button.form-button type="submit" { "Merge" }
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

fn render_create_tag_form(families: &[fidget_spinner_core::TagFamilyRecord]) -> Markup {
    html! {
        form.tag-create-form method="post" action="tags/create" data-preserve-viewport="true" {
            input.compact-input type="text" name="name" placeholder="new tag" aria-label="New tag name";
            input.compact-input.wide-compact-input type="text" name="description" placeholder="description shown to agents" aria-label="New tag description";
            select.compact-select name="family" aria-label="New tag family" title="Optional family for the new tag." {
                option value="" { "no family" }
                @for family in families {
                    option value=(family.name.as_str()) { (family.name) }
                }
            }
            button.inline-icon-button type="submit" aria-label="Add tag" title="Add tag" {
                (plus_icon())
            }
        }
    }
}

fn render_create_metric_form(metrics: &[fidget_spinner_store_sqlite::MetricKeySummary]) -> Markup {
    html! {
        div.metric-create-stack {
            form.tag-create-form.metric-create-form method="post" action="metrics/create" data-preserve-viewport="true" {
                span.metric-create-label { "Observed" }
                input.compact-input type="text" name="key" placeholder="metric_key" aria-label="Metric key" required;
                select.compact-select name="dimension" aria-label="Metric dimension" {
                    option value="time" { "time" }
                    option value="count" { "count" }
                    option value="bytes" { "bytes" }
                    option value="dimensionless" { "dimensionless" }
                }
                input.compact-input type="text" name="display_unit" placeholder="milliseconds" aria-label="Display unit";
                input type="hidden" name="aggregation" value="point";
                (render_metric_objective_select())
                input.compact-input.wide-compact-input type="text" name="description" placeholder="description" aria-label="Metric description";
                button.inline-icon-button type="submit" aria-label="Add observed metric" title="Add observed metric" {
                    (plus_icon())
                }
            }
            @if !metrics.is_empty() {
                form.tag-create-form.metric-create-form.synthetic-metric-create-form method="post" action="metrics/synthetic/create" data-preserve-viewport="true" {
                    span.metric-create-label { "Synthetic" }
                    input.compact-input type="text" name="key" placeholder="synthetic_key" aria-label="Synthetic metric key" required;
                    select.compact-select name="operation" aria-label="Synthetic operation" data-synthetic-operation-select="true" {
                        option value="add" { "+" }
                        option value="sub" { "-" }
                        option value="mul" { "*" }
                        option value="div" { "/" }
                        option value="gmean" { "gmean" }
                    }
                    (render_metric_operand_select("left", "Left operand", metrics, true))
                    (render_metric_operand_select("right", "Right operand", metrics, true))
                    (render_metric_operand_select("term_3", "Extra gmean term 3", metrics, false))
                    (render_metric_operand_select("term_4", "Extra gmean term 4", metrics, false))
                    input type="hidden" name="aggregation" value="point";
                    (render_metric_objective_select())
                    input.compact-input.wide-compact-input type="text" name="description" placeholder="synthetic description" aria-label="Synthetic metric description";
                    button.inline-icon-button type="submit" aria-label="Add synthetic metric" title="Add synthetic metric" {
                        (plus_icon())
                    }
                }
            }
        }
    }
}

fn render_metric_objective_select() -> Markup {
    html! {
        select.compact-select name="objective" aria-label="Objective" {
            option value="minimize" { "minimize" }
            option value="maximize" { "maximize" }
            option value="target" { "target" }
        }
    }
}

fn render_metric_operand_select(
    name: &str,
    label: &str,
    metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    required: bool,
) -> Markup {
    if required {
        html! {
            select.compact-select.wide-compact-select name=(name) aria-label=(label) required data-metric-choice-select="true" {
                (render_metric_operand_options(None, metrics))
            }
        }
    } else {
        html! {
            select.compact-select.wide-compact-select name=(name) aria-label=(label) data-metric-choice-select="true" data-synthetic-gmean-extra="true" title="Only used by the gmean operation." {
                (render_metric_operand_options(Some(label), metrics))
            }
        }
    }
}

fn render_metric_operand_options(
    placeholder: Option<&str>,
    metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
) -> Markup {
    html! {
        @if let Some(placeholder) = placeholder {
            option value="" { (placeholder) }
        }
        @for metric in metrics {
            (render_metric_choice_option(metric))
        }
    }
}

fn selected_kpi_frontier(
    frontiers: &[FrontierSummary],
    requested: Option<&str>,
) -> Option<FrontierSummary> {
    requested
        .and_then(|selector| {
            frontiers.iter().find(|frontier| {
                frontier.slug.as_str() == selector || frontier.id.to_string() == selector
            })
        })
        .or_else(|| frontiers.first())
        .cloned()
}

fn render_kpi_manager(
    frontiers: &[FrontierSummary],
    selected_frontier: Option<&FrontierSummary>,
    kpis: &[KpiSummary],
    metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    kpi_creation_locked: bool,
) -> Markup {
    html! {
        section.card {
            div.card-header {
                h2 { "KPI Metrics" }
                @if let Some(frontier) = selected_frontier {
                    span.muted { (frontier.label) }
                    a.form-button href=(frontier_results_href(&frontier.slug)) { "Results" }
                    (render_kpi_lock_switch(frontier, kpi_creation_locked))
                }
            }
            @if frontiers.is_empty() {
                p.muted { "No active frontiers. Archived frontiers are intentionally excluded from KPI management." }
            } @else if let Some(frontier) = selected_frontier {
                form.tag-create-form method="get" action="metrics" data-preserve-viewport="true" {
                    select.compact-select.wide-compact-select name="frontier" aria-label="KPI frontier" data-auto-submit="true" {
                        @for option in frontiers {
                            option value=(option.slug.as_str()) selected[option.slug == frontier.slug] {
                                (option.label)
                            }
                        }
                    }
                }
                (render_create_kpi_form(frontier, kpis, metrics))
                (render_kpi_registry(frontier, kpis))
            }
        }
    }
}

fn render_kpi_lock_switch(frontier: &FrontierSummary, locked: bool) -> Markup {
    html! {
        form.tag-lock-switch-form method="post" action="metrics/kpi/lock" data-preserve-viewport="true" {
            input type="hidden" name="frontier" value=(frontier.slug.as_str());
            input type="hidden" name="locked" value=(if locked { "unlock" } else { "lock" });
            button
                type="submit"
                class=(if locked { "tag-lock-switch locked" } else { "tag-lock-switch" })
                aria-pressed=(if locked { "true" } else { "false" })
                title="When locked, MCP cannot promote metrics into KPIs for this frontier. Supervisor UI and CLI KPI edits remain open."
            {
                span.switch-track aria-hidden="true" {
                    span.switch-thumb {}
                }
                span.switch-label { "MCP KPI create" }
                span.switch-state { (if locked { "locked" } else { "open" }) }
            }
        }
    }
}

fn render_create_kpi_form(
    frontier: &FrontierSummary,
    kpis: &[KpiSummary],
    metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
) -> Markup {
    let kpi_keys = kpis
        .iter()
        .map(|kpi| kpi.metric.key.clone())
        .collect::<BTreeSet<_>>();
    let candidates = metrics
        .iter()
        .filter(|metric| !kpi_keys.contains(&metric.key))
        .collect::<Vec<_>>();
    let has_candidates = !candidates.is_empty();
    html! {
        form.tag-create-form method="post" action="metrics/kpi" data-preserve-viewport="true" {
            input type="hidden" name="frontier" value=(frontier.slug.as_str());
            select.compact-select.wide-compact-select name="metric" aria-label="Metric to promote" required data-metric-choice-select="true" {
                @if has_candidates {
                    @for metric in candidates {
                        (render_metric_choice_option(metric))
                    }
                } @else {
                    option value="" { "all metrics are KPIs" }
                }
            }
            button.inline-icon-button.promote-icon-button type="submit" aria-label="Promote KPI metric" title="Promote metric to KPI" disabled[!has_candidates] {
                (chevron_up_icon())
            }
        }
    }
}

pub(super) fn render_kpi_registry(frontier: &FrontierSummary, kpis: &[KpiSummary]) -> Markup {
    let has_reorder = kpis.len() > 1;
    html! {
        @if kpis.is_empty() {
            p.muted { "No KPI metrics for this frontier yet." }
        } @else {
            div.table-scroll {
                table.metric-table.kpi-table {
                    colgroup {
                        col.kpi-action-col;
                        col.kpi-metric-col;
                        col.kpi-unit-col;
                        col.kpi-obs-col;
                    }
                    thead {
                        tr {
                            th { "" }
                            th { "Metric" }
                            th { "Unit" }
                            th { "Obs" }
                        }
                    }
                    tbody {
                        @for (index, kpi) in kpis.iter().enumerate() {
                            tr.kpi-metric-row {
                                td.no-truncate.kpi-action-cell {
                                    div.inline-action-row {
                                        @if has_reorder {
                                            (render_kpi_move_button(frontier, kpi, MoveKpiDirection::Up, index == 0))
                                            (render_kpi_move_button(frontier, kpi, MoveKpiDirection::Down, index + 1 == kpis.len()))
                                        }
                                        form.tag-icon-form method="post" action="metrics/kpi/delete" data-preserve-viewport="true" {
                                            input type="hidden" name="frontier" value=(frontier.slug.as_str());
                                            input type="hidden" name="kpi" value=(kpi.metric.key.as_str());
                                            button.inline-icon-button.danger-icon-button type="submit" aria-label=(format!("Demote KPI metric {}", kpi.metric.key)) title="Demote KPI metric" {
                                                (chevron_down_icon())
                                            }
                                        }
                                    }
                                }
                                td.kpi-metric-cell {
                                    div.kpi-metric-stack {
                                        div.metric-name-row {
                                            (render_metric_kind_chip(&kpi.metric))
                                            span.tag-chip { (kpi.metric.key) }
                                        }
                                    @if let Some(description) = kpi.metric.description.as_ref() {
                                            div.kpi-description.muted { (description) }
                                        }
                                    }
                                }
                                td.no-truncate.kpi-unit-cell { (kpi.metric.display_unit.label()) }
                                td.no-truncate.kpi-obs-cell { (kpi.metric.reference_count) }
                            }
                            tr.kpi-reference-row {
                                td.kpi-reference-gutter {}
                                td.kpi-reference-lane colspan="3" {
                                    (render_kpi_reference_editor(frontier, kpi))
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_kpi_reference_editor(frontier: &FrontierSummary, kpi: &KpiSummary) -> Markup {
    html! {
        div.kpi-reference-band {
            span.kpi-reference-heading { "References" }
            div.kpi-reference-stack {
                @if !kpi.references.is_empty() {
                    div.kpi-reference-chip-row {
                        @for reference in &kpi.references {
                            span.kpi-reference-chip title=(format!(
                                "{} = {}",
                                reference.label,
                                format_metric_value(reference.value, &reference.display_unit),
                            )) {
                                span.kpi-reference-label { (&reference.label) }
                                span.kpi-reference-value {
                                    (format_metric_value(reference.value, &reference.display_unit))
                                }
                                form.tag-icon-form method="post" action="metrics/kpi/reference/delete" data-preserve-viewport="true" {
                                    input type="hidden" name="frontier" value=(frontier.slug.as_str());
                                    input type="hidden" name="kpi" value=(kpi.metric.key.as_str());
                                    input type="hidden" name="reference" value=(reference.label.as_str());
                                    button.inline-icon-button.danger-icon-button type="submit" aria-label=(format!("Delete KPI reference {}", reference.label)) title="Delete reference line" {
                                        (trash_icon())
                                    }
                                }
                            }
                        }
                    }
                }
                form.kpi-reference-form method="post" action="metrics/kpi/reference" data-preserve-viewport="true" {
                    input type="hidden" name="frontier" value=(frontier.slug.as_str());
                    input type="hidden" name="kpi" value=(kpi.metric.key.as_str());
                    input.compact-input.kpi-reference-label-input type="text" name="label" placeholder="label" aria-label=(format!("Reference label for {}", kpi.metric.key)) required;
                    input.compact-input.kpi-reference-value-input type="number" step="any" name="value" placeholder="value" aria-label=(format!("Reference value for {}", kpi.metric.key)) required;
                    input.compact-input.kpi-reference-unit-input type="text" name="unit" placeholder=(kpi.metric.display_unit.label()) aria-label=(format!("Reference unit for {}", kpi.metric.key));
                    button.inline-icon-button type="submit" aria-label=(format!("Set KPI reference for {}", kpi.metric.key)) title="Set reference line" {
                        (plus_icon())
                    }
                }
            }
        }
    }
}

fn render_kpi_move_button(
    frontier: &FrontierSummary,
    kpi: &KpiSummary,
    direction: MoveKpiDirection,
    disabled: bool,
) -> Markup {
    let (label, title, icon) = match direction {
        MoveKpiDirection::Up => ("Move KPI metric earlier", "Move earlier", arrow_up_icon()),
        MoveKpiDirection::Down => ("Move KPI metric later", "Move later", arrow_down_icon()),
    };
    html! {
        form.tag-icon-form method="post" action="metrics/kpi/move" data-preserve-viewport="true" {
            input type="hidden" name="frontier" value=(frontier.slug.as_str());
            input type="hidden" name="kpi" value=(kpi.metric.key.as_str());
            input type="hidden" name="direction" value=(kpi_move_direction_value(direction));
            button.inline-icon-button type="submit" aria-label=(format!("{label} {}", kpi.metric.key)) title=(title) disabled[disabled] {
                (icon)
            }
        }
    }
}

const fn kpi_move_direction_value(direction: MoveKpiDirection) -> &'static str {
    match direction {
        MoveKpiDirection::Up => "up",
        MoveKpiDirection::Down => "down",
    }
}

fn render_metric_promote_kpi_button(
    selected_frontier: Option<&FrontierSummary>,
    metric: &fidget_spinner_store_sqlite::MetricKeySummary,
    already_kpi: bool,
) -> Markup {
    html! {
        @if let Some(frontier) = selected_frontier {
            form.tag-icon-form method="post" action="metrics/kpi" data-preserve-viewport="true" {
                input type="hidden" name="frontier" value=(frontier.slug.as_str());
                input type="hidden" name="metric" value=(metric.key.as_str());
                button.inline-icon-button.promote-icon-button
                    type="submit"
                    aria-label=(format!("Promote {} to KPI", metric.key))
                    title=(if already_kpi { "Already a KPI for selected frontier" } else { "Promote metric to KPI" })
                    disabled[already_kpi] {
                    (chevron_up_icon())
                }
            }
        }
    }
}

pub(super) fn render_metric_registry_table(
    metrics: &[fidget_spinner_store_sqlite::MetricKeySummary],
    selected_frontier: Option<&FrontierSummary>,
    kpis: &[KpiSummary],
) -> Markup {
    let kpi_keys = kpis
        .iter()
        .map(|kpi| kpi.metric.key.clone())
        .collect::<BTreeSet<_>>();
    html! {
        section.card {
            div.card-header { h2 { "Metric Registry" } }
            (render_create_metric_form(metrics))
            @if metrics.is_empty() {
                p.muted { "No metrics yet." }
            } @else {
                div.table-scroll {
                    datalist id="metric-merge-targets" {
                        @for target in metrics {
                            option value=(target.key.as_str()) title=(metric_choice_detail(target)) {}
                        }
                    }
                    table.metric-table {
                        thead {
                            tr {
                                th { "" }
                                th.metric-registry-filter-heading {
                                    div.metric-registry-filter-cell {
                                        span { "Metric" }
                                        input.compact-input.metric-registry-filter
                                            type="search"
                                            placeholder="filter"
                                            aria-label="Filter metrics"
                                            data-table-filter-input="metric-registry";
                                    }
                                }
                                th { "Dimension" }
                                th { "Refs" }
                                th { "Merge" }
                            }
                        }
                        tbody {
                            @for metric in metrics {
                                tr data-table-filter-row="metric-registry" data-table-filter-text=(metric_registry_filter_text(metric)) {
                                    td.no-truncate {
                                        div.inline-action-row {
                                            (render_metric_promote_kpi_button(
                                                selected_frontier,
                                                metric,
                                                kpi_keys.contains(&metric.key),
                                            ))
                                            form.tag-icon-form method="post" action="metrics/delete" data-preserve-viewport="true" {
                                                input type="hidden" name="metric" value=(metric.key.as_str());
                                                button.inline-icon-button.danger-icon-button type="submit" aria-label=(format!("Delete {}", metric.key)) title="Delete unused metric" {
                                                    (trash_icon())
                                                }
                                            }
                                        }
                                    }
                                    td.no-truncate {
                                        div.metric-identity-stack {
                                            div.metric-name-row {
                                                (render_metric_kind_chip(metric))
                                                span class=(format!("metric-objective-chip metric-objective-{}", metric.objective.as_str()))
                                                    title=(metric.objective.as_str()) {
                                                    (metric_objective_chip_label(metric.objective.as_str()))
                                                }
                                                form.tag-inline-rename-form.metric-name-form method="post" action="metrics/rename" data-preserve-viewport="true" data-inline-edit-form="true" data-original-value=(metric.key.as_str()) {
                                                    input type="hidden" name="metric" value=(metric.key.as_str());
                                                    span.tag-chip data-inline-edit-label="true" { (metric.key) }
                                                    button.inline-icon-button type="button" data-inline-edit-trigger="true" aria-label=(format!("Rename {}", metric.key)) title="Rename metric" {
                                                        (pencil_icon())
                                                    }
                                                    input.inline-rename-input type="text" name="new_key" value=(metric.key.as_str()) aria-label=(format!("New key for {}", metric.key)) data-inline-edit-input="true";
                                                }
                                            }
                                            form.tag-inline-rename-form.metric-description-form method="post" action="metrics/description" data-preserve-viewport="true" data-inline-edit-form="true" data-inline-edit-allow-clear="true" data-original-value=(metric.description.as_ref().map_or("", NonEmptyText::as_str)) {
                                                input type="hidden" name="metric" value=(metric.key.as_str());
                                                span.muted data-inline-edit-label="true" {
                                                    @if let Some(description) = metric.description.as_ref() {
                                                        (description)
                                                    } @else {
                                                        "No description"
                                                    }
                                                }
                                                button.inline-icon-button type="button" data-inline-edit-trigger="true" aria-label=(format!("Edit description for {}", metric.key)) title="Edit description" {
                                                    (pencil_icon())
                                                }
                                                input.inline-rename-input.wide-compact-input type="text" name="description" value=(metric.description.as_ref().map_or("", NonEmptyText::as_str)) placeholder="description" aria-label=(format!("Description for {}", metric.key)) data-inline-edit-input="true";
                                            }
                                        }
                                    }
                                    td.no-truncate { (metric.dimension.to_string()) }
                                    td.no-truncate { (metric.reference_count) }
                                    td.no-truncate {
                                        form.tag-inline-form method="post" action="metrics/merge" data-preserve-viewport="true" {
                                            input type="hidden" name="source" value=(metric.key.as_str());
                                            input.compact-input
                                                type="text"
                                                name="target"
                                                list="metric-merge-targets"
                                                placeholder="merge into..."
                                                aria-label=(format!("Merge target for {}", metric.key))
                                                data-auto-submit="true";
                                        }
                                    }
                                }
                            }
                            tr data-table-filter-empty="metric-registry" hidden {
                                td.muted colspan="5" { "No matching metrics." }
                            }
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn metric_registry_filter_text(
    metric: &fidget_spinner_store_sqlite::MetricKeySummary,
) -> String {
    let dimension = metric.dimension.to_string();
    [
        metric.key.as_str(),
        dimension.as_str(),
        metric.objective.as_str(),
        metric.description.as_ref().map_or("", NonEmptyText::as_str),
    ]
    .join(" ")
}

fn metric_objective_chip_label(objective: &str) -> &'static str {
    match objective {
        "maximize" => "MAX",
        "minimize" => "MIN",
        "target" => "TGT",
        _ => "OBJ",
    }
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
                    article.mini-card.frontier-card {
                        div.frontier-card-header {
                            a.frontier-card-title href=(frontier_href(&frontier.slug)) title=(frontier.label.as_str()) { (frontier.label) }
                            span class=(format!("frontier-card-status {}", status_chip_classes(frontier_status_class(frontier.status.as_str())))) {
                                (frontier.status.as_str())
                            }
                        }
                        p.frontier-card-objective title=(frontier.objective.as_str()) { (frontier.objective) }
                        div.meta-row {
                            span { (format!("{} worklist hypotheses", frontier.worklist_hypothesis_count)) }
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

fn render_project_status(status: &ProjectStatus, base_href: &str) -> Markup {
    html! {
    section.card {
        div.frontier-title-row {
            h1 { (status.display_name) }
            details.control-popout.frontier-summary-editor {
                summary.inline-icon-button.frontier-edit-toggle aria-label="Edit project description" title="Edit project description" {
                    (pencil_icon())
                }
                div.control-popout-panel.frontier-summary-panel {
                    form.frontier-summary-form method="post" action=(format!("{base_href}description")) data-preserve-viewport="true" {
                        label.filter-control {
                            span.filter-label { "Project Description" }
                            textarea.compact-textarea.frontier-description-input
                                name="description"
                                rows="4"
                                placeholder="What is this project ledger for?"
                            {
                                @if let Some(description) = status.description.as_ref() {
                                    (description.as_str())
                                }
                            }
                        }
                        div.filter-actions {
                            button.form-button type="submit" { "Save" }
                        }
                    }
                }
            }
        }
        @if let Some(description) = status.description.as_ref() {
            (render_markdown_prose(description.as_str()))
        } @else {
            p.muted { "No project description recorded." }
        }
        div.kv-grid {
            (render_kv("Project root", status.project_root.as_str()))
            (render_kv("State root", status.state_root.as_str()))
            (render_kv("Store format", &status.store_format_version.to_string()))
            (render_kv("Frontiers", &status.frontier_count.to_string()))
            (render_kv("Hypotheses", &status.hypothesis_count.to_string()))
            (render_kv("Experiments", &status.experiment_count.to_string()))
            (render_kv("Open experiments", &status.open_experiment_count.to_string()))
        }
    }
    }
}
