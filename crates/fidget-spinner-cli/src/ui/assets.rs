mod styles;

use super::UI_NAV_STATE_KEY;
pub(super) use styles::styles;

pub(super) fn harden_autofill_controls(document: &str) -> String {
    let mut hardened = String::with_capacity(document.len() + 512);
    let mut cursor = 0;
    while let Some(tag_offset) = document[cursor..].find('<') {
        let tag_start = cursor + tag_offset;
        hardened.push_str(&document[cursor..tag_start]);
        let Some(tag_len) = document[tag_start..].find('>').map(|offset| offset + 1) else {
            hardened.push_str(&document[tag_start..]);
            return hardened;
        };
        let tag_end = tag_start + tag_len;
        hardened.push_str(&harden_autofill_tag(&document[tag_start..tag_end]));
        cursor = tag_end;
    }
    hardened.push_str(&document[cursor..]);
    hardened
}

fn harden_autofill_tag(tag: &str) -> String {
    let Some(tag_kind) = AutofillTagKind::from_tag(tag) else {
        return tag.to_owned();
    };
    if tag_kind == AutofillTagKind::HiddenInput {
        return tag.to_owned();
    }
    let mut attributes = Vec::with_capacity(2);
    if tag_kind.accepts_autocomplete_off() && !has_html_attribute(tag, "autocomplete") {
        attributes.push(r#" autocomplete="off""#);
    }
    if tag_kind.accepts_password_manager_ignore()
        && !has_html_attribute(tag, "data-protonpass-ignore")
    {
        attributes.push(r#" data-protonpass-ignore="true""#);
    }
    if attributes.is_empty() {
        return tag.to_owned();
    }
    let Some(insert_at) = tag.rfind('>') else {
        return tag.to_owned();
    };
    let mut hardened =
        String::with_capacity(tag.len() + attributes.iter().map(|attr| attr.len()).sum::<usize>());
    hardened.push_str(&tag[..insert_at]);
    for attribute in attributes {
        hardened.push_str(attribute);
    }
    hardened.push_str(&tag[insert_at..]);
    hardened
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum AutofillTagKind {
    Form,
    Field,
    HiddenInput,
}

impl AutofillTagKind {
    fn from_tag(tag: &str) -> Option<Self> {
        if !tag.starts_with('<') || tag.starts_with("</") || tag.starts_with("<!") {
            return None;
        }
        if tag_has_name(tag, "form") {
            return Some(Self::Form);
        }
        if tag_has_name(tag, "input") {
            return Some(if has_html_attribute_value(tag, "type", "hidden") {
                Self::HiddenInput
            } else {
                Self::Field
            });
        }
        (tag_has_name(tag, "select") || tag_has_name(tag, "textarea")).then_some(Self::Field)
    }

    const fn accepts_autocomplete_off(self) -> bool {
        matches!(self, Self::Form | Self::Field)
    }

    const fn accepts_password_manager_ignore(self) -> bool {
        matches!(self, Self::Field)
    }
}

fn tag_has_name(tag: &str, name: &str) -> bool {
    let Some(rest) = tag.strip_prefix('<').and_then(|tag| tag.strip_prefix(name)) else {
        return false;
    };
    rest.as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_whitespace() || matches!(byte, b'>' | b'/'))
}

fn has_html_attribute(tag: &str, name: &str) -> bool {
    tag.match_indices(name).any(|(index, _)| {
        let before = tag.as_bytes().get(index.wrapping_sub(1)).copied();
        let after = tag.as_bytes().get(index + name.len()).copied();
        before.is_some_and(|byte| byte.is_ascii_whitespace())
            && after.is_some_and(|byte| {
                matches!(byte, b'=' | b'>' | b'/') || byte.is_ascii_whitespace()
            })
    })
}

fn has_html_attribute_value(tag: &str, name: &str, value: &str) -> bool {
    let needle = format!(r#"{name}="{value}""#);
    has_html_attribute(tag, name) && tag.contains(&needle)
}

pub(super) fn interaction_script() -> String {
    format!(
        r#"
const UI_NAV_STATE_KEY = "{UI_NAV_STATE_KEY}";
const AUTO_REFRESH_INTERVAL_MS = 15000;
const PLOT_COPY_RESET_MS = 1600;
const plotCopyResetTimers = new WeakMap();
const chartMetadataCache = new WeakMap();
let chartFragmentController = null;
let chartFragmentSequence = 0;
let chartDrag = null;
let chartTooltipHideTimer = null;
let suppressChartClickUntil = 0;

function stashViewportState() {{
    try {{
        const openDetails = Array.from(
            document.querySelectorAll("details[data-preserve-open][open][id]")
        ).map((details) => details.id);
        sessionStorage.setItem(
            UI_NAV_STATE_KEY,
            JSON.stringify({{
                path: window.location.pathname,
                scrollX: window.scrollX,
                scrollY: window.scrollY,
                openDetails,
            }})
        );
    }} catch (_error) {{
        // Best-effort only. If sessionStorage is unavailable we degrade to normal reload behavior.
    }}
}}

function restoreViewportState() {{
    let rawState = null;
    try {{
        rawState = sessionStorage.getItem(UI_NAV_STATE_KEY);
    }} catch (_error) {{
        return;
    }}
    if (!rawState) {{
        return;
    }}
    try {{
        sessionStorage.removeItem(UI_NAV_STATE_KEY);
    }} catch (_error) {{
        // Ignore removal failure and keep going with restoration.
    }}

    let state = null;
    try {{
        state = JSON.parse(rawState);
    }} catch (_error) {{
        return;
    }}
    if (!state || state.path !== window.location.pathname) {{
        return;
    }}
    if (Array.isArray(state.openDetails)) {{
        for (const detailsId of state.openDetails) {{
            const details = document.getElementById(detailsId);
            if (details instanceof HTMLDetailsElement) {{
                details.open = true;
            }}
        }}
    }}
    const scrollX = Number.isFinite(state.scrollX) ? state.scrollX : 0;
    const scrollY = Number.isFinite(state.scrollY) ? state.scrollY : 0;
    requestAnimationFrame(() => {{
        window.scrollTo(scrollX, scrollY);
        requestAnimationFrame(() => {{
            window.scrollTo(scrollX, scrollY);
        }});
    }});
}}

restoreViewportState();

function plotCopyOriginalLabel(button) {{
    if (!button.dataset.copyLabel) {{
        button.dataset.copyLabel = button.textContent?.trim() || "Copy PNG";
    }}
    return button.dataset.copyLabel;
}}

function cancelPlotCopyReset(button) {{
    const existingTimer = plotCopyResetTimers.get(button);
    if (existingTimer) {{
        clearTimeout(existingTimer);
        plotCopyResetTimers.delete(button);
    }}
}}

function setPlotCopyButtonState(button, label, state, title) {{
    cancelPlotCopyReset(button);
    plotCopyOriginalLabel(button);
    button.textContent = label;
    button.toggleAttribute("data-copied", state === "copied");
    button.toggleAttribute("data-failed", state === "failed");
    if (title) {{
        button.title = title;
    }} else {{
        button.removeAttribute("title");
    }}
}}

function resetPlotCopyButton(button) {{
    button.textContent = plotCopyOriginalLabel(button);
    button.removeAttribute("data-copied");
    button.removeAttribute("data-failed");
    button.removeAttribute("title");
}}

function schedulePlotCopyReset(button) {{
    cancelPlotCopyReset(button);
    const timer = setTimeout(() => {{
        resetPlotCopyButton(button);
        plotCopyResetTimers.delete(button);
    }}, PLOT_COPY_RESET_MS);
    plotCopyResetTimers.set(button, timer);
}}

function autoRefreshRoot() {{
    return document.querySelector("[data-refresh-token-url]");
}}

function autoRefreshDeferred() {{
    const activeElement = document.activeElement;
    return Boolean(
        document.hidden
        || document.querySelector("details.control-popout[open]")
        || document.querySelector("form[data-inline-edit-form=\"true\"].editing")
        || activeElement instanceof HTMLInputElement
        || activeElement instanceof HTMLSelectElement
        || activeElement instanceof HTMLTextAreaElement
        || chartFragmentController !== null
        || chartDrag !== null
        || document.querySelector("button[data-copy-plot-png=\"true\"]:disabled")
    );
}}

async function pollRefreshToken() {{
    const root = autoRefreshRoot();
    if (!(root instanceof HTMLElement) || autoRefreshDeferred()) {{
        return;
    }}
    const tokenUrl = root.dataset.refreshTokenUrl;
    if (!tokenUrl) {{
        return;
    }}
    try {{
        const response = await fetch(tokenUrl, {{
            cache: "no-store",
            headers: {{ "Accept": "text/plain" }},
        }});
        if (!response.ok) {{
            return;
        }}
        const nextToken = (await response.text()).trim();
        if (!nextToken) {{
            return;
        }}
        const previousToken = root.dataset.refreshToken;
        if (!previousToken) {{
            root.dataset.refreshToken = nextToken;
            return;
        }}
        if (previousToken !== nextToken) {{
            stashViewportState();
            window.location.reload();
        }}
    }} catch (_error) {{
        // Auto-refresh must never degrade the navigator if the probe races shutdown.
    }}
}}

async function rasterizeSvgToPngBlob(svg) {{
    const svgClone = svg.cloneNode(true);
    if (svgClone instanceof SVGElement && !svgClone.getAttribute("xmlns")) {{
        svgClone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    }}
    const viewBox = svg.viewBox?.baseVal;
    const width = Math.ceil(
        (viewBox && viewBox.width) || Number(svg.getAttribute("width")) || svg.clientWidth
    );
    const height = Math.ceil(
        (viewBox && viewBox.height) || Number(svg.getAttribute("height")) || svg.clientHeight
    );
    if (!Number.isFinite(width) || !Number.isFinite(height) || width <= 0 || height <= 0) {{
        throw new Error("plot dimensions are unavailable");
    }}

    const svgText = new XMLSerializer().serializeToString(svgClone);
    const svgBlob = new Blob([svgText], {{ type: "image/svg+xml;charset=utf-8" }});
    const svgUrl = URL.createObjectURL(svgBlob);
    try {{
        const image = new Image();
        const imageLoaded = new Promise((resolve, reject) => {{
            image.onload = resolve;
            image.onerror = () => reject(new Error("plot image rasterization failed"));
        }});
        image.src = svgUrl;
        await imageLoaded;

        const canvas = document.createElement("canvas");
        canvas.width = width;
        canvas.height = height;
        const context = canvas.getContext("2d");
        if (!context) {{
            throw new Error("canvas rendering is unavailable");
        }}
        context.drawImage(image, 0, 0, width, height);
        return await new Promise((resolve, reject) => {{
            canvas.toBlob((blob) => {{
                if (blob) {{
                    resolve(blob);
                }} else {{
                    reject(new Error("PNG encoding failed"));
                }}
            }}, "image/png");
        }});
    }} finally {{
        URL.revokeObjectURL(svgUrl);
    }}
}}

async function copyPlotPng(button) {{
    if (!navigator.clipboard || typeof ClipboardItem === "undefined") {{
        throw new Error("PNG clipboard is unavailable in this browser");
    }}
    const frame = button.closest(".chart-frame");
    const svg = frame?.querySelector("svg");
    if (!(svg instanceof SVGSVGElement)) {{
        throw new Error("plot SVG was not found");
    }}
    const pngBlob = await rasterizeSvgToPngBlob(svg);
    await navigator.clipboard.write([
        new ClipboardItem({{ "image/png": pngBlob }}),
    ]);
}}

function chartCard() {{
    return document.getElementById("metric-plot-card");
}}

function chartRootSvg(card = chartCard()) {{
    const svg = card?.querySelector("svg[data-chart-navigator=\"true\"]");
    return svg instanceof SVGSVGElement ? svg : null;
}}

function commitChartUrl(url, replace = false) {{
    const next = url instanceof URL ? url : new URL(url, document.baseURI);
    if (replace) {{
        window.history.replaceState(null, "", next);
    }} else {{
        window.history.pushState(null, "", next);
    }}
}}

function syncChartTabLinks() {{
    const current = new URL(window.location.href);
    for (const link of document.querySelectorAll(".tab-row a")) {{
        if (!(link instanceof HTMLAnchorElement)) continue;
        const target = new URL(link.href, document.baseURI);
        const tab = target.searchParams.get("tab");
        target.search = current.search;
        if (tab) {{
            target.searchParams.set("tab", tab);
        }}
        link.href = target;
    }}
}}

async function requestChartFragment() {{
    const card = chartCard();
    if (!(card instanceof HTMLElement)) {{
        return;
    }}
    const fragmentUrl = card.dataset.chartFragmentUrl;
    if (!fragmentUrl) {{
        return;
    }}
    if (chartFragmentController) {{
        chartFragmentController.abort();
    }}
    const controller = new AbortController();
    chartFragmentController = controller;
    const sequence = ++chartFragmentSequence;
    const openDetails = Array.from(
        card.querySelectorAll("details[data-preserve-open][open][id]")
    ).map((details) => details.id);
    card.setAttribute("aria-busy", "true");
    const url = new URL(fragmentUrl, document.baseURI);
    url.search = window.location.search;
    try {{
        const response = await fetch(url, {{
            cache: "no-store",
            headers: {{ "Accept": "text/html" }},
            signal: controller.signal,
        }});
        if (!response.ok) {{
            throw new Error("chart refresh failed (" + response.status + ")");
        }}
        const documentFragment = new DOMParser().parseFromString(
            await response.text(),
            "text/html"
        );
        const nextCard = documentFragment.getElementById("metric-plot-card");
        if (!(nextCard instanceof HTMLElement)) {{
            throw new Error("chart fragment is malformed");
        }}
        if (sequence !== chartFragmentSequence) {{
            return;
        }}
        const root = autoRefreshRoot();
        const documentToken = root instanceof HTMLElement ? root.dataset.refreshToken : null;
        const sceneToken = nextCard.dataset.chartSceneToken;
        if (documentToken && sceneToken && documentToken !== sceneToken) {{
            stashViewportState();
            window.location.reload();
            return;
        }}
        card.replaceWith(nextCard);
        syncChartTabLinks();
        for (const detailsId of openDetails) {{
            const details = document.getElementById(detailsId);
            if (details instanceof HTMLDetailsElement) {{
                details.open = true;
            }}
        }}
    }} catch (error) {{
        if (error?.name !== "AbortError") {{
            card.setAttribute("aria-busy", "false");
            card.dataset.chartError = error?.message || "chart refresh failed";
        }}
    }} finally {{
        if (chartFragmentController === controller) {{
            chartFragmentController = null;
        }}
    }}
}}

function chartUrlFromForm(form) {{
    const url = new URL(form.action || window.location.href, document.baseURI);
    url.search = new URLSearchParams(new FormData(form)).toString();
    return url;
}}

function chartHitAtClientX(svg, clientX) {{
    let nearest = null;
    let nearestDistance = Number.POSITIVE_INFINITY;
    for (const hit of svg.querySelectorAll("[data-chart-hit=\"true\"]")) {{
        const bounds = hit.getBoundingClientRect();
        const distance = Math.abs(clientX - (bounds.left + bounds.right) / 2);
        if (distance < nearestDistance) {{
            nearest = hit;
            nearestDistance = distance;
        }}
    }}
    return nearest instanceof SVGAElement ? nearest : null;
}}

function chartMetadata(svg) {{
    const cached = chartMetadataCache.get(svg);
    if (cached) {{
        return cached;
    }}
    const raw = svg.querySelector("[data-chart-metadata=\"true\"]")?.textContent || "[]";
    let parsed = [[], []];
    try {{
        parsed = JSON.parse(raw);
    }} catch (_error) {{
        // Keep the experiment link usable even when optional value metadata is malformed.
    }}
    const metadata = {{
        labels: Array.isArray(parsed[0]) ? parsed[0] : [],
        values: new Map(Array.isArray(parsed[1]) ? parsed[1] : []),
    }};
    chartMetadataCache.set(svg, metadata);
    return metadata;
}}

function chartHitSlug(hit) {{
    try {{
        const path = new URL(hit.getAttribute("href") || "", document.baseURI).pathname;
        return decodeURIComponent(path.slice(path.lastIndexOf("/") + 1));
    }} catch (_error) {{
        return null;
    }}
}}

function cancelChartTooltipHide() {{
    if (chartTooltipHideTimer) {{
        clearTimeout(chartTooltipHideTimer);
        chartTooltipHideTimer = null;
    }}
}}

function hideChartTooltip() {{
    cancelChartTooltipHide();
    const tooltip = chartCard()?.querySelector("[data-chart-tooltip=\"true\"]");
    if (tooltip instanceof HTMLElement) {{
        tooltip.hidden = true;
        tooltip.replaceChildren();
    }}
}}

function scheduleChartTooltipHide() {{
    cancelChartTooltipHide();
    chartTooltipHideTimer = setTimeout(hideChartTooltip, 180);
}}

function showChartTooltip(hit, clientX, clientY) {{
    cancelChartTooltipHide();
    const ordinal = hit.dataset.ordinal;
    const frame = hit.closest(".chart-frame");
    const tooltip = frame?.querySelector("[data-chart-tooltip=\"true\"]");
    const svg = hit.closest("svg[data-chart-navigator=\"true\"]");
    if (!(frame instanceof HTMLElement) || !(tooltip instanceof HTMLElement)
        || !(svg instanceof SVGSVGElement)) {{
        return;
    }}
    const title = document.createElement("a");
    title.className = "chart-tooltip-title";
    title.href = hit.getAttribute("href") || "";
    title.textContent = hit.querySelector("title")?.textContent || "Experiment";
    const meta = document.createElement("div");
    meta.className = "chart-tooltip-meta";
    for (const value of [
        ordinal ? "ordinal " + ordinal : null,
    ]) {{
        if (!value) continue;
        const item = document.createElement("span");
        item.textContent = value;
        meta.append(item);
    }}
    const values = document.createElement("dl");
    values.className = "chart-tooltip-values";
    const metadata = chartMetadata(svg);
    const labels = metadata.labels;
    const renderedValues = metadata.values.get(Number(ordinal)) || [];
    for (const [index, label] of labels.entries()) {{
        const value = renderedValues[index];
        if (typeof label !== "string" || typeof value !== "string") continue;
        const term = document.createElement("dt");
        const definition = document.createElement("dd");
        term.textContent = label;
        definition.textContent = value;
        values.append(term, definition);
    }}
    tooltip.replaceChildren(title, meta, values);
    tooltip.hidden = false;
    const frameBounds = frame.getBoundingClientRect();
    const hitBounds = hit.getBoundingClientRect();
    const anchorX = Number.isFinite(clientX) ? clientX : (hitBounds.left + hitBounds.right) / 2;
    const anchorY = Number.isFinite(clientY) ? clientY : hitBounds.top;
    const left = anchorX - frameBounds.left + frame.scrollLeft + 12;
    const top = anchorY - frameBounds.top + frame.scrollTop + 12;
    tooltip.style.left = left + "px";
    tooltip.style.top = top + "px";
    const tooltipBounds = tooltip.getBoundingClientRect();
    if (tooltipBounds.right > frameBounds.right - 8) {{
        tooltip.style.left = Math.max(
            frame.scrollLeft + 8,
            left - tooltipBounds.width - 24
        ) + "px";
    }}
    if (tooltipBounds.bottom > frameBounds.bottom - 8) {{
        tooltip.style.top = Math.max(
            frame.scrollTop + 8,
            top - tooltipBounds.height - 24
        ) + "px";
    }}
    svg.dataset.activeOrdinal = ordinal || "";
}}

function positionChartRubberBand(drag, clientX) {{
    const frameBounds = drag.frame.getBoundingClientRect();
    const left = Math.min(drag.startX, clientX) - frameBounds.left + drag.frame.scrollLeft;
    const width = Math.abs(clientX - drag.startX);
    drag.band.hidden = false;
    drag.band.style.left = left + "px";
    drag.band.style.width = width + "px";
}}

function resetChartWindow() {{
    const url = new URL(window.location.href);
    url.searchParams.delete("plot_from");
    url.searchParams.delete("plot_to");
    commitChartUrl(url);
    requestChartFragment();
}}

function commitChartWindow(svg, startX, endX) {{
    const first = chartHitAtClientX(svg, startX);
    const last = chartHitAtClientX(svg, endX);
    const firstSlug = first ? chartHitSlug(first) : null;
    const lastSlug = last ? chartHitSlug(last) : null;
    if (!firstSlug || !lastSlug) {{
        return;
    }}
    const url = new URL(window.location.href);
    url.searchParams.set("plot_from", firstSlug);
    url.searchParams.set("plot_to", lastSlug);
    commitChartUrl(url);
    requestChartFragment();
}}

function inlineEditInput(form) {{
    const input = form.querySelector("[data-inline-edit-input=\"true\"]");
    return input instanceof HTMLInputElement ? input : null;
}}

function tableFilterStorageKey(filterName) {{
    return `spinner:table-filter:${{window.location.pathname}}${{window.location.search}}:${{filterName}}`;
}}

function restoreTableFilter(input) {{
    const filterName = input.dataset.tableFilterInput;
    if (!filterName) {{
        return;
    }}
    try {{
        const stored = window.sessionStorage.getItem(tableFilterStorageKey(filterName));
        if (stored !== null) {{
            input.value = stored;
        }}
    }} catch (_error) {{
        // Filter persistence is best-effort only.
    }}
}}

function storeTableFilter(input) {{
    const filterName = input.dataset.tableFilterInput;
    if (!filterName) {{
        return;
    }}
    try {{
        if (input.value) {{
            window.sessionStorage.setItem(tableFilterStorageKey(filterName), input.value);
        }} else {{
            window.sessionStorage.removeItem(tableFilterStorageKey(filterName));
        }}
    }} catch (_error) {{
        // Filter persistence is best-effort only.
    }}
}}

function openInlineEdit(form) {{
    const input = inlineEditInput(form);
    if (!input) {{
        return;
    }}
    const original = form.dataset.originalValue || input.defaultValue || input.value;
    form.dataset.originalValue = original;
    input.value = original;
    form.classList.add("editing");
    window.requestAnimationFrame(() => {{
        input.focus();
        input.select();
    }});
}}

function closeInlineEdit(form) {{
    const input = inlineEditInput(form);
    if (input) {{
        input.value = form.dataset.originalValue || input.defaultValue || "";
    }}
    form.classList.remove("editing");
}}

function prepareInlineEditSubmit(form, event) {{
    const input = inlineEditInput(form);
    if (!input) {{
        return;
    }}
    const original = form.dataset.originalValue || input.defaultValue || "";
    const next = input.value.trim();
    const allowClear = form.dataset.inlineEditAllowClear === "true";
    if ((!allowClear && !next) || next === original) {{
        event.preventDefault();
        closeInlineEdit(form);
        return;
    }}
    input.value = next;
}}

function tableFilterRows(filterName) {{
    return Array.from(document.querySelectorAll("[data-table-filter-row]"))
        .filter((row) => row instanceof HTMLTableRowElement && row.dataset.tableFilterRow === filterName);
}}

function tableFilterEmptyRows(filterName) {{
    return Array.from(document.querySelectorAll("[data-table-filter-empty]"))
        .filter((row) => row instanceof HTMLTableRowElement && row.dataset.tableFilterEmpty === filterName);
}}

function applyTableFilter(input) {{
    const filterName = input.dataset.tableFilterInput;
    if (!filterName) {{
        return;
    }}
    const query = input.value.trim().toLowerCase();
    let visibleCount = 0;
    for (const row of tableFilterRows(filterName)) {{
        const haystack = (row.dataset.tableFilterText || row.textContent || "").toLowerCase();
        const visible = !query || haystack.includes(query);
        row.hidden = !visible;
        if (visible) {{
            visibleCount += 1;
        }}
    }}
    for (const emptyRow of tableFilterEmptyRows(filterName)) {{
        emptyRow.hidden = !query || visibleCount > 0;
    }}
}}

function applyAllTableFilters() {{
    for (const input of document.querySelectorAll("[data-table-filter-input]")) {{
        if (input instanceof HTMLInputElement) {{
            restoreTableFilter(input);
            applyTableFilter(input);
        }}
    }}
}}

function syncSyntheticMetricExtras(form) {{
    const operation = form.querySelector("select[data-synthetic-operation-select]");
    if (!(operation instanceof HTMLSelectElement)) {{
        return;
    }}
    const showExtras = operation.value === "gmean";
    for (const extra of form.querySelectorAll("[data-synthetic-gmean-extra]")) {{
        if (extra instanceof HTMLInputElement) {{
            extra.hidden = !showExtras;
        }}
    }}
}}

function syncAllSyntheticMetricExtras() {{
    for (const operation of document.querySelectorAll("select[data-synthetic-operation-select]")) {{
        const form = operation.closest("form");
        if (form instanceof HTMLFormElement) {{
            syncSyntheticMetricExtras(form);
        }}
    }}
}}

window.setInterval(pollRefreshToken, AUTO_REFRESH_INTERVAL_MS);
window.addEventListener("focus", pollRefreshToken);
window.addEventListener("popstate", () => {{
    if (chartCard()) {{
        requestChartFragment();
    }}
}});
document.addEventListener("visibilitychange", () => {{
    if (!document.hidden) {{
        pollRefreshToken();
    }}
}});
pollRefreshToken();
applyAllTableFilters();
syncAllSyntheticMetricExtras();

document.addEventListener("pointerover", (event) => {{
    const target = event.target;
    if (!(target instanceof Element)) {{
        return;
    }}
    if (target.closest("[data-chart-tooltip=\"true\"]")) {{
        cancelChartTooltipHide();
        return;
    }}
    const hit = target.closest("[data-chart-hit=\"true\"]");
    if (hit instanceof Element) {{
        showChartTooltip(hit, event.clientX, event.clientY);
    }}
}});

document.addEventListener("pointerout", (event) => {{
    const target = event.target;
    if (!(target instanceof Element)) {{
        return;
    }}
    if (!target.closest("[data-chart-hit=\"true\"], [data-chart-tooltip=\"true\"]")) {{
        return;
    }}
    const related = event.relatedTarget;
    if (related instanceof Element
        && related.closest("[data-chart-hit=\"true\"], [data-chart-tooltip=\"true\"]")) {{
        return;
    }}
    scheduleChartTooltipHide();
}});

document.addEventListener("focusin", (event) => {{
    const target = event.target;
    if (!(target instanceof Element)) {{
        return;
    }}
    const hit = target.closest("[data-chart-hit=\"true\"]");
    if (hit instanceof Element) {{
        showChartTooltip(hit, Number.NaN, Number.NaN);
    }}
}});

document.addEventListener("pointerdown", (event) => {{
    const target = event.target;
    if (!(target instanceof Element) || event.button !== 0 || !event.isPrimary) {{
        return;
    }}
    const hit = target.closest("[data-chart-hit=\"true\"]");
    const svg = target.closest("svg[data-chart-navigator=\"true\"]");
    const frame = svg?.closest(".chart-frame");
    const band = frame?.querySelector("[data-chart-rubber-band=\"true\"]");
    if (!(hit instanceof Element) || !(svg instanceof SVGSVGElement)
        || !(frame instanceof HTMLElement) || !(band instanceof HTMLElement)) {{
        return;
    }}
    chartDrag = {{
        svg,
        frame,
        band,
        pointerId: event.pointerId,
        startX: event.clientX,
        endX: event.clientX,
        moved: false,
    }};
    svg.setPointerCapture(event.pointerId);
}});

document.addEventListener("pointermove", (event) => {{
    if (!chartDrag || chartDrag.pointerId !== event.pointerId) {{
        return;
    }}
    chartDrag.endX = event.clientX;
    chartDrag.moved ||= Math.abs(chartDrag.endX - chartDrag.startX) >= 6;
    if (chartDrag.moved) {{
        event.preventDefault();
        positionChartRubberBand(chartDrag, chartDrag.endX);
    }}
}});

document.addEventListener("pointerup", (event) => {{
    if (!chartDrag || chartDrag.pointerId !== event.pointerId) {{
        return;
    }}
    const drag = chartDrag;
    chartDrag = null;
    drag.band.hidden = true;
    if (drag.svg.hasPointerCapture(event.pointerId)) {{
        drag.svg.releasePointerCapture(event.pointerId);
    }}
    if (drag.moved) {{
        event.preventDefault();
        suppressChartClickUntil = performance.now() + 350;
        commitChartWindow(drag.svg, drag.startX, event.clientX);
    }}
}});

document.addEventListener("pointercancel", (event) => {{
    if (!chartDrag || chartDrag.pointerId !== event.pointerId) {{
        return;
    }}
    chartDrag.band.hidden = true;
    chartDrag = null;
}});

document.addEventListener("click", (event) => {{
    const target = event.target;
    if (!(target instanceof Element)) {{
        return;
    }}
    if (performance.now() < suppressChartClickUntil
        && target.closest("[data-chart-hit=\"true\"]")) {{
        event.preventDefault();
        return;
    }}
    const resetWindowButton = target.closest("button[data-chart-reset-window=\"true\"]");
    if (resetWindowButton instanceof HTMLButtonElement) {{
        resetChartWindow();
        return;
    }}
    const chartLink = target.closest("[id=\"metric-plot-card\"] a[data-preserve-viewport=\"true\"]");
    if (chartLink instanceof HTMLAnchorElement
        && event.button === 0
        && !event.metaKey
        && !event.ctrlKey
        && !event.shiftKey
        && !event.altKey) {{
        const url = new URL(chartLink.href, document.baseURI);
        if (url.pathname === window.location.pathname
            && url.searchParams.get("tab") === "results") {{
            event.preventDefault();
            commitChartUrl(url);
            requestChartFragment();
            return;
        }}
    }}
    const copyButton = target.closest("button[data-copy-plot-png=\"true\"]");
    if (copyButton instanceof HTMLButtonElement) {{
        copyButton.disabled = true;
        setPlotCopyButtonState(copyButton, "Copying...", "busy");
        copyPlotPng(copyButton)
            .then(() => {{
                setPlotCopyButtonState(copyButton, "Copied", "copied");
            }})
            .catch((error) => {{
                setPlotCopyButtonState(
                    copyButton,
                    "Copy failed",
                    "failed",
                    error?.message || "Copy failed"
                );
            }})
            .finally(() => {{
                copyButton.disabled = false;
                schedulePlotCopyReset(copyButton);
        }});
        return;
    }}
    const editButton = target.closest("button[data-inline-edit-trigger=\"true\"]");
    if (editButton instanceof HTMLButtonElement) {{
        const form = editButton.closest("form[data-inline-edit-form=\"true\"]");
        if (form instanceof HTMLFormElement) {{
            openInlineEdit(form);
        }}
        return;
    }}
    for (const editForm of document.querySelectorAll("form[data-inline-edit-form=\"true\"].editing")) {{
        if (!editForm.contains(target) && editForm instanceof HTMLFormElement) {{
            closeInlineEdit(editForm);
        }}
    }}
    const navigationLink = target.closest("a[data-preserve-viewport=\"true\"]");
    if (
        navigationLink instanceof HTMLAnchorElement
        && event.button === 0
        && !event.defaultPrevented
        && !event.metaKey
        && !event.ctrlKey
        && !event.shiftKey
        && !event.altKey
        && (!navigationLink.target || navigationLink.target === "_self")
    ) {{
        stashViewportState();
    }}
    for (const popout of document.querySelectorAll("details.control-popout[open]")) {{
        if (!popout.contains(target)) {{
            popout.removeAttribute("open");
        }}
    }}
}});

document.addEventListener("submit", (event) => {{
    const target = event.target;
    if (!(target instanceof HTMLFormElement)) {{
        return;
    }}
    if (target.closest("[id=\"metric-plot-card\"]")
        && target.method.toLowerCase() === "get") {{
        event.preventDefault();
        commitChartUrl(chartUrlFromForm(target));
        requestChartFragment();
        return;
    }}
    if (target.hasAttribute("data-inline-edit-form")) {{
        prepareInlineEditSubmit(target, event);
        if (event.defaultPrevented) {{
            return;
        }}
    }}
    if (!target.hasAttribute("data-preserve-viewport")) {{
        return;
    }}
    stashViewportState();
}});

document.addEventListener("keydown", (event) => {{
    const target = event.target;
    const chartSvg = target instanceof Element
        ? target.closest("svg[data-chart-navigator=\"true\"]")
        : null;
    if (chartSvg instanceof SVGSVGElement) {{
        const hits = Array.from(chartSvg.querySelectorAll("[data-chart-hit=\"true\"]"));
        const activeOrdinal = chartSvg.dataset.activeOrdinal;
        let index = hits.findIndex((hit) => hit.dataset.ordinal === activeOrdinal);
        if (event.key === "ArrowLeft" || event.key === "ArrowRight") {{
            event.preventDefault();
            const delta = event.key === "ArrowLeft" ? -1 : 1;
            index = Math.max(0, Math.min(hits.length - 1, index < 0 ? 0 : index + delta));
            const hit = hits[index];
            if (hit instanceof Element) {{
                showChartTooltip(hit, Number.NaN, Number.NaN);
            }}
            return;
        }}
        if (event.key === "Enter" && index >= 0) {{
            const hit = hits[index];
            if (hit instanceof SVGAElement) {{
                window.location.assign(
                    new URL(hit.getAttribute("href") || "", document.baseURI)
                );
            }}
            return;
        }}
        if (event.key === "Escape") {{
            event.preventDefault();
            hideChartTooltip();
            if (new URL(window.location.href).searchParams.has("plot_from")) {{
                resetChartWindow();
            }}
            return;
        }}
    }}
    if (
        target instanceof HTMLInputElement
        && target.hasAttribute("data-inline-edit-input")
    ) {{
        const form = target.closest("form[data-inline-edit-form=\"true\"]");
        if (form instanceof HTMLFormElement && event.key === "Escape") {{
            event.preventDefault();
            closeInlineEdit(form);
            return;
        }}
        if (form instanceof HTMLFormElement && event.key === "Enter") {{
            prepareInlineEditSubmit(form, event);
        }}
    }}
    if (event.key !== "Escape") {{
        return;
    }}
    for (const popout of document.querySelectorAll("details.control-popout[open]")) {{
        popout.removeAttribute("open");
    }}
}});

document.addEventListener("change", (event) => {{
    const target = event.target;
    if (!(target instanceof HTMLElement)) {{
        return;
    }}
    if (target instanceof HTMLInputElement
        && target.hasAttribute("data-chart-series-toggle")) {{
        const key = target.value;
        for (const group of chartRootSvg()?.querySelectorAll("[data-chart-series]") || []) {{
            if (group.getAttribute("data-chart-series") === key) {{
                if (target.checked) {{
                    group.removeAttribute("display");
                }} else {{
                    group.setAttribute("display", "none");
                }}
            }}
        }}
        const url = new URL(window.location.href);
        url.searchParams.delete("hidden_metric");
        for (const toggle of chartCard()?.querySelectorAll("[data-chart-series-toggle]") || []) {{
            if (toggle instanceof HTMLInputElement && !toggle.checked) {{
                url.searchParams.append("hidden_metric", toggle.value);
            }}
        }}
        commitChartUrl(url);
        requestChartFragment();
        return;
    }}
    if (target instanceof HTMLSelectElement && target.hasAttribute("data-synthetic-operation-select")) {{
        const form = target.closest("form");
        if (form instanceof HTMLFormElement) {{
            syncSyntheticMetricExtras(form);
        }}
    }}
    if (!target.hasAttribute("data-auto-submit")) {{
        return;
    }}
    const form = target.closest("form");
    if (!(form instanceof HTMLFormElement)) {{
        return;
    }}
    form.requestSubmit();
}});

document.addEventListener("input", (event) => {{
    const target = event.target;
    if (target instanceof HTMLInputElement && target.hasAttribute("data-table-filter-input")) {{
        applyTableFilter(target);
        storeTableFilter(target);
    }}
}});
"#
    )
}
