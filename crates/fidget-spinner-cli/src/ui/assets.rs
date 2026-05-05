use super::UI_NAV_STATE_KEY;

pub(super) fn harden_autofill_controls(document: String) -> String {
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

window.setInterval(pollRefreshToken, AUTO_REFRESH_INTERVAL_MS);
window.addEventListener("focus", pollRefreshToken);
document.addEventListener("visibilitychange", () => {{
    if (!document.hidden) {{
        pollRefreshToken();
    }}
}});
pollRefreshToken();
applyAllTableFilters();

document.addEventListener("click", (event) => {{
    const target = event.target;
    if (!(target instanceof Element)) {{
        return;
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

pub(super) fn styles() -> &'static str {
    r#"
    :root {
        color-scheme: light;
        --bg: #faf5ec;
        --panel: #fffaf2;
        --panel-2: #f6eee1;
        --border: #dfd1bd;
        --border-strong: #cfbea8;
        --text: #241d16;
        --muted: #6f6557;
        --accent: #67563f;
        --accent-soft: #ece2d2;
        --tag: #efe5d7;
        --accepted: #47663f;
        --kept: #5a6952;
        --parked: #8a6230;
        --rejected: #8a3a34;
        --shadow: rgba(83, 61, 33, 0.055);
    }
    * { box-sizing: border-box; }
    body {
        margin: 0;
        background: var(--bg);
        color: var(--text);
        font: 15px/1.55 "Iosevka Web", "IBM Plex Mono", "SFMono-Regular", monospace;
        overflow-x: hidden;
    }
    a {
        color: var(--accent);
        text-decoration: none;
    }
    a:hover { text-decoration: underline; }
    .shell {
        width: 100%;
        max-width: none;
        margin: 0 auto;
        padding: 18px 20px 34px;
        display: grid;
        gap: 16px;
        grid-template-columns: 280px minmax(0, 1fr);
        align-items: start;
        min-width: 0;
        overflow-x: clip;
    }
    .sidebar {
        position: sticky;
        top: 18px;
        min-width: 0;
    }
    .sidebar-panel {
        border: 1px solid var(--border);
        background: var(--panel);
        padding: 14px;
        display: grid;
        gap: 12px;
        box-shadow: 0 1px 0 var(--shadow);
    }
    .sidebar-project {
        display: grid;
        gap: 7px;
    }
    .sidebar-title-row {
        display: flex;
        gap: 8px;
        align-items: baseline;
        justify-content: space-between;
        min-width: 0;
    }
    .sidebar-home {
        color: var(--text);
        font-size: 18px;
        font-weight: 700;
        min-width: 0;
        overflow-wrap: anywhere;
    }
    .sidebar-home-chip {
        flex: 0 0 auto;
        border: 1px solid var(--border);
        background: var(--panel-2);
        color: var(--accent);
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.05em;
        padding: 2px 6px;
        text-transform: uppercase;
    }
    .sidebar-tags {
        padding: 3px 7px;
        border: 1px solid var(--border);
        background: var(--panel-2);
        color: var(--accent);
        font-size: 12px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .sidebar-actions {
        display: inline-flex;
        gap: 6px;
        align-items: center;
        flex-wrap: wrap;
    }
    .sidebar-copy {
        margin: 0;
        color: var(--muted);
        font-size: 13px;
        line-height: 1.5;
    }
    .sidebar-section {
        display: grid;
        gap: 10px;
    }
    .frontier-nav {
        display: grid;
        gap: 8px;
    }
    .frontier-nav-item {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto;
        gap: 6px;
        align-items: stretch;
        min-width: 0;
    }
    .frontier-nav-link {
        display: grid;
        gap: 4px;
        padding: 10px 12px;
        border: 1px solid var(--border);
        background: var(--panel-2);
        min-width: 0;
    }
    .frontier-nav-link.active {
        border-color: var(--border-strong);
        background: var(--accent-soft);
    }
    .frontier-nav-title {
        color: var(--text);
        font-weight: 700;
    }
    .frontier-nav-meta {
        color: var(--muted);
        font-size: 12px;
    }
    .frontier-action-form {
        display: grid;
        margin: 0;
        align-self: stretch;
    }
    .frontier-action-button {
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        cursor: pointer;
        display: grid;
        place-items: center;
        height: 100%;
        min-width: 30px;
        padding: 0;
        user-select: none;
    }
    .frontier-action-button:hover {
        color: var(--text);
        border-color: var(--border-strong);
    }
    .frontier-action-icon {
        width: 17px;
        height: 17px;
        stroke: currentColor;
        stroke-width: 1.8;
        stroke-linecap: round;
        stroke-linejoin: round;
    }
    .frontier-heading {
        gap: 8px;
    }
    .frontier-title-row {
        display: flex;
        gap: 8px;
        align-items: flex-start;
        justify-content: space-between;
        min-width: 0;
    }
    .frontier-title-row h1 {
        flex: 1 1 auto;
    }
    .frontier-summary-editor {
        flex: 0 0 auto;
    }
    .frontier-edit-toggle {
        list-style: none;
    }
    .frontier-edit-toggle::-webkit-details-marker {
        display: none;
    }
    .frontier-summary-panel {
        width: min(620px, calc(100vw - 80px));
    }
    .frontier-summary-form {
        display: grid;
        gap: 10px;
    }
    .frontier-title-input,
    .frontier-description-input {
        width: 100%;
        max-width: none;
    }
    .sidebar-archived {
        display: grid;
        gap: 8px;
    }
    .sidebar-archived-toggle {
        color: var(--muted);
        cursor: pointer;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        user-select: none;
    }
    .sidebar-archived-list {
        margin-top: 8px;
    }
    .main-column {
        display: grid;
        gap: 12px;
        min-width: 0;
    }
    .tag-family-grid {
        display: grid;
        gap: 10px;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }
    .tag-create-form,
    .tag-inline-form {
        display: flex;
        gap: 6px;
        align-items: center;
        flex-wrap: wrap;
        margin: 0;
    }
    .family-policy-row {
        display: inline-flex;
        gap: 6px;
        align-items: center;
        flex-wrap: wrap;
        justify-content: flex-end;
    }
    .tag-identity-row,
    .tag-icon-form,
    .tag-inline-rename-form {
        display: inline-flex;
        gap: 5px;
        align-items: center;
        min-width: 0;
        margin: 0;
    }
    .tag-inline-rename-form {
        gap: 4px;
    }
    .metric-identity-stack {
        display: grid;
        gap: 4px;
        min-width: 0;
        white-space: normal;
    }
    .metric-name-form {
        white-space: nowrap;
    }
    .metric-name-row {
        display: inline-flex;
        gap: 6px;
        align-items: center;
        min-width: 0;
        flex-wrap: wrap;
    }
    .metric-objective-chip {
        display: inline-grid;
        place-items: center;
        min-width: 3.4ch;
        height: 20px;
        padding: 0 5px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        font-size: 10px;
        font-weight: 700;
        letter-spacing: 0.06em;
    }
    .metric-objective-maximize {
        color: color-mix(in srgb, var(--accepted) 70%, var(--muted));
    }
    .metric-objective-minimize {
        color: color-mix(in srgb, var(--rejected) 55%, var(--muted));
    }
    .metric-description-form {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto;
        align-items: start;
        width: 100%;
        min-width: 0;
    }
    .metric-description-form [data-inline-edit-label="true"] {
        min-width: 0;
        white-space: normal;
        overflow-wrap: anywhere;
    }
    .metric-description-form .inline-icon-button {
        align-self: start;
    }
    .metric-description-form.editing .inline-rename-input {
        display: block;
        width: 100%;
        max-width: none;
    }
    .kpi-table {
        table-layout: fixed;
    }
    .kpi-action-col,
    .kpi-unit-col,
    .kpi-obs-col {
        width: 1%;
    }
    .kpi-metric-col {
        width: auto;
    }
    .kpi-action-row {
        display: flex;
        gap: 3px;
        align-items: flex-start;
    }
    .kpi-metric-cell,
    .kpi-reference-lane {
        white-space: normal !important;
        overflow-wrap: anywhere !important;
    }
    .kpi-metric-stack {
        display: grid;
        gap: 4px;
        min-width: 0;
    }
    .kpi-description {
        max-width: min(86ch, 100%);
        line-height: 1.35;
        white-space: normal;
        overflow-wrap: anywhere;
    }
    .kpi-reference-row td {
        border-top: 0;
        padding-top: 0;
    }
    .kpi-reference-gutter {
        padding: 0 !important;
    }
    .kpi-reference-lane {
        padding-bottom: 9px !important;
    }
    .kpi-reference-band {
        display: flex;
        gap: 8px;
        align-items: flex-start;
        flex-wrap: wrap;
        min-width: 0;
    }
    .kpi-reference-heading {
        color: var(--muted);
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        padding-top: 5px;
        flex: 0 0 auto;
    }
    .kpi-reference-stack,
    .kpi-reference-chip-row,
    .kpi-reference-form {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        flex-wrap: wrap;
        min-width: 0;
    }
    .kpi-reference-stack {
        flex: 1 1 38ch;
    }
    .kpi-reference-chip {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        flex-wrap: nowrap;
        min-width: 0;
    }
    .kpi-reference-chip {
        width: fit-content;
        max-width: 100%;
        padding: 2px 3px 2px 7px;
        border: 1px solid var(--border);
        background: var(--panel);
    }
    .kpi-reference-label {
        font-weight: 700;
        max-width: min(38ch, 42vw);
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .kpi-reference-value {
        color: var(--muted);
        white-space: nowrap;
    }
    .kpi-reference-label-input {
        width: min(22ch, 30vw);
    }
    .kpi-reference-value-input {
        width: 9ch;
    }
    .kpi-reference-unit-input {
        width: 12ch;
    }
    .inline-icon-button {
        display: grid;
        place-items: center;
        width: 24px;
        height: 24px;
        flex: 0 0 24px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        cursor: pointer;
        padding: 0;
    }
    .inline-icon-button:hover {
        border-color: var(--border-strong);
        color: var(--text);
    }
    .danger-icon-button {
        color: var(--rejected);
    }
    .inline-action-icon {
        width: 14px;
        height: 14px;
        stroke: currentColor;
        stroke-width: 1.8;
        stroke-linecap: round;
        stroke-linejoin: round;
    }
    .inline-rename-input {
        display: none;
        width: min(240px, 42vw);
    }
    .tag-inline-rename-form.editing [data-inline-edit-label="true"],
    .tag-inline-rename-form.editing .inline-icon-button {
        display: none;
    }
    .tag-inline-rename-form.editing .inline-rename-input {
        display: inline-block;
    }
    .compact-input,
    .compact-select,
    .compact-textarea,
    .inline-rename-input {
        min-width: 0;
        max-width: 180px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--text);
        font: inherit;
        font-size: 12px;
        padding: 5px 7px;
    }
    .compact-select {
        max-width: 150px;
    }
    .compact-textarea {
        max-width: none;
        min-height: 92px;
        resize: vertical;
    }
    .wide-compact-select {
        max-width: 360px;
        width: min(360px, 64vw);
    }
    .wide-compact-input {
        max-width: 280px;
        width: min(280px, 42vw);
    }
    .form-button {
        border: 1px solid var(--border);
        background: var(--panel-2);
        color: var(--accent);
        cursor: pointer;
        font: inherit;
        font-size: 11px;
        font-weight: 700;
        padding: 5px 7px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .form-button:hover {
        border-color: var(--border-strong);
        color: var(--text);
    }
    .form-button:disabled,
    .inline-icon-button:disabled,
    .compact-input:disabled,
    .compact-select:disabled,
    .compact-textarea:disabled {
        cursor: not-allowed;
        opacity: 0.48;
    }
    .danger-button {
        color: var(--rejected);
    }
    .inline-check {
        display: inline-flex;
        gap: 5px;
        align-items: center;
        color: var(--muted);
        font-size: 12px;
    }
    .table-wrap {
        width: 100%;
        overflow-x: auto;
    }
    .dense-table {
        width: 100%;
        border-collapse: collapse;
        table-layout: auto;
    }
    .dense-table th,
    .dense-table td {
        border-bottom: 1px solid var(--border);
        padding: 7px 8px;
        text-align: left;
        vertical-align: top;
    }
    .dense-table th {
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .dense-table td {
        overflow-wrap: anywhere;
    }
    .dense-table .no-truncate {
        white-space: nowrap;
        overflow-wrap: normal;
    }
    .tag-history-list {
        display: grid;
        gap: 7px;
    }
    .tag-history-row {
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
    }
    .page-header {
        display: grid;
        gap: 6px;
        padding: 12px 14px;
        border: 1px solid var(--border);
        background: var(--panel);
        box-shadow: 0 1px 0 var(--shadow);
        min-width: 0;
    }
    .eyebrow {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        color: var(--muted);
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .sep { color: #a08d70; }
    .page-title {
        margin: 0;
        font-size: clamp(18px, 1.9vw, 24px);
        line-height: 1.15;
        overflow-wrap: anywhere;
        word-break: break-word;
    }
    .page-subtitle {
        margin: 0;
        color: var(--muted);
        max-width: 90ch;
        overflow-wrap: anywhere;
    }
    .tab-row {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
    }
    .tab-chip {
        display: inline-flex;
        align-items: center;
        padding: 8px 12px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .tab-chip.active {
        color: var(--text);
        border-color: var(--border-strong);
        background: var(--accent-soft);
        font-weight: 700;
    }
    .card {
        border: 1px solid var(--border);
        background: var(--panel);
        padding: 14px 16px;
        display: grid;
        gap: 10px;
        box-shadow: 0 1px 0 var(--shadow);
        min-width: 0;
    }
    .subcard {
        border: 1px solid var(--border);
        background: var(--panel-2);
        padding: 10px 12px;
        display: grid;
        gap: 8px;
        min-width: 0;
        align-content: start;
    }
    .compact-subcard {
        justify-items: start;
    }
    .block { display: grid; gap: 10px; }
    .stack {
        display: grid;
        gap: 14px;
    }
    .split {
        display: grid;
        gap: 16px;
        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
        align-items: start;
    }
    .card-grid {
        display: grid;
        gap: 12px;
        grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
        align-items: start;
    }
    .mini-card {
        border: 1px solid var(--border);
        background: var(--panel-2);
        padding: 12px 14px;
        display: grid;
        gap: 9px;
        min-width: 0;
        align-content: start;
        overflow: hidden;
    }
    .frontier-card-header {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto;
        gap: 8px;
        align-items: start;
        min-width: 0;
    }
    .frontier-card-title {
        color: var(--text);
        display: -webkit-box;
        font-size: 16px;
        font-weight: 700;
        line-height: 1.25;
        min-width: 0;
        overflow: hidden;
        overflow-wrap: anywhere;
        text-overflow: ellipsis;
        -webkit-box-orient: vertical;
        -webkit-line-clamp: 2;
    }
    .frontier-card-title:hover {
        text-decoration: underline;
    }
    .frontier-card-status {
        justify-self: end;
        max-width: 100%;
    }
    .frontier-card-objective {
        display: -webkit-box;
        margin: 0;
        max-width: 100%;
        overflow: hidden;
        overflow-wrap: anywhere;
        white-space: normal;
        -webkit-box-orient: vertical;
        -webkit-line-clamp: 4;
    }
    .frontier-card .meta-row {
        min-width: 0;
    }
    .frontier-card .meta-row span {
        min-width: 0;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .card-header {
        display: flex;
        gap: 10px;
        align-items: flex-start;
        flex-wrap: wrap;
    }
    .title-link {
        font-size: 16px;
        font-weight: 700;
        color: var(--text);
        overflow-wrap: anywhere;
        word-break: break-word;
        flex: 1 1 auto;
        min-width: 0;
    }
    h1, h2, h3 {
        margin: 0;
        line-height: 1.15;
        overflow-wrap: anywhere;
        word-break: break-word;
        min-width: 0;
    }
    h1 { font-size: 18px; }
    h2 { font-size: 16px; }
    h3 { font-size: 13px; color: #4f473a; }
    .prose {
        margin: 0;
        color: var(--text);
        max-width: 92ch;
        white-space: pre-wrap;
    }
    .muted { color: var(--muted); }
    .meta-row {
        display: flex;
        flex-wrap: wrap;
        gap: 6px 12px;
        align-items: center;
        font-size: 13px;
    }
    .kv-grid {
        display: grid;
        gap: 6px 12px;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
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
    .fact-strip {
        display: flex;
        flex-wrap: wrap;
        gap: 6px 16px;
        align-items: center;
        min-width: 0;
    }
    .tag-state-card {
        padding-block: 14px;
    }
    .tag-state-band {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 10px 18px;
        min-width: 0;
        flex-wrap: wrap;
    }
    .tag-state-controls {
        display: inline-flex;
        align-items: center;
        justify-content: flex-end;
        gap: 8px;
        flex-wrap: wrap;
        margin-left: auto;
    }
    .tag-lock-switch-form {
        margin: 0;
    }
    .tag-lock-switch {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        cursor: pointer;
        font: inherit;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.05em;
        padding: 4px 7px;
        text-transform: uppercase;
        white-space: nowrap;
    }
    .tag-lock-switch:hover {
        border-color: var(--border-strong);
        color: var(--text);
    }
    .tag-lock-switch.locked {
        border-color: color-mix(in srgb, var(--rejected) 45%, var(--border));
        color: var(--rejected);
        background: color-mix(in srgb, var(--rejected) 8%, var(--panel));
    }
    .switch-track {
        position: relative;
        width: 24px;
        height: 12px;
        border: 1px solid currentColor;
        background: var(--panel-2);
    }
    .switch-thumb {
        position: absolute;
        top: 2px;
        left: 2px;
        width: 6px;
        height: 6px;
        background: currentColor;
    }
    .tag-lock-switch.locked .switch-thumb {
        left: 14px;
    }
    .switch-state {
        color: var(--muted);
    }
    .fact {
        display: inline-flex;
        gap: 5px;
        align-items: baseline;
        min-width: 0;
        white-space: nowrap;
    }
    .fact-label {
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .fact-value {
        min-width: 0;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .outcome-header {
        align-items: center;
        justify-content: space-between;
    }
    .outcome-verdict-strip {
        margin-left: auto;
    }
    .narrative-block {
        background: color-mix(in srgb, var(--panel-2) 70%, var(--panel));
    }
    .provenance-disclosure {
        align-content: start;
    }
    .provenance-summary {
        display: flex;
        gap: 8px 14px;
        align-items: center;
        justify-content: space-between;
        cursor: pointer;
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        user-select: none;
    }
    .provenance-summary::-webkit-details-marker {
        display: none;
    }
    .provenance-summary::before {
        content: ">";
        color: var(--accent);
    }
    .provenance-disclosure[open] > .provenance-summary::before {
        content: "v";
    }
    .provenance-summary-facts {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        justify-content: flex-end;
        color: var(--muted);
        text-transform: none;
        letter-spacing: normal;
    }
    .provenance-body {
        display: grid;
        gap: 10px;
    }
    .provenance-block {
        display: grid;
        gap: 8px;
        min-width: 0;
    }
    .chip-row, .link-list {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        align-items: flex-start;
        align-content: flex-start;
        justify-content: flex-start;
    }
    .tag-cloud { max-width: 100%; }
    .tag-chip, .kind-chip, .status-chip, .metric-pill {
        display: inline-flex;
        align-items: center;
        flex: 0 0 auto;
        width: auto;
        max-width: 100%;
        border: 1px solid var(--border-strong);
        background: var(--tag);
        padding: 4px 8px;
        font-size: 12px;
        line-height: 1.2;
        white-space: nowrap;
    }
    .plot-card-header {
        align-items: center;
    }
    .plot-toolbar {
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
        margin-left: auto;
    }
    .control-popout {
        position: relative;
    }
    .control-popout[open] {
        z-index: 4;
    }
    .control-popout-toggle {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 7px 11px;
        border: 1px solid var(--border);
        background: var(--panel-2);
        color: var(--text);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        cursor: pointer;
        list-style: none;
        user-select: none;
    }
    .control-popout-toggle::-webkit-details-marker {
        display: none;
    }
    .control-popout[open] > .control-popout-toggle {
        border-color: var(--border-strong);
        background: var(--accent-soft);
    }
    .control-popout-panel {
        position: absolute;
        top: calc(100% + 8px);
        right: 0;
        width: min(520px, calc(100vw - 80px));
        max-height: min(72vh, 640px);
        overflow-y: auto;
        border: 1px solid var(--border-strong);
        background: var(--panel);
        padding: 14px 16px;
        display: grid;
        gap: 12px;
        box-shadow: 0 16px 36px rgba(83, 61, 33, 0.16);
    }
    .metric-popout-panel {
        width: min(760px, calc(100vw - 80px));
    }
    .metric-picker-form,
    .metric-picker-groups {
        display: grid;
        gap: 12px;
    }
    .metric-popout-layout {
        display: grid;
        gap: 14px;
        grid-template-columns: minmax(0, 1.6fr) minmax(180px, 0.8fr);
        align-items: start;
    }
    .metric-picker-main,
    .metric-picker-sidecar {
        display: grid;
        gap: 10px;
    }
    .metric-picker-group {
        display: grid;
        gap: 8px;
    }
    .metric-picker-group h4,
    .metric-picker-sidecar h4 {
        margin: 0;
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .metric-picker-disclosure {
        display: grid;
        gap: 8px;
    }
    .metric-picker-disclosure-toggle {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        cursor: pointer;
        user-select: none;
    }
    .metric-picker-list {
        display: grid;
        gap: 6px;
    }
    .metric-checkbox-row {
        display: grid;
        grid-template-columns: auto minmax(0, 1fr);
        gap: 8px;
        align-items: center;
        padding: 6px 9px;
        border: 1px solid var(--border);
        background: var(--panel-2);
        min-width: 0;
    }
    .metric-checkbox-row:hover {
        text-decoration: none;
        border-color: var(--border-strong);
    }
    .metric-checkbox-row.selected {
        border-color: var(--border-strong);
        background: var(--accent-soft);
    }
    .metric-checkbox-row.incompatible {
        opacity: 0.55;
    }
    .metric-checkbox-row input {
        margin: 0;
    }
    .metric-checkbox-copy {
        display: block;
        min-width: 0;
    }
    .metric-checkbox-title {
        color: var(--text);
        font-weight: 700;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .metric-checkbox-row-compact {
        align-self: start;
    }
    .compact-note {
        margin: 0;
        font-size: 12px;
    }
    .filter-form {
        display: grid;
        gap: 12px;
    }
    .filter-form-grid {
        display: grid;
        gap: 10px 12px;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }
    .filter-control {
        display: grid;
        gap: 6px;
        min-width: 0;
    }
    .filter-label {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .filter-select {
        width: 100%;
        min-width: 0;
        padding: 7px 9px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--text);
        font: inherit;
    }
    .filter-actions {
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
    }
    .filter-apply {
        padding: 7px 11px;
        border: 1px solid var(--border-strong);
        background: var(--accent-soft);
        color: var(--text);
        font: inherit;
        cursor: pointer;
    }
    .metric-filter-chip {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        padding: 5px 9px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--text);
        font-size: 12px;
        white-space: nowrap;
    }
    .metric-filter-chip.active {
        border-color: var(--border-strong);
        background: var(--accent-soft);
        font-weight: 700;
    }
    .clear-filter {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .link-chip {
        display: inline-grid;
        gap: 4px;
        align-content: start;
        max-width: min(100%, 72ch);
        padding: 8px 10px;
        border: 1px solid var(--border);
        background: var(--panel);
        min-width: 0;
    }
    .link-chip-main {
        display: flex;
        flex-wrap: wrap;
        gap: 6px 8px;
        align-items: flex-start;
        min-width: 0;
    }
    .link-chip-title {
        overflow-wrap: anywhere;
    }
    .link-chip-summary {
        color: var(--muted);
        font-size: 12px;
        line-height: 1.4;
        overflow-wrap: anywhere;
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
    .status-accepted { color: var(--accepted); border-color: color-mix(in srgb, var(--accepted) 24%, white); background: color-mix(in srgb, var(--accepted) 10%, white); }
    .status-kept { color: var(--kept); border-color: color-mix(in srgb, var(--kept) 22%, white); background: color-mix(in srgb, var(--kept) 9%, white); }
    .status-parked { color: var(--parked); border-color: color-mix(in srgb, var(--parked) 24%, white); background: color-mix(in srgb, var(--parked) 10%, white); }
    .status-rejected { color: var(--rejected); border-color: color-mix(in srgb, var(--rejected) 24%, white); background: color-mix(in srgb, var(--rejected) 10%, white); }
    .status-open, .status-exploring { color: var(--accent); border-color: color-mix(in srgb, var(--accent) 22%, white); background: var(--accent-soft); }
    .status-neutral, .classless { color: #5f584d; border-color: var(--border-strong); background: var(--panel); }
    .status-archived { color: #7a756d; border-color: var(--border); background: var(--panel); }
    .metric-table {
        width: 100%;
        min-width: 0;
        border-collapse: collapse;
        table-layout: auto;
        font-size: 13px;
    }
    .metric-table-fit-col {
        width: 1%;
    }
    .metric-table-title-col {
        min-width: 0;
    }
    .table-scroll {
        width: 100%;
        min-width: 0;
        overflow-x: hidden;
    }
    .metric-table th,
    .metric-table td {
        padding: 7px 8px;
        border-top: 1px solid var(--border);
        text-align: left;
        vertical-align: top;
        white-space: nowrap;
        min-width: 0;
        overflow-wrap: normal;
        word-break: normal;
    }
    .metric-table th {
        color: var(--muted);
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-size: 12px;
    }
    .metric-registry-filter-heading {
        min-width: min(36ch, 42vw);
    }
    .metric-registry-filter-cell {
        display: inline-flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
        min-width: 0;
        width: 100%;
    }
    .metric-registry-filter {
        flex: 1 1 18ch;
        max-width: 28ch;
        min-width: 14ch;
        text-transform: none;
        letter-spacing: normal;
    }
    .metric-table-fit-heading,
    .metric-table-rank-cell,
    .metric-table-closed-cell,
    .metric-table-verdict-cell,
    .metric-table-value-cell {
        width: 1%;
    }
    .metric-table-title-heading {
        overflow: hidden;
    }
    .metric-table-title-cell {
        max-width: 0;
        overflow: hidden;
    }
    .metric-table-link {
        display: block;
        width: 100%;
        min-width: 0;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        vertical-align: top;
    }
    .metric-table-fixed-text {
        display: inline;
    }
    .metric-table-verdict-chip {
        max-width: none;
    }
    .related-block {
        display: grid;
        gap: 8px;
    }
    .chart-frame {
        position: relative;
        border: 1px solid var(--border);
        background: var(--panel-2);
        padding: 8px;
        overflow: hidden;
    }
    .chart-frame svg {
        display: block;
        width: 100%;
        height: auto;
    }
    .chart-action-row {
        position: absolute;
        top: 14px;
        right: 14px;
        z-index: 2;
        display: flex;
        align-items: center;
    }
    .plot-copy-png {
        border: 1px solid var(--border-strong);
        background: color-mix(in srgb, var(--panel) 92%, white);
        color: var(--text);
        padding: 6px 9px;
        font: inherit;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        cursor: pointer;
        box-shadow: 0 8px 18px rgba(83, 61, 33, 0.12);
    }
    .plot-copy-png:disabled {
        cursor: wait;
        opacity: 0.65;
    }
    .plot-copy-png[data-copied] {
        color: var(--accepted);
        border-color: color-mix(in srgb, var(--accepted) 24%, white);
    }
    .plot-copy-png[data-failed] {
        color: var(--rejected);
        border-color: color-mix(in srgb, var(--rejected) 24%, white);
    }
    .metric-table-section {
        margin-top: 2px;
    }
    .metric-table-header {
        display: flex;
        gap: 10px;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
    }
    .metric-table-tabs {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
    }
    .metric-table-tab {
        display: inline-flex;
        align-items: center;
        padding: 6px 10px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .metric-table-tab.active {
        color: var(--text);
        border-color: var(--border-strong);
        background: var(--accent-soft);
        font-weight: 700;
    }
    .metric-table-caption {
        margin: 0;
        font-size: 12px;
    }
    .chart-error {
        color: var(--rejected);
        font-size: 13px;
    }
    .roadmap-list, .simple-list {
        margin: 0;
        padding-left: 18px;
        display: grid;
        gap: 6px;
    }
    .roadmap-list li, .simple-list li {
        overflow-wrap: anywhere;
    }
    .code-block {
        white-space: pre-wrap;
        overflow-wrap: anywhere;
        border: 1px solid var(--border);
        background: var(--panel-2);
        padding: 12px 14px;
    }
    code {
        font-family: inherit;
        font-size: 0.95em;
        background: var(--panel-2);
        padding: 0.05rem 0.3rem;
    }
    @media (max-width: 980px) {
        .shell {
            grid-template-columns: 1fr;
        }
        .sidebar {
            position: static;
        }
        .plot-toolbar {
            width: 100%;
            margin-left: 0;
        }
    }
    @media (max-width: 720px) {
        .shell { padding: 12px; }
        .card, .page-header { padding: 14px; }
        .subcard, .mini-card { padding: 12px; }
        .card-grid, .split, .kv-grid { grid-template-columns: 1fr; }
        .page-title { font-size: 18px; }
        .control-popout {
            width: 100%;
        }
        .control-popout-toggle {
            width: 100%;
            justify-content: center;
        }
        .control-popout-panel,
        .metric-popout-panel {
            position: static;
            width: 100%;
            max-height: none;
            margin-top: 8px;
            box-shadow: 0 1px 0 var(--shadow);
        }
        .metric-popout-layout {
            grid-template-columns: 1fr;
        }
    }
    "#
}
