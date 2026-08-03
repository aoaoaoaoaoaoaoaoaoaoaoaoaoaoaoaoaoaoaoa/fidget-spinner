pub(in crate::ui) fn styles() -> &'static str {
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
    *, *::before, *::after { box-sizing: border-box; }
    :where(.shell, .main-column, .sidebar, .card, .subcard, .mini-card, .card-header, .block, .split, .card-grid, .chip-row, .link-list, .meta-row, .simple-list) {
        min-width: 0;
    }
    :where(.card, .subcard, .mini-card) {
        max-width: 100%;
    }
    :where(.card, .subcard, .mini-card) > * {
        min-width: 0;
    }
    :where(.title-link, .prose, .sidebar-copy, .frontier-nav-title, .frontier-nav-meta, .simple-list, .simple-list a, .link-chip-title, .link-chip-summary, .meta-row > *, .kv-value) {
        overflow-wrap: anywhere;
        word-break: break-word;
    }
    body {
        margin: 0;
        background: var(--bg);
        color: var(--text);
        font: 15px/1.55 "Iosevka Web", "IBM Plex Mono", "SFMono-Regular", monospace;
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
        min-width: 0;
    }
    .frontier-nav-meta {
        color: var(--muted);
        font-size: 12px;
        min-width: 0;
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
        grid-template-columns: repeat(auto-fit, minmax(min(100%, 220px), 1fr));
    }
    .tag-create-form,
    .tag-inline-form {
        display: flex;
        gap: 6px;
        align-items: center;
        flex-wrap: wrap;
        margin: 0;
    }
    .metric-create-stack {
        display: grid;
        gap: 8px;
        min-width: 0;
    }
    .metric-create-form {
        padding: 7px;
        border: 1px solid var(--border);
        background: var(--panel-2);
    }
    .synthetic-metric-create-form {
        background: var(--panel);
    }
    .metric-create-label {
        color: var(--muted);
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        white-space: nowrap;
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
    .metric-kind-chip {
        display: inline-grid;
        place-items: center;
        flex: 0 0 auto;
        height: 20px;
        padding: 0 5px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--muted);
        font-size: 10px;
        font-weight: 700;
        letter-spacing: 0.06em;
        white-space: nowrap;
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
    .inline-action-row {
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
    .promote-icon-button {
        color: var(--accepted);
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
        min-width: 720px;
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
        grid-template-columns: repeat(auto-fit, minmax(min(100%, 320px), 1fr));
        align-items: start;
    }
    .card-grid {
        display: grid;
        gap: 12px;
        grid-template-columns: repeat(auto-fit, minmax(min(100%, 260px), 1fr));
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
        overflow-wrap: anywhere;
        word-break: break-word;
    }
    .markdown-prose {
        white-space: normal;
    }
    .markdown-prose > :first-child {
        margin-top: 0;
    }
    .markdown-prose > :last-child {
        margin-bottom: 0;
    }
    .markdown-prose :where(p, ul, ol, blockquote, pre, table) {
        margin: 0 0 8px;
    }
    .markdown-prose :where(ul, ol) {
        padding-left: 20px;
    }
    .markdown-prose :where(code) {
        font: inherit;
        background: var(--panel);
        border: 1px solid var(--line);
        padding: 0 3px;
    }
    .markdown-prose :where(pre) {
        overflow-x: auto;
        background: var(--panel);
        border: 1px solid var(--line);
        padding: 8px;
    }
    .markdown-prose :where(pre code) {
        background: transparent;
        border: 0;
        padding: 0;
    }
    .markdown-prose :where(blockquote) {
        border-left: 3px solid var(--line-strong);
        padding-left: 10px;
        color: var(--muted);
    }
    .markdown-prose :where(table) {
        border-collapse: collapse;
        max-width: 100%;
    }
    .markdown-prose :where(th, td) {
        border: 1px solid var(--line);
        padding: 3px 6px;
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
        grid-template-columns: repeat(auto-fit, minmax(min(100%, 160px), 1fr));
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
        min-width: 0;
        border: 1px solid var(--border-strong);
        background: var(--tag);
        padding: 4px 8px;
        font-size: 12px;
        line-height: 1.2;
        white-space: normal;
        overflow-wrap: anywhere;
        word-break: break-word;
    }
    .tag-registry-table .tag-chip {
        white-space: nowrap;
        overflow-wrap: normal;
        word-break: normal;
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
    .control-popout:not([open]) > .control-popout-panel {
        display: none;
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
        grid-template-columns: repeat(auto-fit, minmax(min(100%, 180px), 1fr));
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
        display: inline-flex;
        gap: 6px;
        align-items: center;
        min-width: 0;
    }
    .metric-checkbox-title {
        color: var(--text);
        font-weight: 700;
        min-width: 0;
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
        grid-template-columns: repeat(auto-fit, minmax(min(100%, 180px), 1fr));
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
        max-width: none;
        white-space: nowrap;
        overflow-wrap: normal;
        word-break: normal;
    }
    .status-chip {
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 700;
        max-width: none;
        white-space: nowrap;
        overflow-wrap: normal;
        word-break: normal;
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
        min-width: 860px;
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
        overflow-x: auto;
        overscroll-behavior-inline: contain;
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
    .registry-pagination {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 10px;
        flex-wrap: wrap;
        margin-top: 12px;
    }
    .metric-table-fit-heading,
    .metric-table-action-cell,
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
    .metric-table-verdict-actions {
        display: flex;
        align-items: center;
        gap: 4px;
        white-space: nowrap;
    }
    .metric-table-action-cell {
        padding-left: 6px;
        padding-right: 2px;
    }
    .metric-table-action-cell .inline-action-form {
        display: inline-flex;
    }
    .metric-table-scuff-button {
        opacity: 0.72;
    }
    .metric-table-scuff-button:hover {
        opacity: 1;
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
        overflow-x: auto;
        overscroll-behavior-inline: contain;
    }
    #metric-plot-card[aria-busy="true"] .chart-frame {
        opacity: 0.68;
    }
    #metric-plot-card[data-chart-error]::after {
        content: attr(data-chart-error);
        color: var(--rejected);
        font-size: 12px;
    }
    .chart-frame svg {
        display: block;
        width: 100%;
        height: auto;
        min-width: 680px;
        touch-action: pan-y;
    }
    .chart-frame [data-chart-hit="true"] {
        cursor: crosshair;
    }
    .chart-frame [data-chart-hit="true"]:hover rect {
        fill: rgba(103, 86, 63, 0.08);
    }
    .plot-series-controls {
        min-width: 0;
        margin: 0;
        padding: 8px 10px 10px;
        border: 1px solid var(--border);
        background: var(--panel-2);
    }
    .plot-series-controls legend {
        padding: 0 5px;
        color: var(--muted);
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }
    .plot-series-toggle-list {
        display: flex;
        flex-wrap: wrap;
        gap: 6px 14px;
        min-width: 0;
    }
    .plot-series-toggle {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        min-width: 0;
        cursor: pointer;
        font-size: 12px;
    }
    .plot-series-toggle input {
        margin: 0;
    }
    .plot-series-swatch {
        width: 16px;
        flex: 0 0 16px;
        border-top: 2px solid;
    }
    .plot-series-label {
        max-width: 38ch;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .chart-warning {
        margin: 0;
        color: var(--parked);
        font-size: 12px;
        overflow-wrap: anywhere;
    }
    .chart-rubber-band {
        position: absolute;
        z-index: 3;
        top: 8px;
        bottom: 8px;
        border: 1px solid var(--accent);
        background: color-mix(in srgb, var(--accent) 12%, transparent);
        pointer-events: none;
    }
    .chart-hover-card {
        position: absolute;
        z-index: 5;
        width: min(360px, calc(100% - 16px));
        max-height: 280px;
        overflow: auto;
        padding: 10px 12px;
        border: 1px solid var(--border-strong);
        background: var(--panel);
        color: var(--text);
        box-shadow: 0 12px 28px rgba(83, 61, 33, 0.2);
        font-size: 12px;
    }
    .chart-tooltip-title {
        display: block;
        color: var(--text);
        font-weight: 700;
        overflow-wrap: anywhere;
    }
    .chart-tooltip-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 5px 8px;
        align-items: center;
        color: var(--muted);
    }
    .chart-tooltip-values {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto;
        gap: 3px 10px;
        margin: 8px 0 0;
    }
    .chart-tooltip-values dt,
    .chart-tooltip-values dd {
        min-width: 0;
        margin: 0;
    }
    .chart-tooltip-values dt {
        overflow-wrap: anywhere;
    }
    .chart-tooltip-values dd {
        white-space: nowrap;
    }
    .chart-reference-list {
        display: flex;
        flex-wrap: wrap;
        gap: 5px 14px;
        margin-top: 8px;
        color: var(--muted);
        font-size: 11px;
    }
    .chart-reference {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        min-width: 0;
        overflow-wrap: anywhere;
    }
    .chart-reference-swatch {
        width: 16px;
        flex: 0 0 16px;
        border-top: 1px dashed;
    }
    .chart-action-row {
        position: absolute;
        top: 14px;
        right: 14px;
        z-index: 2;
        display: flex;
        align-items: center;
        gap: 6px;
    }
    .plot-copy-png,
    .plot-reset {
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
        min-width: 0;
        max-width: 100%;
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
        max-width: 100%;
        white-space: normal;
        overflow-wrap: anywhere;
        word-break: break-word;
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
        white-space: normal;
        overflow-wrap: anywhere;
        word-break: break-word;
    }
    .chart-error {
        color: var(--rejected);
        font-size: 13px;
    }
    .simple-list {
        margin: 0;
        padding-left: 18px;
        display: grid;
        gap: 6px;
    }
    .simple-list li {
        min-width: 0;
        max-width: 100%;
        overflow-wrap: anywhere;
        word-break: break-word;
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
        .frontier-summary-editor {
            width: auto;
        }
        .frontier-summary-editor[open] {
            width: 100%;
            flex: 1 0 100%;
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
        .chart-frame svg {
            width: 780px;
            max-width: none;
        }
    }
    "#
}
