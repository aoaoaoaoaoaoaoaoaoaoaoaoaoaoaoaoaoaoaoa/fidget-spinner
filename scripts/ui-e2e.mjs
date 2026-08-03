#!/usr/bin/env node

import { execFileSync, spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const LARGE_DB_FLAG = "--large-db";
const LARGE_DB_INDEX = process.argv.indexOf(LARGE_DB_FLAG);
const largeDatabase = LARGE_DB_INDEX < 0 ? null : resolve(process.argv[LARGE_DB_INDEX + 1] ?? "");
const workspace = mkdtempSync(join(tmpdir(), "fidget-spinner-ui-e2e-"));
const artifactRoot = resolve(
    process.env.FIDGET_SPINNER_UI_ARTIFACTS
        ?? join(tmpdir(), `fidget-spinner-ui-e2e-artifacts-${process.pid}`),
);
const children = new Set();

function fail(message) {
    throw new Error(message);
}

function run(executable, args, options = {}) {
    return execFileSync(executable, args, {
        cwd: ROOT,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
        ...options,
    }).trim();
}

function cargoBinary(release) {
    const metadata = JSON.parse(run("cargo", ["metadata", "--format-version", "1", "--no-deps"]));
    const profile = release ? "release" : "debug";
    const executable = join(metadata.target_directory, profile, "fidget-spinner-cli");
    run("cargo", ["build", ...(release ? ["--release"] : []), "--package", "fidget-spinner-cli", "--bin", "fidget-spinner-cli"]);
    return executable;
}

function spinnerEnvironment(stateHome) {
    return { ...process.env, FIDGET_SPINNER_STATE_HOME: stateHome };
}

function seedMockStore(executable, stateHome, projectRoot) {
    const env = spinnerEnvironment(stateHome);
    mkdirSync(projectRoot, { recursive: true });
    run("git", ["init", "-q", projectRoot]);
    writeFileSync(join(projectRoot, "README.md"), "# Browser fixture\n", "utf8");
    run("git", ["-C", projectRoot, "add", "README.md"]);
    run("git", ["-C", projectRoot, "-c", "user.name=UI E2E", "-c", "user.email=ui-e2e@example.invalid", "commit", "-q", "-m", "seed"]);
    const cli = (...args) => run(executable, args, { env });
    cli("init", "--project", projectRoot, "--name", "Long-Breath Experimental Navigator Fixture");
    cli("tag", "add", "--project", projectRoot, "--name", "latency", "--description", "Latency-sensitive work whose deliberately long description probes table containment and readable wrapping.");
    cli("tag", "add", "--project", projectRoot, "--name", "correctness", "--description", "Semantic preservation and proof obligations.");
    cli("frontier", "create", "--project", projectRoot, "--slug", "squeaky-clean", "--label", "Squeaky Clean Release Frontier With A Purposefully Long Label", "--objective", "Exercise every navigator surface without allowing long prose, identifiers, controls, or tables to rupture their containers.");
    cli("metric", "define", "--project", projectRoot, "--key", "wallclock_milliseconds_with_an_intentionally_long_key", "--dimension", "time", "--display-unit", "milliseconds", "--objective", "minimize", "--description", "Cold wall-clock latency across the complete experimental loop.");
    cli("metric", "define", "--project", projectRoot, "--key", "proof_obligations", "--dimension", "count", "--display-unit", "count", "--objective", "minimize", "--description", "Count of outstanding semantic proof obligations.");
    cli("kpi", "create", "--project", projectRoot, "--frontier", "squeaky-clean", "--metric", "wallclock_milliseconds_with_an_intentionally_long_key");
    cli("kpi", "create", "--project", projectRoot, "--frontier", "squeaky-clean", "--metric", "proof_obligations");
    cli("hypothesis", "record", "--project", projectRoot, "--frontier", "squeaky-clean", "--slug", "containment-under-hostile-prose", "--title", "Containment Survives Hostile Prose And Unbroken Identifiers", "--summary", "Every visible box remains intact at narrow and wide viewports.", "--body", "Render deliberatelylongunbrokenidentifiersegmentswithoutallowingthemtopuncturethecard while preserving ordinary prose rhythm.", "--expected-yield", "high", "--confidence", "medium", "--tag", "correctness");
    cli("hypothesis", "record", "--project", projectRoot, "--frontier", "squeaky-clean", "--slug", "transition-latency", "--title", "Transitions Stay Immediate", "--summary", "Static navigation and client interactions remain brisk.", "--body", "Measure cold and warm navigation before accepting the release.", "--expected-yield", "high", "--confidence", "high", "--tag", "latency");
    cli("experiment", "open", "--project", projectRoot, "--hypothesis", "containment-under-hostile-prose", "--slug", "narrow-viewport-assault", "--title", "Narrow Viewport Assault With A Long Experimental Title", "--summary", "Probe tables, popouts, forms, chips, and prose at phone width.", "--tag", "correctness");
    cli("experiment", "open", "--project", projectRoot, "--hypothesis", "transition-latency", "--slug", "measured-transition", "--title", "Measured Browser Transition", "--summary", "Produce a closed result, plot, and editable outcome for browser traversal.", "--tag", "latency");
    cli("experiment", "close", "--project", projectRoot, "--experiment", "measured-transition", "--keep-hypothesis-on-worklist", "false", "--backend", "manual", "--argv", "browser-e2e", "--primary-metric", "wallclock_milliseconds_with_an_intentionally_long_key=37@milliseconds", "--metric", "proof_obligations=0@count", "--verdict", "accepted", "--rationale", "The assembled browser journey completed within its fixture budget.", "--analysis-summary", "Transitions are immediate", "--analysis-body", "The closed fixture exercises result plots and outcome prose without touching operator data.");
    const durations = [18, 24, 31, 45, 63, 88, 125, 190, 280, 420, 680, 5_400_000];
    for (const [index, duration] of durations.entries()) {
        const ordinal = String(index + 1).padStart(2, "0");
        const slug = "bundle-" + ordinal;
        cli("experiment", "open", "--project", projectRoot, "--hypothesis", "containment-under-hostile-prose", "--slug", slug, "--title", "Experimental Bundle " + ordinal, "--summary", "A closed datum for chart interaction, unit selection, and horizontal windowing.", "--tag", "latency");
        cli("experiment", "close", "--project", projectRoot, "--experiment", slug, "--keep-hypothesis-on-worklist", "true", "--backend", "manual", "--argv", "browser-e2e", "--primary-metric", "wallclock_milliseconds_with_an_intentionally_long_key=" + duration + "@milliseconds", "--metric", "proof_obligations=" + (durations.length - index) + "@count", "--verdict", index % 4 === 3 ? "parked" : "accepted", "--rationale", "Synthetic browser fixture measurement.", "--analysis-summary", "Chart fixture datum", "--analysis-body", "This datum exists to exercise the complete semantic SVG interaction contract.");
    }
}

function cloneLargeStore(source, stateHome) {
    const projectRoot = run("sqlite3", [source, "SELECT project_root FROM project_metadata WHERE id = 1;"]);
    const frontier = run("sqlite3", [source, "SELECT frontiers.slug FROM frontiers LEFT JOIN hypotheses ON hypotheses.frontier_id = frontiers.id LEFT JOIN experiments ON experiments.hypothesis_id = hypotheses.id LEFT JOIN experiment_outcomes ON experiment_outcomes.experiment_id = experiments.id WHERE frontiers.status != 'archived' GROUP BY frontiers.id ORDER BY COUNT(experiment_outcomes.experiment_id) DESC, frontiers.updated_at DESC LIMIT 1;"]);
    const destination = join(stateHome, "fidget-spinner", "projects", basename(dirname(source)), "state.sqlite");
    mkdirSync(dirname(destination), { recursive: true });
    run("cp", ["--reflink=auto", source, destination]);
    return { projectRoot, frontier, database: destination };
}

async function waitFor(predicate, description, timeoutMs = 15_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const result = await predicate();
        if (result) return result;
        await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
    }
    fail(`timed out waiting for ${description}`);
}

async function startNavigator(executable, stateHome) {
    const child = spawn(executable, ["ui", "serve", "--bind", "127.0.0.1:0"], {
        cwd: ROOT,
        env: spinnerEnvironment(stateHome),
        stdio: ["ignore", "pipe", "pipe"],
    });
    children.add(child);
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("exit", (code) => {
        if (code !== null && code !== 0) stderr += `\nnavigator exited ${code}`;
    });
    const origin = await waitFor(() => stdout.match(/navigator: (http:\/\/[^/]+)\//)?.[1], "navigator address");
    return {
        child,
        origin,
        stop() {
            child.kill("SIGTERM");
            children.delete(child);
        },
        diagnostics() { return `${stdout}\n${stderr}`.trim(); },
    };
}

async function startChromium(label) {
    const profile = join(workspace, `chromium-${label}`);
    mkdirSync(profile, { recursive: true });
    const child = spawn("chromium", [
        "--headless=new",
        "--no-sandbox",
        "--disable-background-networking",
        "--disable-component-update",
        "--disable-default-apps",
        "--disable-sync",
        "--metrics-recording-only",
        "--no-first-run",
        "--remote-debugging-address=127.0.0.1",
        "--remote-debugging-port=0",
        `--user-data-dir=${profile}`,
        "about:blank",
    ], { stdio: ["ignore", "ignore", "pipe"] });
    children.add(child);
    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    const activePort = join(profile, "DevToolsActivePort");
    const [port] = (await waitFor(() => {
        try { return readFileSync(activePort, "utf8").trim().split("\n"); } catch { return null; }
    }, "Chromium DevTools endpoint")).map((line) => line.trim());
    const target = await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, { method: "PUT" }).then((response) => response.json());
    const client = await CdpClient.connect(target.webSocketDebuggerUrl);
    await Promise.all([
        client.send("Page.enable"),
        client.send("Runtime.enable"),
        client.send("Network.enable"),
    ]);
    return {
        child,
        client,
        stop() {
            client.close();
            child.kill("SIGTERM");
            children.delete(child);
        },
        diagnostics() { return stderr; },
    };
}

class CdpClient {
    static async connect(url) {
        const socket = new WebSocket(url);
        await new Promise((resolveOpen, rejectOpen) => {
            socket.addEventListener("open", resolveOpen, { once: true });
            socket.addEventListener("error", rejectOpen, { once: true });
        });
        return new CdpClient(socket);
    }

    constructor(socket) {
        this.socket = socket;
        this.nextId = 1;
        this.pending = new Map();
        this.listeners = new Map();
        socket.addEventListener("message", (event) => this.receive(JSON.parse(event.data)));
    }

    receive(message) {
        if (message.id) {
            const pending = this.pending.get(message.id);
            if (!pending) return;
            this.pending.delete(message.id);
            if (message.error) pending.reject(new Error(`${pending.method}: ${message.error.message}`));
            else pending.resolve(message.result);
            return;
        }
        const listeners = this.listeners.get(message.method) ?? [];
        this.listeners.delete(message.method);
        for (const listener of listeners) listener(message.params);
    }

    send(method, params = {}) {
        const id = this.nextId++;
        return new Promise((resolveMessage, rejectMessage) => {
            this.pending.set(id, { method, resolve: resolveMessage, reject: rejectMessage });
            this.socket.send(JSON.stringify({ id, method, params }));
        });
    }

    event(method, timeoutMs = 15_000) {
        return new Promise((resolveEvent, rejectEvent) => {
            const timer = setTimeout(() => rejectEvent(new Error(`timed out waiting for ${method}`)), timeoutMs);
            const listener = (params) => { clearTimeout(timer); resolveEvent(params); };
            this.listeners.set(method, [...(this.listeners.get(method) ?? []), listener]);
        });
    }

    async evaluate(expression) {
        const result = await this.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
        if (result.exceptionDetails) fail(`browser evaluation failed: ${result.exceptionDetails.text}`);
        return result.result.value;
    }

    async navigate(url) {
        const loaded = this.event("Page.loadEventFired");
        await this.send("Page.navigate", { url });
        await loaded;
        await this.evaluate("document.fonts?.ready ?? Promise.resolve()");
        return this.navigationTiming();
    }

    async submit(expression) {
        const loaded = this.event("Page.loadEventFired");
        await this.evaluate(expression);
        await loaded;
    }

    navigationTiming() {
        return this.evaluate(`(() => {
            const entry = performance.getEntriesByType("navigation")[0];
            return entry ? {
                responseMs: entry.responseStart,
                completeMs: entry.loadEventEnd,
                transferBytes: entry.transferSize,
                decodedBytes: entry.decodedBodySize,
            } : null;
        })()`);
    }

    close() { this.socket.close(); }
}

function containmentProbe() {
    return `(() => {
        const viewport = document.documentElement.clientWidth;
        const leaks = [];
        const clips = [];
        const overflowing = [];
        const describe = (element) => {
            const id = element.id ? "#" + element.id : "";
            const classes = Array.from(element.classList).slice(0, 3).map((name) => "." + name).join("");
            return element.tagName.toLowerCase() + id + classes;
        };
        for (const element of document.querySelectorAll("body *")) {
            const rect = element.getBoundingClientRect();
            if (rect.width <= 0 || rect.height <= 0) continue;
            const style = getComputedStyle(element);
            const scroller = element.closest(".table-scroll, .table-wrap, .code-block, .control-popout-panel, .metric-popout-panel");
            const chart = element.closest(".chart-frame");
            if ((rect.left < -1 || rect.right > viewport + 1) && !scroller && !chart) {
                leaks.push({ element: describe(element), left: Math.round(rect.left), right: Math.round(rect.right), viewport });
            }
            if (rect.right > viewport + 1) {
                overflowing.push({
                    element: describe(element),
                    right: Math.round(rect.right),
                    scrollWidth: element.scrollWidth,
                    clientWidth: element.clientWidth,
                    scroller: scroller ? describe(scroller) : null,
                    chart: chart ? describe(chart) : null,
                });
            }
            const clipped = element.scrollWidth > element.clientWidth + 1 && ["hidden", "clip"].includes(style.overflowX);
            const intentionalEllipsis = style.textOverflow === "ellipsis";
            const formControl = element.matches("input, select, textarea");
            if (clipped && !intentionalEllipsis && !chart && !formControl) {
                clips.push({ element: describe(element), client: element.clientWidth, scroll: element.scrollWidth });
            }
        }
        return {
            title: document.title,
            url: location.href,
            viewport,
            documentOverflow: document.documentElement.scrollWidth - viewport,
            leaks: leaks.slice(0, 20),
            clips: clips.slice(0, 20),
            overflowing: overflowing.slice(0, 20),
            collapsedHeadings: Array.from(document.querySelectorAll("h1, h2, h3"))
                .filter((heading) => heading.textContent.trim().length > 20 && heading.getBoundingClientRect().width < Math.min(100, viewport * 0.3))
                .map((heading) => ({ element: describe(heading), width: Math.round(heading.getBoundingClientRect().width), text: heading.textContent.trim().slice(0, 80) })),
            duplicateIds: Array.from(document.querySelectorAll("[id]"))
                .map((element) => element.id)
                .filter((id, index, ids) => ids.indexOf(id) !== index)
                .filter((id, index, ids) => ids.indexOf(id) === index),
            unnamedControls: Array.from(document.querySelectorAll("a, button, input, select, textarea, summary"))
                .filter((element) => {
                    if (element.matches('input[type="hidden"]')) return false;
                    const rect = element.getBoundingClientRect();
                    if (rect.width <= 0 || rect.height <= 0) return false;
                    const labels = "labels" in element
                        ? Array.from(element.labels ?? []).map((label) => label.textContent).join(" ")
                        : "";
                    const name = element.getAttribute("aria-label")
                        || element.getAttribute("title")
                        || labels
                        || element.textContent
                        || (element.matches('input[type="submit"]') ? element.value : "");
                    return !name?.trim();
                })
                .map(describe)
                .slice(0, 20),
        };
    })()`;
}

async function screenshot(client, name) {
    const capture = await client.send("Page.captureScreenshot", { format: "png", fromSurface: true });
    const path = join(artifactRoot, `${name}.png`);
    writeFileSync(path, Buffer.from(capture.data, "base64"));
    return path;
}


async function waitForBrowser(client, expression, description, timeoutMs = 15_000) {
    return waitFor(() => client.evaluate(expression), description, timeoutMs);
}

async function exerciseChart(client, resultsUrl, origin) {
    await client.navigate(resultsUrl);
    const initial = await client.evaluate("(() => ({ toggles: document.querySelectorAll('[data-chart-series-toggle]').length, hits: document.querySelectorAll('[data-chart-hit=true]').length, axis: Array.from(document.querySelectorAll('svg[data-chart-navigator] text')).map((node) => node.textContent) }))()");
    if (initial.toggles !== 2) fail("chart did not expose both KPI display toggles");
    if (initial.hits < 10) fail("chart fixture did not expose enough linked data points");
    if (!initial.axis.includes("hours")) fail("full chart did not choose hours for its visible magnitude");

    const hoverPoint = await client.evaluate("(() => { const hit = document.querySelector('[data-chart-hit=true]'); const box = hit.getBoundingClientRect(); return { x: (box.left + box.right) / 2, y: (box.top + box.bottom) / 2 }; })()");
    await client.send("Input.dispatchMouseEvent", { type: "mouseMoved", x: hoverPoint.x, y: hoverPoint.y });
    await waitForBrowser(client, "Boolean(document.querySelector('[data-chart-tooltip=true]:not([hidden]) a[href*=\"experiment/\"]'))", "linked chart tooltip");
    const tooltip = await client.evaluate("(() => ({ title: document.querySelector('[data-chart-tooltip=true] a')?.textContent, values: document.querySelector('[data-chart-tooltip=true] dl')?.textContent, href: document.querySelector('[data-chart-tooltip=true] a')?.href }))()");
    if (!tooltip.title || !tooltip.values || !tooltip.href.includes("/experiment/")) fail("chart tooltip omitted experiment identity, values, or link");

    const firstMetric = await client.evaluate("document.querySelector('[data-chart-series-toggle]')?.value");
    const toggleStarted = performance.now();
    await client.evaluate("document.querySelector('[data-chart-series-toggle]').click()");
    await waitForBrowser(client, "location.search.includes('hidden_metric=') && document.getElementById('metric-plot-card')?.getAttribute('aria-busy') === 'false'", "KPI toggle fragment");
    const toggleMs = performance.now() - toggleStarted;
    const hiddenState = await client.evaluate("(() => ({ checked: document.querySelector('[data-chart-series-toggle]')?.checked, seriesPresent: Array.from(document.querySelectorAll('[data-chart-series]')).some((node) => node.dataset.chartSeries === " + JSON.stringify(firstMetric) + "), error: document.getElementById('metric-plot-card')?.dataset.chartError }))()");
    if (hiddenState.checked || hiddenState.seriesPresent || hiddenState.error) fail("KPI toggle did not reconcile the authoritative chart");
    await client.evaluate("document.querySelector('[data-chart-series-toggle]').click()");
    await waitForBrowser(client, "!location.search.includes('hidden_metric=') && document.querySelector('[data-chart-series-toggle]')?.checked === true", "KPI toggle restoration");

    const drag = await client.evaluate("(() => { const hits = Array.from(document.querySelectorAll('[data-chart-hit=true]')); const first = hits[1].getBoundingClientRect(); const last = hits[6].getBoundingClientRect(); return { startX: (first.left + first.right) / 2, endX: (last.left + last.right) / 2, y: (first.top + first.bottom) / 2 }; })()");
    const zoomStarted = performance.now();
    await client.send("Input.dispatchMouseEvent", { type: "mousePressed", x: drag.startX, y: drag.y, button: "left", clickCount: 1 });
    await client.send("Input.dispatchMouseEvent", { type: "mouseMoved", x: drag.endX, y: drag.y, button: "left" });
    await client.send("Input.dispatchMouseEvent", { type: "mouseReleased", x: drag.endX, y: drag.y, button: "left", clickCount: 1 });
    await waitForBrowser(client, "location.search.includes('plot_from=') && Boolean(document.querySelector('[data-chart-reset-window=true]'))", "drag zoom fragment");
    const zoomMs = performance.now() - zoomStarted;
    const zoomed = await client.evaluate("(() => ({ hits: document.querySelectorAll('[data-chart-hit=true]').length, axis: Array.from(document.querySelectorAll('svg[data-chart-navigator] text')).map((node) => node.textContent), error: document.getElementById('metric-plot-card')?.dataset.chartError }))()");
    if (zoomed.hits >= initial.hits || !zoomed.axis.includes("milliseconds") || zoomed.error) fail("horizontal zoom did not narrow data and recompute units");

    await client.evaluate("document.querySelector('[data-chart-reset-window=true]').click()");
    await waitForBrowser(client, "!location.search.includes('plot_from=') && Array.from(document.querySelectorAll('svg[data-chart-navigator] text')).some((node) => node.textContent === 'hours')", "chart zoom reset");

    await client.send("Browser.grantPermissions", {
        origin,
        permissions: ["clipboardReadWrite", "clipboardSanitizedWrite"],
    });
    await client.evaluate("document.querySelector('[data-copy-plot-png=true]').click()");
    await waitForBrowser(client, "document.querySelector('[data-copy-plot-png=true]')?.textContent.trim() === 'Copied'", "PNG clipboard export");
    await client.evaluate("(() => { const form = document.querySelector('#metric-selection-popout form'); for (const input of form.querySelectorAll('input[name=metric]')) input.checked = false; form.requestSubmit(); })()");
    await waitForBrowser(client, "new URL(location.href).searchParams.get('metric_mode') === 'explicit' && new URL(location.href).searchParams.getAll('metric').length === 0 && document.getElementById('metric-plot-card')?.textContent.includes('No metrics selected')", "explicit empty metric selection");
    const emptySelectionPreserved = await client.evaluate("(() => { const url = new URL(document.querySelector('.tab-row a')?.href); return url.searchParams.get('metric_mode') === 'explicit' && url.searchParams.getAll('metric').length === 0; })()");
    if (!emptySelectionPreserved) fail("fragment refresh did not reconcile tab navigation state");
    await client.evaluate("history.back()");
    await waitForBrowser(client, "document.querySelectorAll('[data-chart-series-toggle]').length === 2", "metric selection history restoration");
    assertPageIntegrity(await client.evaluate(containmentProbe()));
    return { toggleMs, zoomMs, tooltip };
}

function assertPageIntegrity(probe) {
    if (probe.documentOverflow > 1
        || probe.leaks.length
        || probe.clips.length
        || probe.collapsedHeadings.length
        || probe.duplicateIds.length
        || probe.unnamedControls.length) {
        fail(`page integrity failure on ${probe.url}: ${JSON.stringify(probe, null, 2)}`);
    }
}

async function exerciseMock(executable) {
    const stateHome = join(workspace, "mock-state");
    const projectRoot = join(workspace, "mock-project");
    seedMockStore(executable, stateHome, projectRoot);
    const server = await startNavigator(executable, stateHome);
    const browser = await startChromium("mock");
    try {
        const { client } = browser;
        await client.send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
        await client.navigate(`${server.origin}/`);
        const projectHref = await client.evaluate("document.querySelector('.title-link')?.href");
        if (!projectHref) fail("project index did not expose the mock project");
        const rejectedCrossOriginWrite = await fetch(`${projectHref}tags/create`, {
            method: "POST",
            headers: {
                "content-type": "application/x-www-form-urlencoded",
                origin: "https://attacker.example",
            },
            body: "name=forged&description=cross-origin",
        });
        if (rejectedCrossOriginWrite.status !== 403) fail(`cross-origin write returned ${rejectedCrossOriginWrite.status}, expected 403`);
        const pages = [
            projectHref,
            `${projectHref}tags`,
            `${projectHref}metrics`,
            `${projectHref}frontier/squeaky-clean?tab=open`,
            `${projectHref}frontier/squeaky-clean?tab=results`,
        ];
        const timings = {};
        const desktopScreenshots = {};
        for (const [index, url] of pages.entries()) {
            const name = ["project", "tags", "metrics", "frontierOpen", "frontierResults"][index];
            timings[name] = await client.navigate(url);
            assertPageIntegrity(await client.evaluate(containmentProbe()));
            desktopScreenshots[name] = await screenshot(client, `mock-desktop-${name}`);
        }
        const chartJourney = await exerciseChart(
            client,
            `${projectHref}frontier/squeaky-clean?tab=results`,
            server.origin,
        );
        await client.navigate(`${projectHref}frontier/squeaky-clean?tab=open`);
        const hypothesisHref = await client.evaluate("document.querySelector('a[href*=" + JSON.stringify("hypothesis/containment") + "]')?.href");
        if (!hypothesisHref) fail("frontier page did not expose a hypothesis link");
        await client.navigate(hypothesisHref);
        assertPageIntegrity(await client.evaluate(containmentProbe()));
        const experimentHref = await client.evaluate("document.querySelector('a[href*=" + JSON.stringify("experiment/narrow") + "]')?.href");
        if (!experimentHref) fail("hypothesis page did not expose an experiment link");
        await client.navigate(experimentHref);
        assertPageIntegrity(await client.evaluate(containmentProbe()));

        const closedHypothesisHref = `${projectHref}hypothesis/transition-latency`;
        const closedExperimentHref = `${projectHref}experiment/measured-transition`;
        await client.navigate(closedHypothesisHref);
        assertPageIntegrity(await client.evaluate(containmentProbe()));
        await client.submit(`(() => {
            const form = document.querySelector('form[action$="/body"]');
            form.elements.body.value = "Updated through the real browser while preserving the single-paragraph contract.";
            form.requestSubmit();
        })()`);
        if (!(await client.evaluate("document.body.textContent.includes('Updated through the real browser')"))) fail("browser hypothesis edit did not persist");
        await client.navigate(closedExperimentHref);
        assertPageIntegrity(await client.evaluate(containmentProbe()));
        await client.submit(`(() => {
            const form = document.querySelector('form[action$="/outcome-prose"]');
            form.elements.rationale.value = "Updated outcome rationale through an actual browser submission.";
            form.requestSubmit();
        })()`);
        if (!(await client.evaluate("document.body.textContent.includes('Updated outcome rationale')"))) fail("browser outcome edit did not persist");

        await client.navigate(`${projectHref}tags`);
        await client.submit(`(() => {
            const form = document.querySelector('form[action="tags/create"]');
            form.elements.name.value = "browser-added";
            form.elements.description.value = "Created through an actual browser submission.";
            form.requestSubmit();
        })()`);
        if (!(await client.evaluate("document.body.textContent.includes('browser-added')"))) fail("browser tag mutation did not persist");

        await client.navigate(`${projectHref}metrics`);
        const hiddenRows = await client.evaluate(`(() => {
            const input = document.querySelector('[data-table-filter-input="metric-registry"]');
            input.value = "proof_obligations";
            input.dispatchEvent(new InputEvent("input", { bubbles: true }));
            return Array.from(document.querySelectorAll('[data-table-filter-row="metric-registry"]')).filter((row) => row.hidden).length;
        })()`);
        if (hiddenRows < 1) fail("metric registry filter did not hide nonmatching rows");
        const syntheticExtrasToggle = await client.evaluate(`(() => {
            const operation = document.querySelector('[data-synthetic-operation-select]');
            const extras = Array.from(document.querySelectorAll('[data-synthetic-gmean-extra]'));
            const initiallyHidden = extras.length === 2 && extras.every((extra) => extra.hidden);
            operation.value = "gmean";
            operation.dispatchEvent(new Event("change", { bubbles: true }));
            return initiallyHidden && extras.every((extra) => !extra.hidden);
        })()`);
        if (!syntheticExtrasToggle) fail("synthetic gmean operand controls did not reveal on selection");

        await client.navigate(`${projectHref}frontier/squeaky-clean?tab=open`);
        const popoutClosed = await client.evaluate(`(() => {
            const details = document.querySelector('details.control-popout');
            if (!details) return false;
            details.open = true;
            document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
            return !details.open;
        })()`);
        if (!popoutClosed) fail("Escape did not close an open control popout");

        await client.submit(`(() => {
            const form = document.querySelector('form[action$="/summary"]');
            form.elements.label.value = "Squeaky Clean Browser-Edited Frontier";
            form.requestSubmit();
        })()`);
        if (!(await client.evaluate("document.querySelector('h1')?.textContent.includes('Browser-Edited')"))) fail("browser frontier edit did not persist");

        await client.navigate(projectHref);
        await client.submit(`document.querySelector('form[action$="/archive"]').requestSubmit()`);
        if (!(await client.evaluate("document.body.textContent.includes('Archived (1)')"))) fail("browser frontier archive did not persist");
        await client.submit(`document.querySelector('form[action$="/unarchive"]').requestSubmit()`);
        if (await client.evaluate("document.body.textContent.includes('Archived (1)')")) fail("browser frontier unarchive did not persist");

        await client.send("Emulation.setDeviceMetricsOverride", { width: 360, height: 800, deviceScaleFactor: 1, mobile: true });
        const mobileScreenshots = {};
        for (const [index, url] of pages.slice(1).entries()) {
            const name = ["tags", "metrics", "frontierOpen", "frontierResults"][index];
            await client.navigate(url);
            assertPageIntegrity(await client.evaluate(containmentProbe()));
            mobileScreenshots[name] = await screenshot(client, `mock-mobile-${name}`);
        }
        await client.navigate(hypothesisHref);
        assertPageIntegrity(await client.evaluate(containmentProbe()));
        await client.navigate(experimentHref);
        assertPageIntegrity(await client.evaluate(containmentProbe()));
        return { timings, chartJourney, desktopScreenshots, mobileScreenshots };
    } finally {
        browser.stop();
        server.stop();
    }
}

async function exerciseLarge(executable, source) {
    const stateHome = join(workspace, "large-state");
    const fixture = cloneLargeStore(source, stateHome);
    run("fadvise", ["--advice", "dontneed", fixture.database]);
    const server = await startNavigator(executable, stateHome);
    const browser = await startChromium("large");
    try {
        const { client } = browser;
        await client.send("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
        const project = `${server.origin}/project/${encodeURIComponent(fixture.projectRoot)}/`;
        const pages = {
            project,
            tags: `${project}tags`,
            metrics: `${project}metrics`,
            frontier: `${project}frontier/${encodeURIComponent(fixture.frontier)}`,
        };
        const timings = {};
        const desktopScreenshots = {};
        for (const [name, url] of Object.entries(pages)) {
            run("fadvise", ["--advice", "dontneed", fixture.database]);
            timings[name] = await client.navigate(url);
            assertPageIntegrity(await client.evaluate(containmentProbe()));
            desktopScreenshots[name] = await screenshot(client, `large-desktop-${name}`);
        }
        const budgets = { project: 1_000, tags: 1_000, metrics: 2_000, frontier: 2_500 };
        for (const [name, budget] of Object.entries(budgets)) {
            if (timings[name].completeMs > budget) fail(`${name} cold navigation ${timings[name].completeMs.toFixed(1)} ms exceeds ${budget} ms`);
        }
        if (timings.metrics.decodedBytes > 1_500_000) fail(`metrics response ${timings.metrics.decodedBytes} bytes exceeds 1.5 MB`);
        if (timings.frontier.decodedBytes > 1_100_000) fail(`frontier response ${timings.frontier.decodedBytes} bytes exceeds 1.1 MB`);
        const warmTimings = {};
        for (const [name, url] of Object.entries(pages)) {
            warmTimings[name] = await client.navigate(url);
            assertPageIntegrity(await client.evaluate(containmentProbe()));
        }
        const warmBudgets = { project: 750, tags: 750, metrics: 1_250, frontier: 2_000 };
        for (const [name, budget] of Object.entries(warmBudgets)) {
            if (warmTimings[name].completeMs > budget) fail(`${name} warm navigation ${warmTimings[name].completeMs.toFixed(1)} ms exceeds ${budget} ms`);
        }
        const largeToggleStarted = performance.now();
        const largeToggleAvailable = await client.evaluate("Boolean(document.querySelector('[data-chart-series-toggle]'))");
        if (!largeToggleAvailable) fail("large frontier did not expose a KPI toggle");
        await client.evaluate("document.querySelector('[data-chart-series-toggle]').click()");
        await waitForBrowser(client, "location.search.includes('hidden_metric=') && document.getElementById('metric-plot-card')?.getAttribute('aria-busy') === 'false'", "large chart fragment");
        const chartFragmentMs = performance.now() - largeToggleStarted;
        const chartFragmentBytes = await client.evaluate("(() => { const entries = performance.getEntriesByType('resource').filter((entry) => entry.name.includes('/chart?')); return entries.at(-1)?.decodedBodySize ?? 0; })()");
        if (chartFragmentMs > 1_000) fail("large chart fragment exceeded 1000 ms");
        if (chartFragmentBytes > 1_100_000) fail("large chart fragment exceeded 1.1 MB");

        await client.send("Emulation.setDeviceMetricsOverride", { width: 360, height: 800, deviceScaleFactor: 1, mobile: true });
        const mobileScreenshots = {};
        for (const name of ["tags", "metrics", "frontier"]) {
            await client.navigate(pages[name]);
            assertPageIntegrity(await client.evaluate(containmentProbe()));
            mobileScreenshots[name] = await screenshot(client, `large-mobile-${name}`);
        }
        return {
            fixture,
            timings,
            warmTimings,
            chartFragment: { completeMs: chartFragmentMs, decodedBytes: chartFragmentBytes },
            desktopScreenshots,
            mobileScreenshots,
        };
    } finally {
        browser.stop();
        server.stop();
    }
}

function cleanup() {
    for (const child of children) child.kill("SIGKILL");
    if (!process.env.FIDGET_SPINNER_UI_KEEP_WORKSPACE) rmSync(workspace, { recursive: true, force: true });
}

process.on("SIGINT", () => { cleanup(); process.exit(130); });
process.on("SIGTERM", () => { cleanup(); process.exit(143); });

mkdirSync(artifactRoot, { recursive: true });
try {
    const executable = cargoBinary(largeDatabase !== null);
    const mock = await exerciseMock(executable);
    const large = largeDatabase ? await exerciseLarge(executable, largeDatabase) : null;
    console.log(JSON.stringify({ mock, large, artifacts: artifactRoot }, null, 2));
} finally {
    cleanup();
}
