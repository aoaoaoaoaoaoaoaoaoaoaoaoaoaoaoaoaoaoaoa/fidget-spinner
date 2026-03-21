# Fidget Spinner

Fidget Spinner is a local-first frontier ledger for long-running optimization
work.

It is intentionally not a general research notebook. It is a hard scientific
spine:

- `frontier` is scope and grounding, not a graph node
- `hypothesis` is a real graph vertex
- `experiment` is a real graph vertex with one mandatory owning hypothesis
- influence edges form a sparse DAG over that canonical tree spine
- `artifact` is an external reference only; Spinner never reads artifact bodies

The product goal is token austerity. `frontier.open` is the only sanctioned
overview dump. Everything else should require deliberate traversal one object at
a time.

## Current Model

The ledger has four first-class object families:

- `frontier`
  - a named scope
  - owns one mutable `brief`
  - partitions hypotheses and experiments
- `hypothesis`
  - terse claim or intervention
  - title + summary + exactly one paragraph of body
- `experiment`
  - open or closed
  - belongs to exactly one hypothesis
  - may cite other hypotheses or experiments as influences
  - when closed, stores dimensions, metrics, verdict, rationale, and optional analysis
- `artifact`
  - reference to an external document, link, log, plot, table, dump, or binary
  - attaches to frontiers, hypotheses, or experiments
  - only metadata and locator live in Spinner
  - Spinner never reads the body

There are no canonical freeform `note` or `source` nodes anymore. If a piece of
text does not belong in a frontier brief, hypothesis, or experiment analysis, it
probably belongs outside Spinner as an artifact.

## Design Rules

- `frontier.open` is the only overview surface.
- No broad prose dumps in list-like tools.
- Artifact bodies are never read through Spinner.
- Live metrics are derived, not manually curated.
- Selectors are permissive: UUID or slug, one field, no parallel `_id` / `_slug`.
- Slow intentional graph walking is preferred to burning context on giant feeds.

## Local Install

Install the release CLI into `~/.local/bin` and refresh the bundled skill
symlinks in `~/.codex/skills`:

```bash
./scripts/install-local.sh
```

The installed binary is `~/.local/bin/fidget-spinner-cli`.

The installer also installs a user systemd service for the libgrid navigator at
`http://127.0.0.1:8913/` and refreshes it on every reinstall:

```bash
systemctl --user status fidget-spinner-libgrid-ui.service
journalctl --user -u fidget-spinner-libgrid-ui.service -f
```

You can override the default service target for one install with:

```bash
FIDGET_SPINNER_UI_PROJECT=/abs/path/to/project ./scripts/install-local.sh
```

## Quickstart

Initialize a project:

```bash
cargo run -p fidget-spinner-cli -- init --project . --name libgrid
```

Register the tag, metric, and run-dimension vocabulary before heavy ingest:

```bash
cargo run -p fidget-spinner-cli -- tag add \
  --project . \
  --name root-conquest \
  --description "Root-cash-out work"
```

```bash
cargo run -p fidget-spinner-cli -- metric define \
  --project . \
  --key nodes_solved \
  --unit count \
  --objective maximize \
  --visibility canonical \
  --description "Solved search nodes on the target rail"
```

```bash
cargo run -p fidget-spinner-cli -- dimension define \
  --project . \
  --key instance \
  --value-type string \
  --description "Workload slice"
```

Create a frontier:

```bash
cargo run -p fidget-spinner-cli -- frontier create \
  --project . \
  --label "native mip" \
  --objective "Drive braid-rail LP cash-out" \
  --slug native-mip
```

Write the frontier brief:

```bash
cargo run -p fidget-spinner-cli -- frontier update-brief \
  --project . \
  --frontier native-mip \
  --situation "Root LP spend is understood; node-local LP churn is the active frontier."
```

Record a hypothesis:

```bash
cargo run -p fidget-spinner-cli -- hypothesis record \
  --project . \
  --frontier native-mip \
  --slug node-local-loop \
  --title "Node-local logical cut loop" \
  --summary "Push cut cash-out below root." \
  --body "Thread node-local logical cuts through native LP reoptimization so the same intervention can cash out below root on parity rails without corrupting root ownership semantics." \
  --tag root-conquest
```

Open an experiment:

```bash
cargo run -p fidget-spinner-cli -- experiment open \
  --project . \
  --hypothesis node-local-loop \
  --slug parity-20s \
  --title "Parity rail 20s" \
  --summary "Live challenger on the canonical braid slice." \
  --tag root-conquest
```

Close an experiment:

```bash
cargo run -p fidget-spinner-cli -- experiment close \
  --project . \
  --experiment parity-20s \
  --backend manual \
  --argv matched-lp-site-traces \
  --dimension instance=4x5-braid \
  --primary-metric nodes_solved=273 \
  --verdict accepted \
  --rationale "Matched LP site traces isolate node reoptimization as the dominant native LP sink."
```

Record an external artifact by reference:

```bash
cargo run -p fidget-spinner-cli -- artifact record \
  --project . \
  --kind document \
  --slug lp-review-doc \
  --label "LP review tranche" \
  --summary "External markdown tranche." \
  --locator /abs/path/to/review.md \
  --attach hypothesis:node-local-loop
```

Inspect live metrics:

```bash
cargo run -p fidget-spinner-cli -- metric keys --project . --frontier native-mip --scope live
```

```bash
cargo run -p fidget-spinner-cli -- metric best \
  --project . \
  --frontier native-mip \
  --hypothesis node-local-loop \
  --key nodes_solved
```

## MCP Surface

Serve the MCP host:

```bash
cargo run -p fidget-spinner-cli -- mcp serve
```

If the host starts unbound, bind it with:

```json
{"name":"project.bind","arguments":{"path":"<project-root-or-nested-path>"}}
```

The main model-facing tools are:

- `system.health`
- `system.telemetry`
- `project.bind`
- `project.status`
- `tag.add`
- `tag.list`
- `frontier.create`
- `frontier.list`
- `frontier.read`
- `frontier.open`
- `frontier.brief.update`
- `frontier.history`
- `hypothesis.record`
- `hypothesis.list`
- `hypothesis.read`
- `hypothesis.update`
- `hypothesis.history`
- `experiment.open`
- `experiment.list`
- `experiment.read`
- `experiment.update`
- `experiment.close`
- `experiment.history`
- `artifact.record`
- `artifact.list`
- `artifact.read`
- `artifact.update`
- `artifact.history`
- `metric.define`
- `metric.keys`
- `metric.best`
- `run.dimension.define`
- `run.dimension.list`

`frontier.open` is the grounding call. It returns:

- frontier brief
- active tags
- live metric keys
- active hypotheses with deduped current state
- open experiments

Everything deeper should be fetched by explicit selector.

## Navigator

Serve the local navigator:

```bash
cargo run -p fidget-spinner-cli -- ui serve --path . --bind 127.0.0.1:8913
```

`ui serve --path` accepts:

- the project root
- `.fidget_spinner/`
- any descendant inside `.fidget_spinner/`
- a parent containing exactly one descendant store

The navigator mirrors the product philosophy:

- root page lists frontiers
- frontier page is the only overview
- hypothesis / experiment / artifact pages are detail reads
- local navigation happens card-to-card
- artifact bodies are never surfaced

## Store Layout

Each initialized project gets:

```text
.fidget_spinner/
    project.json
    state.sqlite
```

In git-backed projects `.fidget_spinner/` normally belongs in `.gitignore` or
`.git/info/exclude`.

## Doctrine

- hypotheses are short and disciplined
- experiments carry the real scientific record
- verdicts are explicit: `accepted`, `kept`, `parked`, `rejected`
- artifacts keep large text and dumps off the token hot path
- live metrics answer “what matters now?”, not “what has ever existed?”
- the ledger is about experimental truth, not recreating git inside the database
