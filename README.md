# Fidget Spinner

Fidget Spinner is a local system of record for long-running experimental work.
A frontier scopes the hill, hypotheses hold KPI-moving ideas, and every
experiment tests one owning hypothesis. Git owns implementation state; Spinner
owns experimental truth.

It is not a notebook, task tracker, cloud service, or parallel source archive.

## Install

Fidget Spinner is release-tested on x86_64 Linux, Apple silicon and Intel Macs,
and x86_64 Windows. Install the unsigned command-line release from its signed
source tag with Cargo:

```console
cargo install --git https://github.com/aoaoaoaoaoaoaoaoaoaoaoaoaoaoaoaoaoaoaoa/fidget-spinner.git \
  --tag v1.0.4 --locked fidget-spinner-cli
fidget-spinner-cli skill install
```

The enhanced Linux installer also installs the bundled Codex skills and a
systemd user service for the navigator. It requires the pinned Rust toolchain,
Cargo, Git, and `jq`:

```bash
./scripts/install-local.sh
```

It upgrades owned artifacts atomically and refuses foreign paths. State
survives uninstall:

```bash
./scripts/install-local.sh --uninstall
```

Set `FIDGET_SPINNER_INSTALL_SYSTEMD=0` to omit service integration. The
checked-in `.mcp-depot.toml` provides an optional eager binary path; standalone
installation remains complete.

## Use

```bash
fidget-spinner-cli init --project . --name my-project
fidget-spinner-cli project status --project .
fidget-spinner-cli mcp serve --project .
```

An unbound MCP session starts with `system.health`, then `project.bind`. Ground
through `frontier.open`; use selector reads for detail. The bundled
[`fidget-spinner`](assets/codex-skills/fidget-spinner/SKILL.md) skill owns the
agent workflow. [`frontier-loop`](assets/codex-skills/frontier-loop/SKILL.md)
adds an indefinite measured loop.

The navigator listens at <http://127.0.0.1:8913/>. Run it directly with:

```bash
fidget-spinner-cli ui serve --bind 127.0.0.1:8913
```

## State And Authority

Project stores live beneath the operating system's per-user state directory at
`fidget-spinner/projects/`. On Linux this is
`${XDG_STATE_HOME:-~/.local/state}/fidget-spinner/projects/`; macOS uses the
user Application Support directory and Windows uses local AppData.
`FIDGET_SPINNER_STATE_HOME` accepts an absolute test or deployment root.

Non-scuffed experiment closure requires a clean Git worktree and records
`HEAD`. Format 20 opens formats 9 through 19 through transactional migrations;
unknown formats fail closed. Back up `state.sqlite` with Spinner stopped before
a format upgrade.

The navigator is an unauthenticated supervisor surface. It binds loopback and
enforces browser origin checks by default. `--allow-remote` adds no
authentication; use it only behind a trusted access-controlled boundary.

Operator contracts live in [operations](docs/operations.md). Domain policy
lives in [metric and KPI governance](docs/metric-kpi-governance.md) and
[supervisory tag governance](docs/supervisory-tag-governance.md). The navigator
design contract lives in [SPEC.md](SPEC.md).

## Verify

```bash
./check.py check
./check.py deep
```

`check` runs the source policy, formatter, Clippy, and Rust tests. `deep` adds
rustdoc, a real Chromium journey, and isolated install/uninstall verification.
Version 1.0 freezes the documented CLI, MCP JSON, and store compatibility
contracts for the 1.x line. Fidget Spinner is MIT-licensed.
