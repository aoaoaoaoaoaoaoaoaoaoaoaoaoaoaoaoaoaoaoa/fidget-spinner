# Fidget Spinner

Local-first experiment ledger for agents doing long optimization runs.

Not a notebook. Not a diary. Not a cloud service.

## Shape

- `frontier`: scope, brief, KPIs
- `hypothesis`: cheap KPI-moving idea
- `experiment`: measured trial owned by one hypothesis

Closed experiments require a clean git worktree and record `HEAD`.

State lives under:

```text
~/.local/state/fidget-spinner/projects/
```

## Install

```bash
./scripts/install-local.sh
```

Installs:

- `~/.local/bin/fidget-spinner-cli`
- Codex skills
- user service at `http://127.0.0.1:8913/`

## Use

```bash
fidget-spinner-cli init --project . --name my-project
fidget-spinner-cli mcp serve
```

In MCP, start with:

```text
system.health
project.bind
frontier.open
```

Then walk deliberately by selector.

## Doctrine

Open hypotheses eagerly. Open experiments only for KPI-directed work. Commit
fast before closing. Let git hold implementation state; let Spinner hold
experimental truth.
