# Fidget Spinner

Fidget Spinner records experimental inquiry in a local, append-only ledger.
Hypotheses carry claims, trials record evidence, and dispositions settle hypotheses.
Baselines, epochs and regimes track comparisons under a declared measurement gauge.
Reviews collect lessons across hypotheses.

The `fidget-spinner` binary provides a stdio MCP server, a CLI and a loopback
navigator. The pure Rust core owns the domain rules; the revision-pinned
`trebuchet` engine supplies shared ledger, inspection and board mechanisms.

## Build and use

The current source depends on a local `trebuchet` Git repository at the path
declared in `Cargo.toml`. Hosted builds and source installation elsewhere require
that dependency to be made available first.

```sh
cargo build --locked --package fidget-spinner
cargo run --locked --package fidget-spinner -- mcp serve
```

The CLI accepts JSON operation arrays from a file or stdin:

```sh
printf '%s\n' '[{"op":"frontier","objective":"Reduce solver latency"}]' |
  fidget-spinner patch --project . --frontier solver
fidget-spinner status --project . --frontier solver
fidget-spinner history --project . --frontier solver
fidget-spinner ui serve --bind 127.0.0.1:8913
```

Use `--help` for `read`, `import`, `health` and inspection options.
The six MCP tools and the operation language are specified in [SPEC.md](SPEC.md);
[GLOSSARY.md](GLOSSARY.md) and [FORMAL_SEMANTICS.md](FORMAL_SEMANTICS.md) define the domain.

## Install

On Linux, `./scripts/install-local.sh` builds the binary and installs the bundled
Codex skills and navigator user service. It requires Cargo, Git and `jq`.
Set `FIDGET_SPINNER_INSTALL_SYSTEMD=0` to omit service integration.
`./scripts/install-local.sh --uninstall` removes owned installation artifacts
without deleting ledger state.

## State

On Linux, all projects share
`${XDG_STATE_HOME:-~/.local/state}/fidget-spinner/ledger.sqlite3`, in store format 21.
Other platforms use their native per-user state directory.
A project's identity is its canonical repository root; linked worktrees share it.

Version 3 replaces the previous implementation. Old per-project stores are
neither opened nor migrated in place. Timestamped `{at,ops}` JSONL can be imported
into a new frontier with `fidget-spinner import`.

## Verify

The gate script requires `uv`.

`./check.py check` (also `verify`) runs formatting, Clippy and Rust tests without
rewriting sources. `./check.py deep` adds rustdoc and an isolated CLI journey;
it launches neither a browser nor the installer. `fix` applies the configured
canonicalization commands; `install` invokes the local installer.

Fidget Spinner is MIT-licensed.
