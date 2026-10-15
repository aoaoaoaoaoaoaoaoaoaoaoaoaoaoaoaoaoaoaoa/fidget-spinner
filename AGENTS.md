fidget-spinner: local inquiry ledger, MCP server and navigator; trebuchet owns shared mechanisms

## gate
- `./check.py check` after edits; `./check.py verify` for non-mutating gate
- `./check.py deep` for docs and isolated CLI proof; `./scripts/install-local.sh` for installation

## invariants
- Keep `crates/fidget-spinner-core` pure; I/O and transport belong in `crates/fidget-spinner`
- Ledger ops replay forever: never change an existing op's meaning; new meaning needs new op or new store format with migration
- Store format 21 at `$XDG_STATE_HOME/fidget-spinner/ledger.sqlite3`; tests use isolated stores, never live state
- Pin trebuchet by Git rev in `Cargo.toml`; keep shared mechanisms out of Spinner's domain core

## routes
- Changing domain names → `GLOSSARY.md`
- Changing domain rules → `FORMAL_SEMANTICS.md`
- Changing addressing, operations, inspection, navigator or persistence → `SPEC.md`
- Changing MCP output, schemas, serialization or telemetry → `../mcp_depot/TOKEN_POLICY.md`
