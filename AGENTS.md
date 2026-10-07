fidget_spinner: local-first, agent-first frontier ledger for autonomous optimization work

## invariants
- No OAuth, hosted control plane, mandatory cloud resources, or managed-compute marketplace in core design
- Frontier = scope and grounding, not graph vertex; hypotheses and experiments are the vertices
- Every experiment has exactly one owning hypothesis
- Per-project state: centralized per-user SQLite under platform state dir (`$XDG_STATE_HOME/fidget-spinner/` on Linux)
- Frontier brief: the one sanctioned freeform overview; `frontier.open`: the only sanctioned overview surface
- Slow intentional traversal over giant context dumps
- Pin exact stable toolchain in `rust-toolchain.toml`

## routes
- Changing MCP output, schemas, serialization, or telemetry → `../mcp_depot/TOKEN_POLICY.md`
