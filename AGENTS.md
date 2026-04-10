# Fidget Spinner

Fidget Spinner is a local-first, agent-first frontier ledger for autonomous
optimization work.

Constraints that are part of the product:

- no OAuth
- no hosted control plane
- no mandatory cloud resources
- no managed-compute marketplace in the core design
- frontier is scope and grounding, not a graph vertex
- hypotheses and experiments are the true graph vertices
- every experiment has one mandatory owning hypothesis
- per-project state lives in centralized per-user SQLite under `~/.local/state/fidget-spinner/`
- the frontier brief is the one sanctioned freeform overview
- artifacts are references only; Spinner never reads artifact bodies
- slow intentional traversal beats giant context dumps
- `frontier.open` is the only sanctioned overview surface

Engineering posture:

- root `Cargo.toml` owns lint policy and canonical check commands
- every crate opts into `[lints] workspace = true`
- pin an exact stable toolchain in `rust-toolchain.toml`
- keep runners thin and orchestration-only
- prefer precise domain types over loose bags of strings

MVP target:

- dogfood against `libgrid` worktrees
- replace sprawling freeform experiment markdown with structured
  frontier/hypothesis/experiment records plus artifact references
- make live metrics and influence lineage discoverable without giant dumps
- bundle the frontier-loop skill with the MCP surface instead of treating it as
  folklore
