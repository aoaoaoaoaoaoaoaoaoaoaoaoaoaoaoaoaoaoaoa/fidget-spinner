# Fidget Spinner

Fidget Spinner is a local-first, agent-first experimental DAG for autonomous
program optimization, source capture, and experiment adjudication.

Constraints that are part of the product:

- no OAuth
- no hosted control plane
- no mandatory cloud resources
- no managed-compute marketplace in the core design
- DAG is canonical truth
- frontier state is a derived projection
- per-project state lives under `.fidget_spinner/`
- project payload schemas are local and warning-heavy, not globally rigid
- off-path nodes should remain cheap
- core-path work should remain hypothesis-owned and experiment-gated

Engineering posture:

- root `Cargo.toml` owns lint policy and canonical check commands
- every crate opts into `[lints] workspace = true`
- pin an exact stable toolchain in `rust-toolchain.toml`
- keep runners thin and orchestration-only
- prefer precise domain types over loose bags of strings

MVP target:

- dogfood against `libgrid` worktrees
- replace sprawling freeform experiment markdown with structured
  contract/hypothesis/run/analysis/decision nodes plus cheap source/note side paths
- make runs, comparisons, artifacts, and code snapshots first-class
- bundle the frontier-loop skill with the MCP surface instead of treating it as
  folklore
