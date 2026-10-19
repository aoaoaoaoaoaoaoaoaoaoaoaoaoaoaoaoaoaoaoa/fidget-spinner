# Changelog

## Unreleased

- Dresses the navigator in the board chrome it shares with Vauban: a favicon, the
  product header and place, a landing table of frontiers with settlement bars and
  counts, state colours, hover cards and foreground records.
- Repairs frontier boards that stayed at "laying out…": their board requests carried
  a doubled query mark. A board that cannot load now shows the refusal.
- Titles cells by hypothesis and counts their trials; shows KPI tiles with the
  incumbent value, charts only from two readings on, and trial readings with their
  change against the baseline.
- Replaces Recent epochs and Show all settled with one Settled control.
- Archives concluded frontiers, through an `archive` operation or the index's Archive
  button: an archived frontier leaves the index and the candidates for an omitted
  frontier until it reopens, and keeps its records. Refusals of an omitted frontier
  mark each candidate active or concluded.
- Retires the frontier-loop skill. The installer ships each skill under
  `assets/codex-skills` and removes the skills it installed that it no longer ships.

## 3.0.0 - 2026-10-14

- Replaces the implementation with a pure domain core and a revision-pinned
  trebuchet engine for ledger, inspection and navigator mechanisms.
- Replaces experiments with append-only trials and verdicts with dispositions.
  Renames measure to reading, break to regime change, current baseline to
  incumbent baseline, and due to docket membership.
- Removes situation, walk, experiment open/close, expected yield, confidence,
  attention, conditions, tags, metric registries and locks, project operations,
  command/backend capture, clean-worktree capture, SQL, history snapshots and
  unknowns. Summaries and work states are computed.
- Replaces the 42-tool MCP surface with six tools: patch, status, hypothesis and
  review packets, history and health. The CLI binary is `fidget-spinner`.
- Stores every project's frontiers in one format-21 `ledger.sqlite3`.
  Old per-project stores are left untouched and are not migrated in place.
  Timestamped operation logs can be imported into new frontiers.
- Replaces obsolete browser and portability gates with an isolated CLI proof.
  Hosted CI is removed while trebuchet remains a local Git dependency.

## 1.0.9 - 2026-08-24

- Raises the pinned Rust toolchain and minimum supported Rust version to 1.98.0.

## 1.0.8 - 2026-08-15

- Makes project-discovery proofs independent of Cargo target placement.
- Preserves the underlying I/O cause when a live rollout retains its incumbent.

## 1.0.7 - 2026-08-15

- Updates libmcp for bounded Windows telemetry rollover.

## 1.0.6 - 2026-08-15

- Declares libmcp rollover contracts for the MCP tool catalog.
- Uses the binding response as the authority in the destructive-reseed
  portability scenario.

## 1.0.1 - 2026-08-10

- Carries the CLI, MCP server, bundled skills, and local navigator across
  x86_64 Linux, Apple silicon and Intel Macs, and x86_64 Windows.
- Uses native per-user state roots on macOS and Windows while retaining XDG
  state semantics on Linux.
- Proves Cargo installation, project reopening, navigator first contact, skill
  installation, removal, and state preservation on every release host.
- Replaces Plotters with a dependency-free semantic SVG chart planned and
  rendered in Rust.
- Adds direct series toggles, linked datum tooltips, horizontal drag zoom,
  dynamic time and byte units, and fragment-speed Results transitions while
  preserving PNG clipboard export.
- Replaces per-series store walks with one canonical, set-oriented chart scene
  cached by database refresh token.
- Bounds initial experiment tables to 250 matching rows and extends the real
  browser gate across chart interaction, clipboard export, hostile geometry,
  and the largest non-archived frontier in a supplied ledger.
- Publishes local binary upgrades atomically and makes idle Unix MCP hosts
  adopt stable successor inodes proactively without losing stdio, session
  state, binding, request history, or telemetry.
- Moves lossless timed frame polling into libmcp and pins the official GitHub
  source.

## 1.0.0 - 2026-08-02

First supported release.

- Establishes the local-first frontier, hypothesis, experiment, metric, KPI,
  condition, and supervisory-tag contracts.
- Ships the stdio MCP server, command-line supervisor, local navigator, and
  bundled Codex skills as one source release.
- Upgrades the host/worker runtime to libmcp 2.0.2 and pins Rust 1.97.1.
- Makes store creation and migrations atomic through store format 20.
- Hardens browser mutation authority, remote binding, typed faults, and bounded
  telemetry.
- Adds real-browser UI journeys, hostile narrow-layout probes, and cold/warm
  large-ledger performance budgets.
- Makes per-user installation self-contained, XDG-aware, ownership-proving,
  non-destructive on foreign paths, and reversibly tested.
