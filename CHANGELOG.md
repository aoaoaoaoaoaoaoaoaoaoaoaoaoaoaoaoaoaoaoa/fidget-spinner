# Changelog

## Unreleased

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
  source, removing the retired Swarm dependency.

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
