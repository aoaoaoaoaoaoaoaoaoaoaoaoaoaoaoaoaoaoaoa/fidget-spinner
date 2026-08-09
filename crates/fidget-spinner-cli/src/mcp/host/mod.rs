//! Stable host process that owns the MCP session and routes store work to a disposable worker.

mod config;
mod process;
mod runtime;

pub(crate) use runtime::run_host as serve;
