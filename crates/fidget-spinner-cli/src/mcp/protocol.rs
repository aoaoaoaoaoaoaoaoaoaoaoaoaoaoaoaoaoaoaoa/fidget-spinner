use std::path::PathBuf;

use libmcp::HostSessionKernelSnapshot;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::mcp::telemetry::ServerTelemetry;

pub(crate) const PROTOCOL_VERSION: &str = "2025-11-25";
pub(crate) const SERVER_NAME: &str = "fidget-spinner";
pub(crate) const HOST_STATE_ENV: &str = "FIDGET_SPINNER_MCP_HOST_STATE";
pub(crate) const FORCE_ROLLOUT_ENV: &str = "FIDGET_SPINNER_MCP_TEST_FORCE_ROLLOUT_KEY";
pub(crate) const CRASH_ONCE_ENV: &str = "FIDGET_SPINNER_MCP_TEST_HOST_CRASH_ONCE_KEY";
pub(crate) const TRANSIENT_ONCE_ENV: &str = "FIDGET_SPINNER_MCP_TEST_WORKER_TRANSIENT_ONCE_KEY";
pub(crate) const TRANSIENT_ONCE_MARKER_ENV: &str =
    "FIDGET_SPINNER_MCP_TEST_WORKER_TRANSIENT_ONCE_MARKER";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct HostStateSeed {
    pub session_kernel: HostSessionKernelSnapshot,
    pub telemetry: ServerTelemetry,
    pub next_request_id: u64,
    pub binding: Option<ProjectBindingSeed>,
    pub worker_generation: u64,
    pub force_rollout_consumed: bool,
    pub crash_once_consumed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ProjectBindingSeed {
    pub requested_path: PathBuf,
    pub project_root: PathBuf,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(crate) struct HostRequestId(pub u64);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum WorkerRequest {
    Execute {
        id: HostRequestId,
        operation: WorkerOperation,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum WorkerOperation {
    CallTool { name: String, arguments: Value },
    ReadResource { uri: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct WorkerResponse {
    pub id: HostRequestId,
    pub outcome: WorkerOutcome,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum WorkerOutcome {
    Success {
        result: Value,
    },
    Fault {
        fault: crate::mcp::fault::FaultRecord,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct BinaryFingerprint {
    pub length_bytes: u64,
    pub modified_unix_nanos: u128,
}

#[derive(Clone, Debug)]
pub(crate) struct WorkerSpawnConfig {
    pub executable: PathBuf,
}
