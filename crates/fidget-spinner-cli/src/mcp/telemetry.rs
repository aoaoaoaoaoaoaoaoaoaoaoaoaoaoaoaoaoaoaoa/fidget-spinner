use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::mcp::fault::FaultRecord;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct OperationTelemetry {
    pub requests: u64,
    pub successes: u64,
    pub errors: u64,
    pub retries: u64,
    pub last_latency_ms: Option<u128>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ServerTelemetry {
    pub requests: u64,
    pub successes: u64,
    pub errors: u64,
    pub retries: u64,
    pub worker_restarts: u64,
    pub host_rollouts: u64,
    pub last_fault: Option<FaultRecord>,
    pub operations: BTreeMap<String, OperationTelemetry>,
}

impl ServerTelemetry {
    pub fn record_request(&mut self, operation: &str) {
        self.requests += 1;
        self.operations
            .entry(operation.to_owned())
            .or_default()
            .requests += 1;
    }

    pub fn record_success(&mut self, operation: &str, latency_ms: u128) {
        self.successes += 1;
        let entry = self.operations.entry(operation.to_owned()).or_default();
        entry.successes += 1;
        entry.last_latency_ms = Some(latency_ms);
    }

    pub fn record_retry(&mut self, operation: &str) {
        self.retries += 1;
        self.operations
            .entry(operation.to_owned())
            .or_default()
            .retries += 1;
    }

    pub fn record_error(&mut self, operation: &str, fault: FaultRecord, latency_ms: u128) {
        self.errors += 1;
        self.last_fault = Some(fault.clone());
        let entry = self.operations.entry(operation.to_owned()).or_default();
        entry.errors += 1;
        entry.last_latency_ms = Some(latency_ms);
    }

    pub fn record_worker_restart(&mut self) {
        self.worker_restarts += 1;
    }

    pub fn record_rollout(&mut self) {
        self.host_rollouts += 1;
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct InitializationHealth {
    pub ready: bool,
    pub seed_captured: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct WorkerHealth {
    pub worker_generation: u64,
    pub alive: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct BinaryHealth {
    pub current_executable: String,
    pub launch_path_stable: bool,
    pub rollout_pending: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct BindingHealth {
    pub bound: bool,
    pub requested_path: Option<String>,
    pub project_root: Option<String>,
    pub state_root: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct HealthSnapshot {
    pub initialization: InitializationHealth,
    pub binding: BindingHealth,
    pub worker: WorkerHealth,
    pub binary: BinaryHealth,
    pub last_fault: Option<FaultRecord>,
}
