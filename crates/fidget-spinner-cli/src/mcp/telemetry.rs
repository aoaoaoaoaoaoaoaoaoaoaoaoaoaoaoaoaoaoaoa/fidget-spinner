use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::mcp::fault::FaultRecord;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct OperationTelemetry {
    pub requests: u64,
    pub successes: u64,
    pub errors: u64,
    pub retries: u64,
    pub last_latency_ms: Option<u128>,
    #[serde(default)]
    pub fault_codes: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ServerTelemetry {
    #[serde(default = "OffsetDateTime::now_utc", with = "time::serde::rfc3339")]
    pub window_started_at: OffsetDateTime,
    pub requests: u64,
    pub successes: u64,
    pub errors: u64,
    pub retries: u64,
    pub worker_restarts: u64,
    pub host_rollouts: u64,
    pub last_fault: Option<FaultRecord>,
    pub operations: BTreeMap<String, OperationTelemetry>,
}

const MAX_OPERATION_BUCKETS: usize = 128;

impl Default for ServerTelemetry {
    fn default() -> Self {
        Self {
            window_started_at: OffsetDateTime::now_utc(),
            requests: 0,
            successes: 0,
            errors: 0,
            retries: 0,
            worker_restarts: 0,
            host_rollouts: 0,
            last_fault: None,
            operations: BTreeMap::new(),
        }
    }
}

impl ServerTelemetry {
    fn operation(&mut self, operation: &str) -> &mut OperationTelemetry {
        let bucket = if self.operations.contains_key(operation)
            || self.operations.len() < MAX_OPERATION_BUCKETS
        {
            operation
        } else {
            "other"
        };
        self.operations.entry(bucket.to_owned()).or_default()
    }

    pub fn record_request(&mut self, operation: &str) {
        self.requests += 1;
        self.operation(operation).requests += 1;
    }

    pub fn record_success(&mut self, operation: &str, latency_ms: u128) {
        self.successes += 1;
        let entry = self.operation(operation);
        entry.successes += 1;
        entry.last_latency_ms = Some(latency_ms);
    }

    pub fn record_retry(&mut self, operation: &str) {
        self.retries += 1;
        self.operation(operation).retries += 1;
    }

    pub fn record_error(&mut self, operation: &str, fault: FaultRecord, latency_ms: u128) {
        self.errors += 1;
        self.last_fault = Some(fault.clone());
        let code = fault.code.clone();
        let entry = self.operation(operation);
        entry.errors += 1;
        entry.last_latency_ms = Some(latency_ms);
        *entry.fault_codes.entry(code).or_default() += 1;
    }

    pub fn record_worker_restart(&mut self) {
        self.worker_restarts += 1;
    }

    pub fn record_rollout(&mut self) {
        self.host_rollouts += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_OPERATION_BUCKETS, ServerTelemetry};
    use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};

    #[test]
    fn telemetry_retains_bounded_coded_desire_path_evidence() {
        let mut telemetry = ServerTelemetry::default();
        for index in 0..=MAX_OPERATION_BUCKETS {
            telemetry.record_request(&format!("unknown:{index}"));
        }
        let fault = FaultRecord::new(
            FaultKind::InvalidInput,
            FaultStage::Protocol,
            "unknown:0",
            "bad arguments",
        );
        telemetry.record_error("unknown:0", fault, 1);

        assert_eq!(telemetry.operations.len(), MAX_OPERATION_BUCKETS + 1);
        assert_eq!(telemetry.operations["other"].requests, 1);
        assert_eq!(
            telemetry.operations["unknown:0"].fault_codes["invalid_protocol_input"],
            1
        );
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
