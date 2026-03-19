use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::OffsetDateTime;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum FaultKind {
    InvalidInput,
    NotInitialized,
    Unavailable,
    Transient,
    Internal,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum FaultStage {
    Host,
    Worker,
    Store,
    Transport,
    Protocol,
    Rollout,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FaultRecord {
    pub kind: FaultKind,
    pub stage: FaultStage,
    pub operation: String,
    pub message: String,
    pub retryable: bool,
    pub retried: bool,
    pub worker_generation: Option<u64>,
    pub occurred_at: OffsetDateTime,
}

impl FaultRecord {
    #[must_use]
    pub fn new(
        kind: FaultKind,
        stage: FaultStage,
        operation: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            stage,
            operation: operation.into(),
            message: message.into(),
            retryable: false,
            retried: false,
            worker_generation: None,
            occurred_at: OffsetDateTime::now_utc(),
        }
    }

    #[must_use]
    pub fn retryable(mut self, worker_generation: Option<u64>) -> Self {
        self.retryable = true;
        self.worker_generation = worker_generation;
        self
    }

    #[must_use]
    pub fn mark_retried(mut self) -> Self {
        self.retried = true;
        self
    }

    #[must_use]
    pub fn into_jsonrpc_error(self) -> Value {
        json!({
            "code": self.jsonrpc_code(),
            "message": self.message.clone(),
            "data": self,
        })
    }

    #[must_use]
    pub fn into_tool_result(self) -> Value {
        json!({
            "content": [{
                "type": "text",
                "text": self.message,
            }],
            "structuredContent": self,
            "isError": true,
        })
    }

    #[must_use]
    pub const fn jsonrpc_code(&self) -> i64 {
        match self.kind {
            FaultKind::InvalidInput => -32602,
            FaultKind::NotInitialized => -32002,
            FaultKind::Unavailable => -32004,
            FaultKind::Transient | FaultKind::Internal => -32603,
        }
    }
}
