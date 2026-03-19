use std::io::{self, BufRead, Write};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;

use serde::Serialize;
use serde_json::{Value, json};

use super::{
    binary::BinaryRuntime,
    config::HostConfig,
    process::{ProjectBinding, WorkerSupervisor},
};
use crate::mcp::catalog::{
    DispatchTarget, ReplayContract, list_resources, resource_spec, tool_definitions, tool_spec,
};
use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};
use crate::mcp::protocol::{
    CRASH_ONCE_ENV, FORCE_ROLLOUT_ENV, HOST_STATE_ENV, HostRequestId, HostStateSeed,
    PROTOCOL_VERSION, ProjectBindingSeed, SERVER_NAME, SessionSeed, WorkerOperation,
    WorkerSpawnConfig,
};
use crate::mcp::telemetry::{
    BinaryHealth, BindingHealth, HealthSnapshot, InitializationHealth, ServerTelemetry,
    WorkerHealth,
};

pub(crate) fn run_host(
    initial_project: Option<PathBuf>,
) -> Result<(), fidget_spinner_store_sqlite::StoreError> {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut host = HostRuntime::new(HostConfig::new(initial_project)?)?;

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => {
                eprintln!("mcp stdin failure: {error}");
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }

        let maybe_response = host.handle_line(&line);
        if let Some(response) = maybe_response {
            write_message(&mut stdout, &response)?;
        }
        host.maybe_roll_forward()?;
    }

    Ok(())
}

struct HostRuntime {
    config: HostConfig,
    binding: Option<ProjectBinding>,
    session: SessionSeed,
    telemetry: ServerTelemetry,
    next_request_id: u64,
    worker: WorkerSupervisor,
    binary: BinaryRuntime,
    force_rollout_key: Option<String>,
    force_rollout_consumed: bool,
    rollout_requested: bool,
    crash_once_key: Option<String>,
    crash_once_consumed: bool,
}

impl HostRuntime {
    fn new(config: HostConfig) -> Result<Self, fidget_spinner_store_sqlite::StoreError> {
        let restored = restore_host_state();
        let session = restored
            .as_ref()
            .map_or_else(SessionSeed::default, |seed| seed.session.clone());
        let telemetry = restored
            .as_ref()
            .map_or_else(ServerTelemetry::default, |seed| seed.telemetry.clone());
        let next_request_id = restored
            .as_ref()
            .map_or(1, |seed| seed.next_request_id.max(1));
        let worker_generation = restored.as_ref().map_or(0, |seed| seed.worker_generation);
        let force_rollout_consumed = restored
            .as_ref()
            .is_some_and(|seed| seed.force_rollout_consumed);
        let crash_once_consumed = restored
            .as_ref()
            .is_some_and(|seed| seed.crash_once_consumed);
        let binding = restored
            .as_ref()
            .and_then(|seed| seed.binding.clone().map(ProjectBinding::from))
            .or(config
                .initial_project
                .clone()
                .map(resolve_project_binding)
                .transpose()?
                .map(|resolved| resolved.binding));

        let worker = {
            let mut worker = WorkerSupervisor::new(
                WorkerSpawnConfig {
                    executable: config.executable.clone(),
                },
                worker_generation,
            );
            if let Some(project_root) = binding.as_ref().map(|binding| binding.project_root.clone())
            {
                worker.rebind(project_root);
            }
            worker
        };

        Ok(Self {
            config: config.clone(),
            binding,
            session,
            telemetry,
            next_request_id,
            worker,
            binary: BinaryRuntime::new(config.executable.clone())?,
            force_rollout_key: std::env::var(FORCE_ROLLOUT_ENV).ok(),
            force_rollout_consumed,
            rollout_requested: false,
            crash_once_key: std::env::var(CRASH_ONCE_ENV).ok(),
            crash_once_consumed,
        })
    }

    fn handle_line(&mut self, line: &str) -> Option<Value> {
        let message = match serde_json::from_str::<Value>(line) {
            Ok(message) => message,
            Err(error) => {
                return Some(jsonrpc_error(
                    Value::Null,
                    FaultRecord::new(
                        FaultKind::InvalidInput,
                        FaultStage::Protocol,
                        "jsonrpc.parse",
                        format!("parse error: {error}"),
                    ),
                ));
            }
        };
        self.handle_message(message)
    }

    fn handle_message(&mut self, message: Value) -> Option<Value> {
        let Some(object) = message.as_object() else {
            return Some(jsonrpc_error(
                Value::Null,
                FaultRecord::new(
                    FaultKind::InvalidInput,
                    FaultStage::Protocol,
                    "jsonrpc.message",
                    "invalid request: expected JSON object",
                ),
            ));
        };

        let method = object.get("method").and_then(Value::as_str)?;
        let id = object.get("id").cloned();
        let params = object.get("params").cloned().unwrap_or_else(|| json!({}));
        let operation_key = operation_key(method, &params);
        let started_at = Instant::now();

        self.telemetry.record_request(&operation_key);
        let response = match self.dispatch(method, params, id.clone()) {
            Ok(Some(result)) => {
                self.telemetry
                    .record_success(&operation_key, started_at.elapsed().as_millis());
                id.map(|id| jsonrpc_result(id, result))
            }
            Ok(None) => {
                self.telemetry
                    .record_success(&operation_key, started_at.elapsed().as_millis());
                None
            }
            Err(fault) => {
                self.telemetry.record_error(
                    &operation_key,
                    fault.clone(),
                    started_at.elapsed().as_millis(),
                );
                Some(match id {
                    Some(id) => match method {
                        "tools/call" => jsonrpc_result(id, fault.into_tool_result()),
                        _ => jsonrpc_error(id, fault),
                    },
                    None => jsonrpc_error(Value::Null, fault),
                })
            }
        };

        if self.should_force_rollout(&operation_key) {
            self.force_rollout_consumed = true;
            self.telemetry.record_rollout();
            self.rollout_requested = true;
        }

        response
    }

    fn dispatch(
        &mut self,
        method: &str,
        params: Value,
        request_id: Option<Value>,
    ) -> Result<Option<Value>, FaultRecord> {
        match method {
            "initialize" => {
                self.session.initialize_params = Some(params.clone());
                self.session.initialized = false;
                Ok(Some(json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {
                        "tools": { "listChanged": false },
                        "resources": { "listChanged": false, "subscribe": false }
                    },
                    "serverInfo": {
                        "name": SERVER_NAME,
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "instructions": "The DAG is canonical truth. Frontier state is derived. Bind the session with project.bind before project-local DAG operations when the MCP is running unbound."
                })))
            }
            "notifications/initialized" => {
                if self.session.initialize_params.is_none() {
                    return Err(FaultRecord::new(
                        FaultKind::NotInitialized,
                        FaultStage::Host,
                        "notifications/initialized",
                        "received initialized notification before initialize",
                    ));
                }
                self.session.initialized = true;
                Ok(None)
            }
            "notifications/cancelled" => Ok(None),
            "ping" => Ok(Some(json!({}))),
            other => {
                self.require_initialized(other)?;
                match other {
                    "tools/list" => Ok(Some(json!({ "tools": tool_definitions() }))),
                    "resources/list" => Ok(Some(json!({ "resources": list_resources() }))),
                    "tools/call" => Ok(Some(self.dispatch_tool_call(params, request_id)?)),
                    "resources/read" => Ok(Some(self.dispatch_resource_read(params)?)),
                    _ => Err(FaultRecord::new(
                        FaultKind::InvalidInput,
                        FaultStage::Protocol,
                        other,
                        format!("method `{other}` is not implemented"),
                    )),
                }
            }
        }
    }

    fn dispatch_tool_call(
        &mut self,
        params: Value,
        _request_id: Option<Value>,
    ) -> Result<Value, FaultRecord> {
        let envelope = deserialize::<ToolCallEnvelope>(params, "tools/call")?;
        let spec = tool_spec(&envelope.name).ok_or_else(|| {
            FaultRecord::new(
                FaultKind::InvalidInput,
                FaultStage::Host,
                format!("tools/call:{}", envelope.name),
                format!("unknown tool `{}`", envelope.name),
            )
        })?;
        match spec.dispatch {
            DispatchTarget::Host => self.handle_host_tool(&envelope.name, envelope.arguments),
            DispatchTarget::Worker => self.dispatch_worker_tool(spec, envelope.arguments),
        }
    }

    fn dispatch_resource_read(&mut self, params: Value) -> Result<Value, FaultRecord> {
        let args = deserialize::<ReadResourceArgs>(params, "resources/read")?;
        let spec = resource_spec(&args.uri).ok_or_else(|| {
            FaultRecord::new(
                FaultKind::InvalidInput,
                FaultStage::Host,
                format!("resources/read:{}", args.uri),
                format!("unknown resource `{}`", args.uri),
            )
        })?;
        match spec.dispatch {
            DispatchTarget::Host => Ok(Self::handle_host_resource(spec.uri)),
            DispatchTarget::Worker => self.dispatch_worker_operation(
                format!("resources/read:{}", args.uri),
                spec.replay,
                WorkerOperation::ReadResource { uri: args.uri },
            ),
        }
    }

    fn dispatch_worker_tool(
        &mut self,
        spec: crate::mcp::catalog::ToolSpec,
        arguments: Value,
    ) -> Result<Value, FaultRecord> {
        let operation = format!("tools/call:{}", spec.name);
        self.dispatch_worker_operation(
            operation.clone(),
            spec.replay,
            WorkerOperation::CallTool {
                name: spec.name.to_owned(),
                arguments,
            },
        )
    }

    fn dispatch_worker_operation(
        &mut self,
        operation: String,
        replay: ReplayContract,
        worker_operation: WorkerOperation,
    ) -> Result<Value, FaultRecord> {
        let binding = self.require_bound_project(&operation)?;
        self.worker.rebind(binding.project_root.clone());

        if self.should_crash_worker_once(&operation) {
            self.worker.arm_crash_once();
        }

        let request_id = self.allocate_request_id();
        match self.worker.execute(request_id, worker_operation.clone()) {
            Ok(result) => Ok(result),
            Err(fault) => {
                if replay == ReplayContract::SafeReplay && fault.retryable {
                    self.telemetry.record_retry(&operation);
                    self.telemetry.record_worker_restart();
                    self.worker
                        .restart()
                        .map_err(|restart_fault| restart_fault.mark_retried())?;
                    match self.worker.execute(request_id, worker_operation) {
                        Ok(result) => Ok(result),
                        Err(retry_fault) => Err(retry_fault.mark_retried()),
                    }
                } else {
                    Err(fault)
                }
            }
        }
    }

    fn handle_host_tool(&mut self, name: &str, arguments: Value) -> Result<Value, FaultRecord> {
        match name {
            "project.bind" => {
                let args = deserialize::<ProjectBindArgs>(arguments, "tools/call:project.bind")?;
                let resolved = resolve_project_binding(PathBuf::from(args.path))
                    .map_err(host_store_fault("tools/call:project.bind"))?;
                self.worker.rebind(resolved.binding.project_root.clone());
                self.binding = Some(resolved.binding);
                tool_success(&resolved.status)
            }
            "skill.list" => tool_success(&json!({
                "skills": crate::bundled_skill::bundled_skill_summaries(),
            })),
            "skill.show" => {
                let args = deserialize::<SkillShowArgs>(arguments, "tools/call:skill.show")?;
                let skill = args.name.as_deref().map_or_else(
                    || Ok(crate::bundled_skill::default_bundled_skill()),
                    |name| {
                        crate::bundled_skill::bundled_skill(name).ok_or_else(|| {
                            FaultRecord::new(
                                FaultKind::InvalidInput,
                                FaultStage::Host,
                                "tools/call:skill.show",
                                format!("unknown bundled skill `{name}`"),
                            )
                        })
                    },
                )?;
                tool_success(&json!({
                    "name": skill.name,
                    "description": skill.description,
                    "resource_uri": skill.resource_uri,
                    "body": skill.body,
                }))
            }
            "system.health" => tool_success(&HealthSnapshot {
                initialization: InitializationHealth {
                    ready: self.session.initialized,
                    seed_captured: self.session.initialize_params.is_some(),
                },
                binding: binding_health(self.binding.as_ref()),
                worker: WorkerHealth {
                    worker_generation: self.worker.generation(),
                    alive: self.worker.is_alive(),
                },
                binary: BinaryHealth {
                    current_executable: self.binary.path.display().to_string(),
                    launch_path_stable: self.binary.launch_path_stable,
                    rollout_pending: self.binary.rollout_pending().unwrap_or(false),
                },
                last_fault: self.telemetry.last_fault.clone(),
            }),
            "system.telemetry" => tool_success(&self.telemetry),
            other => Err(FaultRecord::new(
                FaultKind::InvalidInput,
                FaultStage::Host,
                format!("tools/call:{other}"),
                format!("unknown host tool `{other}`"),
            )),
        }
    }

    fn handle_host_resource(uri: &str) -> Value {
        match uri {
            "fidget-spinner://skill/fidget-spinner" => {
                skill_resource(uri, crate::bundled_skill::default_bundled_skill().body)
            }
            "fidget-spinner://skill/frontier-loop" => skill_resource(
                uri,
                crate::bundled_skill::frontier_loop_bundled_skill().body,
            ),
            _ => unreachable!("host resources are catalog-gated"),
        }
    }

    fn require_initialized(&self, operation: &str) -> Result<(), FaultRecord> {
        if self.session.initialized {
            return Ok(());
        }
        Err(FaultRecord::new(
            FaultKind::NotInitialized,
            FaultStage::Host,
            operation,
            "client must call initialize and notifications/initialized before normal operations",
        ))
    }

    fn require_bound_project(&self, operation: &str) -> Result<&ProjectBinding, FaultRecord> {
        self.binding.as_ref().ok_or_else(|| {
            FaultRecord::new(
                FaultKind::Unavailable,
                FaultStage::Host,
                operation,
                "project is not bound; call project.bind with the target project root or a nested path inside it",
            )
        })
    }

    fn allocate_request_id(&mut self) -> HostRequestId {
        let id = HostRequestId(self.next_request_id);
        self.next_request_id += 1;
        id
    }

    fn maybe_roll_forward(&mut self) -> Result<(), fidget_spinner_store_sqlite::StoreError> {
        let binary_pending = self.binary.rollout_pending()?;
        if !self.rollout_requested && !binary_pending {
            return Ok(());
        }
        if binary_pending && !self.rollout_requested {
            self.telemetry.record_rollout();
        }
        self.roll_forward()
    }

    fn roll_forward(&mut self) -> Result<(), fidget_spinner_store_sqlite::StoreError> {
        let state = HostStateSeed {
            session: self.session.clone(),
            telemetry: self.telemetry.clone(),
            next_request_id: self.next_request_id,
            binding: self.binding.clone().map(ProjectBindingSeed::from),
            worker_generation: self.worker.generation(),
            force_rollout_consumed: self.force_rollout_consumed,
            crash_once_consumed: self.crash_once_consumed,
        };
        let serialized = serde_json::to_string(&state)?;
        let mut command = Command::new(&self.binary.path);
        let _ = command.arg("mcp").arg("serve");
        if let Some(project) = self.config.initial_project.as_ref() {
            let _ = command.arg("--project").arg(project);
        }
        let _ = command.env(HOST_STATE_ENV, serialized);
        #[cfg(unix)]
        {
            let error = command.exec();
            Err(fidget_spinner_store_sqlite::StoreError::Io(error))
        }
        #[cfg(not(unix))]
        {
            return Err(fidget_spinner_store_sqlite::StoreError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "host rollout requires unix exec support",
            )));
        }
    }

    fn should_force_rollout(&self, operation: &str) -> bool {
        self.force_rollout_key
            .as_deref()
            .is_some_and(|key| key == operation)
            && !self.force_rollout_consumed
    }

    fn should_crash_worker_once(&mut self, operation: &str) -> bool {
        let should_crash = self
            .crash_once_key
            .as_deref()
            .is_some_and(|key| key == operation)
            && !self.crash_once_consumed;
        if should_crash {
            self.crash_once_consumed = true;
        }
        should_crash
    }
}

#[derive(Debug, Serialize)]
struct ProjectBindStatus {
    requested_path: String,
    project_root: String,
    state_root: String,
    display_name: fidget_spinner_core::NonEmptyText,
    schema: fidget_spinner_core::PayloadSchemaRef,
    git_repo_detected: bool,
}

struct ResolvedProjectBinding {
    binding: ProjectBinding,
    status: ProjectBindStatus,
}

fn resolve_project_binding(
    requested_path: PathBuf,
) -> Result<ResolvedProjectBinding, fidget_spinner_store_sqlite::StoreError> {
    let store = crate::open_store(&requested_path)?;
    Ok(ResolvedProjectBinding {
        binding: ProjectBinding {
            requested_path: requested_path.clone(),
            project_root: PathBuf::from(store.project_root().as_str()),
        },
        status: ProjectBindStatus {
            requested_path: requested_path.display().to_string(),
            project_root: store.project_root().to_string(),
            state_root: store.state_root().to_string(),
            display_name: store.config().display_name.clone(),
            schema: store.schema().schema_ref(),
            git_repo_detected: crate::run_git(
                store.project_root(),
                &["rev-parse", "--show-toplevel"],
            )?
            .is_some(),
        },
    })
}

fn binding_health(binding: Option<&ProjectBinding>) -> BindingHealth {
    match binding {
        Some(binding) => BindingHealth {
            bound: true,
            requested_path: Some(binding.requested_path.display().to_string()),
            project_root: Some(binding.project_root.display().to_string()),
            state_root: Some(
                binding
                    .project_root
                    .join(fidget_spinner_store_sqlite::STORE_DIR_NAME)
                    .display()
                    .to_string(),
            ),
        },
        None => BindingHealth {
            bound: false,
            requested_path: None,
            project_root: None,
            state_root: None,
        },
    }
}

fn skill_resource(uri: &str, body: &str) -> Value {
    json!({
        "contents": [{
            "uri": uri,
            "mimeType": "text/markdown",
            "text": body,
        }]
    })
}

impl From<ProjectBindingSeed> for ProjectBinding {
    fn from(value: ProjectBindingSeed) -> Self {
        Self {
            requested_path: value.requested_path,
            project_root: value.project_root,
        }
    }
}

impl From<ProjectBinding> for ProjectBindingSeed {
    fn from(value: ProjectBinding) -> Self {
        Self {
            requested_path: value.requested_path,
            project_root: value.project_root,
        }
    }
}

fn restore_host_state() -> Option<HostStateSeed> {
    let raw = std::env::var(HOST_STATE_ENV).ok()?;
    serde_json::from_str::<HostStateSeed>(&raw).ok()
}

fn deserialize<T: for<'de> serde::Deserialize<'de>>(
    value: Value,
    operation: &str,
) -> Result<T, FaultRecord> {
    serde_json::from_value(value).map_err(|error| {
        FaultRecord::new(
            FaultKind::InvalidInput,
            FaultStage::Protocol,
            operation,
            format!("invalid params: {error}"),
        )
    })
}

fn operation_key(method: &str, params: &Value) -> String {
    match method {
        "tools/call" => params.get("name").and_then(Value::as_str).map_or_else(
            || "tools/call".to_owned(),
            |name| format!("tools/call:{name}"),
        ),
        "resources/read" => params.get("uri").and_then(Value::as_str).map_or_else(
            || "resources/read".to_owned(),
            |uri| format!("resources/read:{uri}"),
        ),
        other => other.to_owned(),
    }
}

fn tool_success(value: &impl Serialize) -> Result<Value, FaultRecord> {
    Ok(json!({
        "content": [{
            "type": "text",
            "text": crate::to_pretty_json(value).map_err(|error| {
                FaultRecord::new(FaultKind::Internal, FaultStage::Host, "tool_success", error.to_string())
            })?,
        }],
        "structuredContent": serde_json::to_value(value).map_err(|error| {
            FaultRecord::new(FaultKind::Internal, FaultStage::Host, "tool_success", error.to_string())
        })?,
        "isError": false,
    }))
}

fn host_store_fault(
    operation: &'static str,
) -> impl FnOnce(fidget_spinner_store_sqlite::StoreError) -> FaultRecord {
    move |error| {
        FaultRecord::new(
            FaultKind::InvalidInput,
            FaultStage::Host,
            operation,
            error.to_string(),
        )
    }
}

fn jsonrpc_result(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn jsonrpc_error(id: Value, fault: FaultRecord) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": fault.into_jsonrpc_error(),
    })
}

fn write_message(
    stdout: &mut impl Write,
    message: &Value,
) -> Result<(), fidget_spinner_store_sqlite::StoreError> {
    serde_json::to_writer(&mut *stdout, message)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct ToolCallEnvelope {
    name: String,
    #[serde(default = "empty_json_object")]
    arguments: Value,
}

fn empty_json_object() -> Value {
    json!({})
}

#[derive(Debug, serde::Deserialize)]
struct ReadResourceArgs {
    uri: String,
}

#[derive(Debug, serde::Deserialize)]
struct ProjectBindArgs {
    path: String,
}

#[derive(Debug, serde::Deserialize)]
struct SkillShowArgs {
    name: Option<String>,
}
