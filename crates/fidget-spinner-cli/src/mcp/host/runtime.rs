use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use libmcp::{
    FrameLimit, FramedMessage, HandoffOutcome, HostRejection, HostSessionKernel, ReleaseRuntime,
    ReplayContract, SnapshotLimits, load_snapshot_file_from_env, write_frame_blocking,
    write_snapshot_file,
};
#[cfg(not(unix))]
use libmcp::{FrameReadOutcome, read_frame_blocking};
#[cfg(unix)]
use libmcp::{TimedFrameReadOutcome, TimedFrameReader};
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::{
    config::HostConfig,
    process::{ProjectBinding, WorkerSupervisor},
};
use crate::mcp::catalog::{
    DispatchTarget, list_resources, resource_spec, tool_definitions, tool_spec,
};
use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};
use crate::mcp::output::{
    ToolOutput, fallback_detailed_tool_output, split_presentation, tool_success,
};
use crate::mcp::protocol::{
    CRASH_ONCE_ENV, FORCE_ROLLOUT_ENV, HOST_STATE_ENV, HostRequestId, HostStateSeed,
    PROTOCOL_VERSION, ProjectBindingSeed, SERVER_NAME, WorkerOperation, WorkerSpawnConfig,
};
use crate::mcp::telemetry::{
    BinaryHealth, BindingHealth, HealthSnapshot, InitializationHealth, ServerTelemetry,
    WorkerHealth,
};

pub(crate) fn run_host(
    initial_project: Option<PathBuf>,
) -> Result<(), fidget_spinner_store_sqlite::StoreError> {
    #[cfg(unix)]
    {
        run_polling_host(initial_project)
    }
    #[cfg(not(unix))]
    {
        run_blocking_host(initial_project)
    }
}

#[cfg(unix)]
fn run_polling_host(
    initial_project: Option<PathBuf>,
) -> Result<(), fidget_spinner_store_sqlite::StoreError> {
    let stdin = io::stdin();
    let mut stdin = TimedFrameReader::new(stdin.lock(), FrameLimit::DEFAULT);
    let mut stdout = io::stdout().lock();
    let mut host = HostRuntime::new(HostConfig::new(initial_project)?)?;
    host.release
        .admit_successor()
        .map_err(fidget_spinner_store_sqlite::StoreError::Io)?;

    loop {
        match stdin.read_frame(HOST_CONTROL_POLL_INTERVAL)? {
            TimedFrameReadOutcome::Frame(payload) => {
                if let Some(response) = host.handle_payload(payload) {
                    write_message(&mut stdout, &response)?;
                }
            }
            TimedFrameReadOutcome::EndOfStream => return Ok(()),
            TimedFrameReadOutcome::TimedOut => {}
        }
        if !stdin.has_buffered_input() && host.maybe_roll_forward() {
            return Ok(());
        }
    }
}

#[cfg(not(unix))]
fn run_blocking_host(
    initial_project: Option<PathBuf>,
) -> Result<(), fidget_spinner_store_sqlite::StoreError> {
    let mut stdin = io::BufReader::new(io::stdin().lock());
    let mut stdout = io::stdout().lock();
    let mut host = HostRuntime::new(HostConfig::new(initial_project)?)?;
    host.release
        .admit_successor()
        .map_err(fidget_spinner_store_sqlite::StoreError::Io)?;

    while let FrameReadOutcome::Frame(payload) =
        read_frame_blocking(&mut stdin, FrameLimit::DEFAULT)?
    {
        if let Some(response) = host.handle_payload(payload) {
            write_message(&mut stdout, &response)?;
        }
        if host.maybe_roll_forward() {
            return Ok(());
        }
    }

    Ok(())
}

const HOST_PENDING_CAPACITY: usize = 128;
const HOST_SNAPSHOT_MAX_BYTES: usize = 16 * 1024 * 1024;
const HOST_REPLAY_ATTEMPTS: u8 = 1;
#[cfg(unix)]
const HOST_CONTROL_POLL_INTERVAL: Duration = Duration::from_millis(250);
const HOST_ROLLOUT_RETRY_DELAY: Duration = Duration::from_secs(5);
const HOST_HANDOFF_TIMEOUT: Duration = Duration::from_secs(15);

struct HostRuntime {
    config: HostConfig,
    binding: Option<ProjectBinding>,
    session_kernel: HostSessionKernel,
    telemetry: ServerTelemetry,
    next_request_id: u64,
    worker: WorkerSupervisor,
    release: ReleaseRuntime,
    force_rollout_key: Option<String>,
    force_rollout_consumed: bool,
    rollout_requested: bool,
    crash_once_key: Option<String>,
    crash_once_consumed: bool,
    rollout_retry_not_before: Option<Instant>,
}

impl HostRuntime {
    fn new(config: HostConfig) -> Result<Self, fidget_spinner_store_sqlite::StoreError> {
        let restored = restore_host_state()?;
        let limits = snapshot_limits()?;
        let session_kernel = restored
            .as_ref()
            .map(|seed| seed.session_kernel.clone().restore(limits))
            .transpose()
            .map_err(snapshot_store_error)?
            .unwrap_or_else(HostSessionKernel::cold);
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
        let release = ReleaseRuntime::discover(SERVER_NAME)
            .map_err(fidget_spinner_store_sqlite::StoreError::Io)?;

        Ok(Self {
            config: config.clone(),
            binding,
            session_kernel,
            telemetry,
            next_request_id,
            worker,
            release,
            force_rollout_key: std::env::var(FORCE_ROLLOUT_ENV).ok(),
            force_rollout_consumed,
            rollout_requested: false,
            crash_once_key: std::env::var(CRASH_ONCE_ENV).ok(),
            crash_once_consumed,
            rollout_retry_not_before: None,
        })
    }

    fn handle_payload(&mut self, payload: Vec<u8>) -> Option<Value> {
        let frame = match FramedMessage::parse(payload) {
            Ok(frame) => frame,
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
        self.handle_frame(frame)
    }

    fn handle_frame(&mut self, frame: FramedMessage) -> Option<Value> {
        let Some(object) = frame.value().as_object() else {
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
        let journaled = id.is_some();
        let params = object.get("params").cloned().unwrap_or_else(|| json!({}));
        let operation_key = operation_key(method, &params);
        let started_at = Instant::now();

        if let Err(rejection) = self.session_kernel.observe_client_frame(&frame) {
            return id.map(|id| jsonrpc_error(id, host_rejection_fault(method, rejection)));
        }
        if journaled
            && let Err(rejection) = self.session_kernel.begin_request_dispatch(
                &frame,
                request_replay_contract(method, &params),
                HOST_PENDING_CAPACITY,
            )
        {
            return id.map(|id| jsonrpc_error(id, host_rejection_fault(method, rejection)));
        }

        self.telemetry.record_request(&operation_key);
        let response = match self.dispatch(method, params) {
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

        if journaled
            && let Some(response) = response.as_ref()
            && let Err(error) = self.complete_public_response(response)
        {
            eprintln!("MCP host-session completion failure: {error}");
        }

        response
    }

    fn dispatch(&mut self, method: &str, params: Value) -> Result<Option<Value>, FaultRecord> {
        match method {
            "initialize" => Ok(Some(json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {
                    "tools": { "listChanged": false },
                    "resources": { "listChanged": false, "subscribe": false }
                },
                "serverInfo": {
                    "name": SERVER_NAME,
                    "version": env!("CARGO_PKG_VERSION")
                },
                "instructions": "Bind the session with project.bind before project-local work when the MCP is unbound. Use frontier.open as the only overview surface, then walk hypotheses and experiments deliberately by selector. Hypotheses are cheap idea-capture nodes: record them eagerly when a plausible KPI-moving branch appears, always setting expected_yield and confidence as crude low/medium/high vibe checks. Use hypothesis.update to reprioritize fields or tidy wording/tags/parents, and hypothesis.attention.set to shelve stale idle ideas or restore them to the worklist. Closing the last open experiment for a hypothesis requires keep_hypothesis_on_worklist=true or false."
            }))),
            "notifications/initialized" => {
                if !self.seed_captured() {
                    return Err(FaultRecord::new(
                        FaultKind::NotInitialized,
                        FaultStage::Host,
                        "notifications/initialized",
                        "received initialized notification before initialize",
                    ));
                }
                Ok(None)
            }
            "notifications/cancelled" => Ok(None),
            "ping" => Ok(Some(json!({}))),
            other => {
                self.require_initialized(other)?;
                match other {
                    "tools/list" => Ok(Some(json!({ "tools": tool_definitions() }))),
                    "resources/list" => Ok(Some(json!({ "resources": list_resources() }))),
                    "tools/call" => Ok(Some(self.dispatch_tool_call(params)?)),
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

    fn dispatch_tool_call(&mut self, params: Value) -> Result<Value, FaultRecord> {
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
        self.refresh_worker_for_binary_rollout(&operation)?;

        if self.should_crash_worker_once(&operation) {
            self.worker.arm_crash_once();
        }

        let request_id = self.allocate_request_id();
        match self.worker.execute(request_id, worker_operation.clone()) {
            Ok(result) => Ok(result),
            Err(fault) => {
                if fault.is_store_format_mismatch() {
                    return self.retry_after_store_format_rollout(
                        &operation,
                        request_id,
                        worker_operation,
                        fault,
                    );
                }
                if replay == ReplayContract::Convergent && fault.retryable {
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

    fn refresh_worker_for_binary_rollout(&mut self, operation: &str) -> Result<(), FaultRecord> {
        let observation = self.release.observe().map_err(|error| {
            FaultRecord::new(
                FaultKind::Internal,
                FaultStage::Rollout,
                operation,
                format!("failed to inspect MCP binary rollout state: {error}"),
            )
        })?;
        if self.release.is_managed() || !observation.rollout_ready() {
            return Ok(());
        }
        self.telemetry.record_worker_restart();
        self.worker.restart()?;
        Ok(())
    }

    fn retry_after_store_format_rollout(
        &mut self,
        operation: &str,
        request_id: HostRequestId,
        worker_operation: WorkerOperation,
        first_fault: FaultRecord,
    ) -> Result<Value, FaultRecord> {
        self.telemetry.record_retry(operation);
        self.telemetry.record_worker_restart();
        self.worker
            .restart()
            .map_err(|restart_fault| restart_fault.mark_retried())?;
        match self.worker.execute(request_id, worker_operation) {
            Ok(result) => Ok(result),
            Err(retry_fault) if retry_fault.is_store_format_mismatch() => {
                Err(first_fault.mark_retried())
            }
            Err(retry_fault) => Err(retry_fault.mark_retried()),
        }
    }

    fn handle_host_tool(&mut self, name: &str, arguments: Value) -> Result<Value, FaultRecord> {
        let operation = format!("tools/call:{name}");
        let (presentation, arguments) =
            split_presentation(arguments, &operation, FaultStage::Host)?;
        match name {
            "project.bind" => {
                let args = deserialize::<ProjectBindArgs>(arguments, "tools/call:project.bind")?;
                let resolved = resolve_project_binding(PathBuf::from(args.path))
                    .map_err(host_store_fault("tools/call:project.bind"))?;
                self.worker
                    .refresh_binding(resolved.binding.project_root.clone());
                self.binding = Some(resolved.binding);
                tool_success(
                    project_bind_output(&resolved.status)?,
                    presentation,
                    FaultStage::Host,
                    "tools/call:project.bind",
                )
            }
            "skill.list" => tool_success(
                skill_list_output()?,
                presentation,
                FaultStage::Host,
                "tools/call:skill.list",
            ),
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
                tool_success(
                    skill_show_output(skill)?,
                    presentation,
                    FaultStage::Host,
                    "tools/call:skill.show",
                )
            }
            "system.health" => {
                let health = HealthSnapshot {
                    initialization: InitializationHealth {
                        ready: self.session_initialized(),
                        seed_captured: self.seed_captured(),
                    },
                    binding: binding_health(self.binding.as_ref()),
                    worker: WorkerHealth {
                        worker_generation: self.worker.generation(),
                        alive: self.worker.is_alive(),
                    },
                    binary: BinaryHealth {
                        current_executable: self.config.executable.display().to_string(),
                        launch_path_stable: self.release.launch_path_stable(),
                        rollout_pending: self.rollout_requested
                            || self
                                .release
                                .observe()
                                .map_or(true, libmcp::ReleaseObservation::rollout_pending),
                    },
                    last_fault: self.telemetry.last_fault.clone(),
                };
                tool_success(
                    system_health_output(&health)?,
                    presentation,
                    FaultStage::Host,
                    "tools/call:system.health",
                )
            }
            "system.telemetry" => tool_success(
                system_telemetry_output(&self.telemetry)?,
                presentation,
                FaultStage::Host,
                "tools/call:system.telemetry",
            ),
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
        if self.session_initialized() {
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

    fn session_initialized(&self) -> bool {
        self.session_kernel
            .initialization_seed()
            .is_some_and(|seed| seed.initialized_notification().is_some())
    }

    fn seed_captured(&self) -> bool {
        self.session_kernel.initialization_seed().is_some()
    }

    fn complete_public_response(&mut self, response: &Value) -> Result<(), String> {
        let payload = serde_json::to_vec(response)
            .map_err(|error| format!("failed to encode terminal response: {error}"))?;
        let response = FramedMessage::parse(payload)
            .map_err(|error| format!("failed to validate terminal response: {error}"))?;
        let _completed = self
            .session_kernel
            .complete_response(&response)
            .map_err(|error| format!("kernel rejected terminal response: {error:?}"))?;
        Ok(())
    }

    fn allocate_request_id(&mut self) -> HostRequestId {
        let id = HostRequestId(self.next_request_id);
        self.next_request_id += 1;
        id
    }

    fn maybe_roll_forward(&mut self) -> bool {
        let now = Instant::now();
        if self
            .rollout_retry_not_before
            .is_some_and(|deadline| now < deadline)
        {
            return false;
        }
        self.rollout_retry_not_before = None;
        let observation = match self.release.observe() {
            Ok(observation) => observation,
            Err(error) => {
                self.rollout_retry_not_before =
                    Instant::now().checked_add(HOST_ROLLOUT_RETRY_DELAY);
                eprintln!("MCP successor observation failure: {error}");
                return false;
            }
        };
        if self.rollout_requested
            && !observation.rollout_ready()
            && let Err(error) = self.release.arm_current_relaunch()
        {
            self.defer_rollout(error);
            return false;
        }
        if !self.rollout_requested && !observation.rollout_ready() {
            return false;
        }
        if observation.rollout_ready() && !self.rollout_requested {
            self.telemetry.record_rollout();
        }
        match self.roll_forward() {
            Ok(HandoffOutcome::Relinquish) => true,
            Ok(HandoffOutcome::Retained) => false,
            Err(error) => {
                self.defer_rollout(error);
                false
            }
        }
    }

    fn roll_forward(&self) -> Result<HandoffOutcome, fidget_spinner_store_sqlite::StoreError> {
        let state = HostStateSeed {
            session_kernel: self.session_kernel.snapshot(),
            telemetry: self.telemetry.clone(),
            next_request_id: self.next_request_id,
            binding: self.binding.clone().map(ProjectBindingSeed::from),
            worker_generation: self.worker.generation(),
            force_rollout_consumed: self.force_rollout_consumed,
            crash_once_consumed: self.crash_once_consumed,
        };
        let state_capsule = write_snapshot_file("fidget-spinner-mcp-host-reexec", &state)
            .map_err(fidget_spinner_store_sqlite::StoreError::Io)?;
        self.release
            .handoff(HOST_STATE_ENV, state_capsule.path(), HOST_HANDOFF_TIMEOUT)
            .map_err(fidget_spinner_store_sqlite::StoreError::Io)
    }

    fn defer_rollout(&mut self, error: impl std::fmt::Display) {
        self.rollout_retry_not_before = Instant::now().checked_add(HOST_ROLLOUT_RETRY_DELAY);
        eprintln!("fidget-spinner MCP rollout retained incumbent: {error}");
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
    frontier_count: u64,
    hypothesis_count: u64,
    experiment_count: u64,
    open_experiment_count: u64,
}

struct ResolvedProjectBinding {
    binding: ProjectBinding,
    status: ProjectBindStatus,
}

fn resolve_project_binding(
    requested_path: PathBuf,
) -> Result<ResolvedProjectBinding, fidget_spinner_store_sqlite::StoreError> {
    let store = crate::open_or_init_store_for_binding(&requested_path)?;
    let project_status = store.status_from_mcp()?;
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
            frontier_count: project_status.frontier_count,
            hypothesis_count: project_status.hypothesis_count,
            experiment_count: project_status.experiment_count,
            open_experiment_count: project_status.open_experiment_count,
        },
    })
}

fn binding_health(binding: Option<&ProjectBinding>) -> BindingHealth {
    match binding {
        Some(binding) => BindingHealth {
            bound: true,
            requested_path: Some(binding.requested_path.display().to_string()),
            project_root: Some(binding.project_root.display().to_string()),
            state_root: fidget_spinner_store_sqlite::state_root_for_project_root(
                &crate::utf8_path(binding.project_root.clone()),
            )
            .ok()
            .map(|state_root| state_root.to_string()),
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

fn restore_host_state() -> Result<Option<HostStateSeed>, fidget_spinner_store_sqlite::StoreError> {
    load_snapshot_file_from_env(HOST_STATE_ENV, HOST_SNAPSHOT_MAX_BYTES)
        .map_err(fidget_spinner_store_sqlite::StoreError::Io)
}

fn snapshot_limits() -> Result<SnapshotLimits, fidget_spinner_store_sqlite::StoreError> {
    SnapshotLimits::try_new(
        HOST_PENDING_CAPACITY,
        HOST_PENDING_CAPACITY,
        FrameLimit::DEFAULT.get(),
        HOST_REPLAY_ATTEMPTS,
    )
    .map_err(|error| {
        fidget_spinner_store_sqlite::StoreError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            error.to_string(),
        ))
    })
}

fn snapshot_store_error(error: libmcp::SnapshotError) -> fidget_spinner_store_sqlite::StoreError {
    fidget_spinner_store_sqlite::StoreError::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        error.to_string(),
    ))
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

fn request_replay_contract(method: &str, params: &Value) -> ReplayContract {
    match method {
        "tools/call" => params
            .get("name")
            .and_then(Value::as_str)
            .and_then(tool_spec)
            .map_or(ReplayContract::NeverReplay, |spec| spec.replay),
        "resources/read" => params
            .get("uri")
            .and_then(Value::as_str)
            .and_then(resource_spec)
            .map_or(ReplayContract::NeverReplay, |spec| spec.replay),
        "initialize" | "ping" | "tools/list" | "resources/list" => ReplayContract::Convergent,
        _ => ReplayContract::NeverReplay,
    }
}

fn host_rejection_fault(operation: &str, rejection: HostRejection) -> FaultRecord {
    let kind = match rejection {
        HostRejection::QueueOverflow
        | HostRejection::ReplayBudgetExhausted
        | HostRejection::PendingCapacityExhausted => FaultKind::Unavailable,
        HostRejection::DuplicateRequestId
        | HostRejection::InvalidRequestFrame
        | HostRejection::RequestNotPending => FaultKind::InvalidInput,
        HostRejection::AmbiguousOutcome | HostRejection::InvalidExecutionState => {
            FaultKind::Internal
        }
    };
    FaultRecord::new(
        kind,
        FaultStage::Host,
        operation,
        format!("host session rejected request: {rejection:?}"),
    )
}

fn project_bind_output(status: &ProjectBindStatus) -> Result<ToolOutput, FaultRecord> {
    let mut concise = Map::new();
    let _ = concise.insert("project_root".to_owned(), json!(status.project_root));
    let _ = concise.insert("state_root".to_owned(), json!(status.state_root));
    let _ = concise.insert("display_name".to_owned(), json!(status.display_name));
    let _ = concise.insert("frontier_count".to_owned(), json!(status.frontier_count));
    let _ = concise.insert(
        "hypothesis_count".to_owned(),
        json!(status.hypothesis_count),
    );
    let _ = concise.insert(
        "experiment_count".to_owned(),
        json!(status.experiment_count),
    );
    let _ = concise.insert(
        "open_experiment_count".to_owned(),
        json!(status.open_experiment_count),
    );
    if status.requested_path != status.project_root {
        let _ = concise.insert("requested_path".to_owned(), json!(status.requested_path));
    }
    fallback_detailed_tool_output(
        &Value::Object(concise),
        status,
        [
            format!("bound project {}", status.display_name),
            format!("root: {}", status.project_root),
            format!("state: {}", status.state_root),
            format!("frontiers: {}", status.frontier_count),
            format!("hypotheses: {}", status.hypothesis_count),
            format!(
                "experiments: {} total, {} open",
                status.experiment_count, status.open_experiment_count
            ),
        ]
        .join("\n"),
        None,
        libmcp::SurfaceKind::Mutation,
        FaultStage::Host,
        "tools/call:project.bind",
    )
}

fn skill_list_output() -> Result<ToolOutput, FaultRecord> {
    let skills = crate::bundled_skill::bundled_skill_summaries();
    let concise = json!({
        "skills": skills.iter().map(|skill| {
            json!({
                "name": skill.name,
                "description": skill.description,
            })
        }).collect::<Vec<_>>(),
    });
    let mut lines = vec![format!("{} bundled skill(s)", skills.len())];
    lines.extend(
        skills
            .iter()
            .map(|skill| format!("{}: {}", skill.name, skill.description)),
    );
    fallback_detailed_tool_output(
        &concise,
        &json!({ "skills": skills }),
        lines.join("\n"),
        None,
        libmcp::SurfaceKind::List,
        FaultStage::Host,
        "tools/call:skill.list",
    )
}

fn skill_show_output(skill: crate::bundled_skill::BundledSkill) -> Result<ToolOutput, FaultRecord> {
    fallback_detailed_tool_output(
        &json!({
            "name": skill.name,
            "resource_uri": skill.resource_uri,
            "body": skill.body,
        }),
        &json!({
            "name": skill.name,
            "description": skill.description,
            "resource_uri": skill.resource_uri,
            "body": skill.body,
        }),
        skill.body,
        None,
        libmcp::SurfaceKind::Read,
        FaultStage::Host,
        "tools/call:skill.show",
    )
}

fn system_health_output(health: &HealthSnapshot) -> Result<ToolOutput, FaultRecord> {
    let mut concise = Map::new();
    let _ = concise.insert(
        "ready".to_owned(),
        json!(health.initialization.ready && health.initialization.seed_captured),
    );
    let _ = concise.insert("bound".to_owned(), json!(health.binding.bound));
    if let Some(project_root) = health.binding.project_root.as_ref() {
        let _ = concise.insert("project_root".to_owned(), json!(project_root));
    }
    let _ = concise.insert(
        "worker_generation".to_owned(),
        json!(health.worker.worker_generation),
    );
    let _ = concise.insert("worker_alive".to_owned(), json!(health.worker.alive));
    let _ = concise.insert(
        "launch_path_stable".to_owned(),
        json!(health.binary.launch_path_stable),
    );
    let _ = concise.insert(
        "rollout_pending".to_owned(),
        json!(health.binary.rollout_pending),
    );
    let mut lines = vec![format!(
        "{} | {}",
        if health.initialization.ready && health.initialization.seed_captured {
            "ready"
        } else {
            "not-ready"
        },
        if health.binding.bound {
            "bound"
        } else {
            "unbound"
        }
    )];
    if let Some(project_root) = health.binding.project_root.as_ref() {
        lines.push(format!("project: {project_root}"));
    }
    lines.push(format!(
        "worker: gen {} {}",
        health.worker.worker_generation,
        if health.worker.alive { "alive" } else { "dead" }
    ));
    lines.push(format!(
        "binary: {}{}",
        if health.binary.launch_path_stable {
            "stable"
        } else {
            "unstable"
        },
        if health.binary.rollout_pending {
            " rollout-pending"
        } else {
            ""
        }
    ));
    fallback_detailed_tool_output(
        &Value::Object(concise),
        health,
        lines.join("\n"),
        None,
        libmcp::SurfaceKind::Ops,
        FaultStage::Host,
        "tools/call:system.health",
    )
}

fn system_telemetry_output(telemetry: &ServerTelemetry) -> Result<ToolOutput, FaultRecord> {
    let hot_operations = telemetry
        .operations
        .iter()
        .map(|(operation, stats)| {
            (
                operation.clone(),
                stats.requests,
                stats.errors,
                stats.retries,
                stats.last_latency_ms.unwrap_or(0),
            )
        })
        .collect::<Vec<_>>();
    let mut hot_operations = hot_operations;
    hot_operations.sort_by(|left, right| {
        right
            .1
            .cmp(&left.1)
            .then_with(|| right.2.cmp(&left.2))
            .then_with(|| right.3.cmp(&left.3))
            .then_with(|| left.0.cmp(&right.0))
    });
    let hot_operations = hot_operations
        .into_iter()
        .take(6)
        .map(|(operation, requests, errors, retries, last_latency_ms)| {
            json!({
                "operation": operation,
                "requests": requests,
                "errors": errors,
                "retries": retries,
                "last_latency_ms": last_latency_ms,
                "fault_codes": telemetry.operations[&operation].fault_codes,
            })
        })
        .collect::<Vec<_>>();

    let mut concise = Map::new();
    let _ = concise.insert(
        "window_started_at".to_owned(),
        json!(telemetry.window_started_at),
    );
    let _ = concise.insert("requests".to_owned(), json!(telemetry.requests));
    let _ = concise.insert("successes".to_owned(), json!(telemetry.successes));
    let _ = concise.insert("errors".to_owned(), json!(telemetry.errors));
    let _ = concise.insert("retries".to_owned(), json!(telemetry.retries));
    let _ = concise.insert(
        "worker_restarts".to_owned(),
        json!(telemetry.worker_restarts),
    );
    let _ = concise.insert("host_rollouts".to_owned(), json!(telemetry.host_rollouts));
    let _ = concise.insert("hot_operations".to_owned(), Value::Array(hot_operations));
    if let Some(fault) = telemetry.last_fault.as_ref() {
        let _ = concise.insert(
            "last_fault".to_owned(),
            json!({
                "kind": format!("{:?}", fault.kind).to_ascii_lowercase(),
                "code": fault.code,
                "operation": fault.operation,
                "message": fault.message,
            }),
        );
    }

    let mut lines = vec![format!(
        "requests={} success={} error={} retry={}",
        telemetry.requests, telemetry.successes, telemetry.errors, telemetry.retries
    )];
    lines.push(format!(
        "worker_restarts={} host_rollouts={}",
        telemetry.worker_restarts, telemetry.host_rollouts
    ));
    let mut ranked_operations = telemetry.operations.iter().collect::<Vec<_>>();
    ranked_operations.sort_by(|(left_name, left), (right_name, right)| {
        right
            .requests
            .cmp(&left.requests)
            .then_with(|| right.errors.cmp(&left.errors))
            .then_with(|| right.retries.cmp(&left.retries))
            .then_with(|| left_name.cmp(right_name))
    });
    if !ranked_operations.is_empty() {
        lines.push("hot operations:".to_owned());
        for (operation, stats) in ranked_operations.into_iter().take(6) {
            lines.push(format!(
                "{} req={} err={} retry={} last={}ms",
                operation,
                stats.requests,
                stats.errors,
                stats.retries,
                stats.last_latency_ms.unwrap_or(0),
            ));
        }
    }
    if let Some(fault) = telemetry.last_fault.as_ref() {
        lines.push(format!("last fault: {} {}", fault.operation, fault.message));
    }
    fallback_detailed_tool_output(
        &Value::Object(concise),
        telemetry,
        lines.join("\n"),
        None,
        libmcp::SurfaceKind::Ops,
        FaultStage::Host,
        "tools/call:system.telemetry",
    )
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
    let payload = serde_json::to_vec(message)?;
    write_frame_blocking(stdout, &payload, FrameLimit::DEFAULT)?;
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
