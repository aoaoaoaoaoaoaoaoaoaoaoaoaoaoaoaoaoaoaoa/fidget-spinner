use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};
use crate::mcp::protocol::{
    HostRequestId, WorkerOperation, WorkerOutcome, WorkerRequest, WorkerResponse, WorkerSpawnConfig,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct ProjectBinding {
    pub(super) requested_path: PathBuf,
    pub(super) project_root: PathBuf,
}

pub(super) struct WorkerSupervisor {
    config: WorkerSpawnConfig,
    generation: u64,
    crash_before_reply_once: bool,
    bound_project_root: Option<PathBuf>,
    child: Option<Child>,
    stdin: Option<BufWriter<ChildStdin>>,
    stdout: Option<BufReader<ChildStdout>>,
}

impl WorkerSupervisor {
    pub(super) fn new(config: WorkerSpawnConfig, generation: u64) -> Self {
        Self {
            config,
            generation,
            crash_before_reply_once: false,
            bound_project_root: None,
            child: None,
            stdin: None,
            stdout: None,
        }
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn rebind(&mut self, project_root: PathBuf) {
        if self
            .bound_project_root
            .as_ref()
            .is_some_and(|current| current == &project_root)
        {
            return;
        }
        self.kill_current_worker();
        self.bound_project_root = Some(project_root);
    }

    pub(super) fn refresh_binding(&mut self, project_root: PathBuf) {
        self.kill_current_worker();
        self.bound_project_root = Some(project_root);
    }

    pub(super) fn execute(
        &mut self,
        request_id: HostRequestId,
        operation: WorkerOperation,
    ) -> Result<Value, FaultRecord> {
        self.ensure_worker()?;
        let request = WorkerRequest::Execute {
            id: request_id,
            operation,
        };
        let stdin = self.stdin.as_mut().ok_or_else(|| {
            FaultRecord::new(
                FaultKind::Transient,
                FaultStage::Transport,
                "worker.stdin",
                "worker stdin is not available",
            )
            .retryable(Some(self.generation))
        })?;
        serde_json::to_writer(&mut *stdin, &request).map_err(|error| {
            FaultRecord::new(
                FaultKind::Transient,
                FaultStage::Transport,
                "worker.write",
                format!("failed to write worker request: {error}"),
            )
            .retryable(Some(self.generation))
        })?;
        stdin.write_all(b"\n").map_err(|error| {
            FaultRecord::new(
                FaultKind::Transient,
                FaultStage::Transport,
                "worker.write",
                format!("failed to frame worker request: {error}"),
            )
            .retryable(Some(self.generation))
        })?;
        stdin.flush().map_err(|error| {
            FaultRecord::new(
                FaultKind::Transient,
                FaultStage::Transport,
                "worker.write",
                format!("failed to flush worker request: {error}"),
            )
            .retryable(Some(self.generation))
        })?;

        if self.crash_before_reply_once {
            self.crash_before_reply_once = false;
            self.kill_current_worker();
            return Err(FaultRecord::new(
                FaultKind::Transient,
                FaultStage::Transport,
                "worker.read",
                "worker crashed before replying",
            )
            .retryable(Some(self.generation)));
        }

        let stdout = self.stdout.as_mut().ok_or_else(|| {
            FaultRecord::new(
                FaultKind::Transient,
                FaultStage::Transport,
                "worker.stdout",
                "worker stdout is not available",
            )
            .retryable(Some(self.generation))
        })?;
        let mut line = String::new();
        let bytes = stdout.read_line(&mut line).map_err(|error| {
            FaultRecord::new(
                FaultKind::Transient,
                FaultStage::Transport,
                "worker.read",
                format!("failed to read worker response: {error}"),
            )
            .retryable(Some(self.generation))
        })?;
        if bytes == 0 {
            self.kill_current_worker();
            return Err(FaultRecord::new(
                FaultKind::Transient,
                FaultStage::Transport,
                "worker.read",
                "worker exited before replying",
            )
            .retryable(Some(self.generation)));
        }
        let response = serde_json::from_str::<WorkerResponse>(&line).map_err(|error| {
            FaultRecord::new(
                FaultKind::Transient,
                FaultStage::Protocol,
                "worker.read",
                format!("invalid worker response: {error}"),
            )
            .retryable(Some(self.generation))
        })?;
        match response.outcome {
            WorkerOutcome::Success { result } => Ok(result),
            WorkerOutcome::Fault { fault } => Err(fault),
        }
    }

    pub(super) fn restart(&mut self) -> Result<(), FaultRecord> {
        self.kill_current_worker();
        self.ensure_worker()
    }

    pub(super) fn is_alive(&mut self) -> bool {
        let Some(child) = self.child.as_mut() else {
            return false;
        };
        if let Ok(None) = child.try_wait() {
            true
        } else {
            self.child = None;
            self.stdin = None;
            self.stdout = None;
            false
        }
    }

    pub(super) fn arm_crash_once(&mut self) {
        self.crash_before_reply_once = true;
    }

    fn ensure_worker(&mut self) -> Result<(), FaultRecord> {
        if self.is_alive() {
            return Ok(());
        }
        let Some(project_root) = self.bound_project_root.as_ref() else {
            return Err(FaultRecord::new(
                FaultKind::Unavailable,
                FaultStage::Host,
                "worker.spawn",
                "project is not bound; call project.bind before using project tools",
            ));
        };
        self.generation += 1;
        let mut child = Command::new(&self.config.executable)
            .arg("mcp")
            .arg("worker")
            .arg("--project")
            .arg(project_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| {
                FaultRecord::new(
                    FaultKind::Transient,
                    FaultStage::Transport,
                    "worker.spawn",
                    format!("failed to spawn worker: {error}"),
                )
                .retryable(Some(self.generation))
            })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            FaultRecord::new(
                FaultKind::Internal,
                FaultStage::Transport,
                "worker.spawn",
                "worker stdin pipe was not created",
            )
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            FaultRecord::new(
                FaultKind::Internal,
                FaultStage::Transport,
                "worker.spawn",
                "worker stdout pipe was not created",
            )
        })?;
        self.child = Some(child);
        self.stdin = Some(BufWriter::new(stdin));
        self.stdout = Some(BufReader::new(stdout));
        Ok(())
    }

    fn kill_current_worker(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
        self.stdin = None;
        self.stdout = None;
    }
}
