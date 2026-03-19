use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use camino::Utf8PathBuf;

use crate::mcp::fault::{FaultKind, FaultRecord, FaultStage};
use crate::mcp::protocol::{WorkerOutcome, WorkerRequest, WorkerResponse};
use crate::mcp::service::WorkerService;

pub(crate) fn serve(project: PathBuf) -> Result<(), fidget_spinner_store_sqlite::StoreError> {
    let project = Utf8PathBuf::from(project.to_string_lossy().into_owned());
    let mut service = WorkerService::new(&project)?;
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => {
                eprintln!("worker stdin failure: {error}");
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }

        let request = match serde_json::from_str::<WorkerRequest>(&line) {
            Ok(request) => request,
            Err(error) => {
                let response = WorkerResponse {
                    id: crate::mcp::protocol::HostRequestId(0),
                    outcome: WorkerOutcome::Fault {
                        fault: FaultRecord::new(
                            FaultKind::InvalidInput,
                            FaultStage::Protocol,
                            "worker.parse",
                            format!("invalid worker request: {error}"),
                        ),
                    },
                };
                write_message(&mut stdout, &response)?;
                continue;
            }
        };

        let WorkerRequest::Execute { id, operation } = request;
        let outcome = match service.execute(operation) {
            Ok(result) => WorkerOutcome::Success { result },
            Err(fault) => WorkerOutcome::Fault { fault },
        };
        write_message(&mut stdout, &WorkerResponse { id, outcome })?;
    }

    Ok(())
}

fn write_message(
    stdout: &mut impl Write,
    response: &WorkerResponse,
) -> Result<(), fidget_spinner_store_sqlite::StoreError> {
    serde_json::to_writer(&mut *stdout, response)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}
