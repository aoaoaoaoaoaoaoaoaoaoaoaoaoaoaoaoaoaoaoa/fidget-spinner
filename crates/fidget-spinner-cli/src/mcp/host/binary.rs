use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fidget_spinner_store_sqlite::StoreError;

const SUCCESSOR_SETTLE_TIME: Duration = Duration::from_millis(200);

#[derive(Clone, Debug, Eq, PartialEq)]
struct BinaryFingerprint {
    length_bytes: u64,
    modified_unix_nanos: u128,
    filesystem_identity: Option<FilesystemIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesystemIdentity {
    device_id: u64,
    inode: u64,
}

struct SuccessorCandidate {
    fingerprint: BinaryFingerprint,
    first_observed_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BinaryObservation {
    Incumbent,
    SuccessorSettling,
    SuccessorReady,
}

impl BinaryObservation {
    pub(super) fn rollout_pending(self) -> bool {
        self != Self::Incumbent
    }

    pub(super) fn rollout_ready(self) -> bool {
        self == Self::SuccessorReady
    }
}

pub(super) struct BinaryRuntime {
    pub(super) path: PathBuf,
    incumbent_fingerprint: BinaryFingerprint,
    successor: Option<SuccessorCandidate>,
    pub(super) launch_path_stable: bool,
}

impl BinaryRuntime {
    pub(super) fn new(path: PathBuf) -> Result<Self, StoreError> {
        let incumbent_fingerprint = fingerprint_binary(&path)?;
        Ok(Self {
            launch_path_stable: !path
                .components()
                .any(|component| component.as_os_str().to_string_lossy() == "target"),
            path,
            incumbent_fingerprint,
            successor: None,
        })
    }

    pub(super) fn observe_rollout(&mut self) -> Result<BinaryObservation, StoreError> {
        let fingerprint = match fingerprint_binary(&self.path) {
            Ok(fingerprint) => fingerprint,
            Err(StoreError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                self.successor = None;
                return Ok(BinaryObservation::SuccessorSettling);
            }
            Err(error) => return Err(error),
        };
        if fingerprint == self.incumbent_fingerprint {
            self.successor = None;
            return Ok(BinaryObservation::Incumbent);
        }
        match self.successor.as_ref() {
            Some(candidate)
                if candidate.fingerprint == fingerprint
                    && candidate.first_observed_at.elapsed() >= SUCCESSOR_SETTLE_TIME =>
            {
                Ok(BinaryObservation::SuccessorReady)
            }
            Some(candidate) if candidate.fingerprint == fingerprint => {
                Ok(BinaryObservation::SuccessorSettling)
            }
            _ => {
                self.successor = Some(SuccessorCandidate {
                    fingerprint,
                    first_observed_at: Instant::now(),
                });
                Ok(BinaryObservation::SuccessorSettling)
            }
        }
    }
}

fn fingerprint_binary(path: &Path) -> Result<BinaryFingerprint, StoreError> {
    let metadata = fs::metadata(path)?;
    let modified_unix_nanos = metadata
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| io::Error::other(format!("invalid binary mtime: {error}")))?
        .as_nanos();
    Ok(BinaryFingerprint {
        length_bytes: metadata.len(),
        modified_unix_nanos,
        #[cfg(unix)]
        filesystem_identity: Some(FilesystemIdentity {
            device_id: metadata.dev(),
            inode: metadata.ino(),
        }),
        #[cfg(not(unix))]
        filesystem_identity: None,
    })
}
