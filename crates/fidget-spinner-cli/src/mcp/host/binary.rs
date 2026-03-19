use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use fidget_spinner_store_sqlite::StoreError;

use crate::mcp::protocol::BinaryFingerprint;

pub(super) struct BinaryRuntime {
    pub(super) path: PathBuf,
    startup_fingerprint: BinaryFingerprint,
    pub(super) launch_path_stable: bool,
}

impl BinaryRuntime {
    pub(super) fn new(path: PathBuf) -> Result<Self, StoreError> {
        let startup_fingerprint = fingerprint_binary(&path)?;
        Ok(Self {
            launch_path_stable: !path
                .components()
                .any(|component| component.as_os_str().to_string_lossy() == "target"),
            path,
            startup_fingerprint,
        })
    }

    pub(super) fn rollout_pending(&self) -> Result<bool, StoreError> {
        Ok(fingerprint_binary(&self.path)? != self.startup_fingerprint)
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
    })
}
