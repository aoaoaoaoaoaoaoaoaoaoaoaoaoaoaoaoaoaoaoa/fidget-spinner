use std::path::{Path, PathBuf};

use fidget_spinner_store_sqlite::StoreError;

#[derive(Clone, Debug)]
pub(super) struct HostConfig {
    pub(super) executable: PathBuf,
    pub(super) initial_project: Option<PathBuf>,
}

impl HostConfig {
    pub(super) fn new(initial_project: Option<&Path>) -> Result<Self, StoreError> {
        Ok(Self {
            executable: std::env::current_exe()?,
            initial_project: initial_project.map(Path::to_path_buf),
        })
    }
}
