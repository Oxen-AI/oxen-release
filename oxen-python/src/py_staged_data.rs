use pyo3::prelude::*;

use liboxen::model::StagedData as OxenStagedData;
use liboxen::model::{StagedEntry, StagedEntryStatus};
use liboxen::view::RemoteStagedStatus;
use std::collections::HashMap;
use std::path::PathBuf;

// use crate::error::PyOxenError;

#[pyclass]
pub struct PyStagedData {
    pub data: OxenStagedData,
}

#[pymethods]
impl PyStagedData {
    pub fn added_files(&self) -> PyResult<Vec<String>> {
        Ok(self
            .data
            .added_files
            .iter()
            .map(|f| String::from(f.0.to_string_lossy()))
            .collect())
    }
}

impl From<RemoteStagedStatus> for PyStagedData {
    fn from(remote_status: RemoteStagedStatus) -> PyStagedData {
        let mut status = OxenStagedData::empty();
        status.added_dirs = remote_status.added_dirs;
        let added_files: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.added_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename),
                    StagedEntry::empty_status(StagedEntryStatus::Added),
                )
            }));
        let added_mods: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.modified_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename),
                    StagedEntry::empty_status(StagedEntryStatus::Modified),
                )
            }));
        status.added_files = added_files.into_iter().chain(added_mods).collect();
        PyStagedData { data: status }
    }
}
