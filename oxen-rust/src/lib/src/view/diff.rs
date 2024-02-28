use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::model::diff::diff_entry_status::DiffEntryStatus;

use super::StatusMessage;
#[derive(Deserialize, Serialize, Debug)]
pub struct DirTreeDiffResponse {
    pub dirs: Vec<DirDiffChildrenSummary>,
    #[serde(flatten)]
    pub status: StatusMessage,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DirDiffChildrenSummary {
    pub name: PathBuf,
    pub num_subdirs: usize,
    pub can_display: bool,
    pub children: Vec<DirDiffStatus>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DirDiffStatus {
    pub name: PathBuf,
    pub status: DiffEntryStatus,
}
