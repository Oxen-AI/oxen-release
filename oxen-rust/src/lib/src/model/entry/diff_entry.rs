use serde::{Deserialize, Serialize};

use crate::model::EntryDataType;
use crate::view::entry::ResourceVersion;
use crate::{
    model::{CommitEntry, LocalRepository},
    util,
};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum DiffEntryStatus {
    Added,
    Modified,
    Removed,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DiffEntry {
    pub status: DiffEntryStatus,
    pub data_type: EntryDataType,
    pub filename: String,
    pub is_dir: bool,
    pub size: u64,
    pub head_resource: Option<ResourceVersion>,
    pub base_resource: Option<ResourceVersion>,
}

impl DiffEntry {
    pub fn from_commit_entry(
        repo: &LocalRepository,
        base_entry: Option<&CommitEntry>,
        head_entry: Option<&CommitEntry>,
        status: DiffEntryStatus,
    ) -> DiffEntry {
        // Need to check whether we have the head or base entry to check data about the file
        let (current_entry, version_path) = if let Some(entry) = head_entry {
            (entry, util::fs::version_path(repo, head_entry.unwrap()))
        } else {
            (
                base_entry.unwrap(),
                util::fs::version_path(repo, base_entry.unwrap()),
            )
        };

        DiffEntry {
            status,
            data_type: util::fs::file_datatype(&version_path),
            filename: current_entry.path.as_os_str().to_str().unwrap().to_string(),
            is_dir: version_path.is_dir(),
            size: current_entry.num_bytes,
            head_resource: DiffEntry::resource_from_entry(head_entry),
            base_resource: DiffEntry::resource_from_entry(base_entry),
        }
    }

    fn resource_from_entry(entry: Option<&CommitEntry>) -> Option<ResourceVersion> {
        entry.map(|entry| ResourceVersion {
            version: entry.commit_id.to_string(),
            path: entry.path.as_os_str().to_str().unwrap().to_string(),
        })
    }
}
