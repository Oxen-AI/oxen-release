use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::model::{Commit, EntryDataType, MetadataEntry};
use crate::view::entry::ResourceVersion;
use crate::{
    api,
    model::{CommitEntry, LocalRepository},
    util,
};

use super::diff_entry_changes::{CountChange, SizeChange};
use super::diff_entry_status::DiffEntryStatus;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DiffEntry {
    pub status: String,
    pub data_type: EntryDataType,
    pub filename: String,
    pub is_dir: bool,
    pub size: u64,
    // Resource
    pub head_resource: Option<ResourceVersion>,
    pub base_resource: Option<ResourceVersion>,

    // Entry
    pub head_entry: Option<MetadataEntry>,
    pub base_entry: Option<MetadataEntry>,
}

impl DiffEntry {
    pub fn has_changes(&self) -> bool {
        // TODO: do a deeper check than size, but this is good for MVP
        match (&self.head_entry, &self.base_entry) {
            (Some(head), Some(base)) => head.size != base.size,
            _ => false,
        }
    }

    pub fn from_dir(
        repo: &LocalRepository,
        base_dir: Option<&PathBuf>,
        base_commit: &Commit,
        head_dir: Option<&PathBuf>,
        head_commit: &Commit,
        status: DiffEntryStatus,
    ) -> DiffEntry {
        // Get the metadata entries
        let base_entry = DiffEntry::metadata_from_dir(repo, base_dir, base_commit);
        let head_entry = DiffEntry::metadata_from_dir(repo, head_dir, head_commit);
        // Need to check whether we have the head or base entry to check data about the file
        let (current_dir, current_entry) = if let Some(dir) = head_dir {
            (dir, head_entry.to_owned().unwrap())
        } else {
            (base_dir.unwrap(), base_entry.to_owned().unwrap())
        };

        DiffEntry {
            status: status.to_string(),
            data_type: EntryDataType::Dir,
            filename: current_dir.as_os_str().to_str().unwrap().to_string(),
            is_dir: true,
            size: current_entry.size,
            head_resource: DiffEntry::resource_from_dir(head_dir, head_commit),
            base_resource: DiffEntry::resource_from_dir(base_dir, base_commit),
            head_entry,
            base_entry,
        }
    }

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

        // TODO: Compute for dirs and files
        // TODO: Do we want to just return the two entry objects under base and head

        // FEELS LIKE
        // 1) We return the base and the head entity, with a type, with metadata per type
        // 2) We return a "diff" structure, that respects that type, ie text diff, image diff, dir diff, tabular diff, etc
        // 3) As long as we have that stubbed out, ship the SIZE diff on the dir, and we can extend other types l8r
        let _size = SizeChange {
            base: 0,
            head: 0,
            delta: 0,
        };

        let _file_counts = CountChange {
            added: 0,
            removed: 0,
            modified: 0,
        };

        DiffEntry {
            status: status.to_string(),
            data_type: util::fs::file_data_type(&version_path),
            filename: current_entry.path.as_os_str().to_str().unwrap().to_string(),
            is_dir: false,
            size: current_entry.num_bytes,
            head_resource: DiffEntry::resource_from_entry(head_entry),
            base_resource: DiffEntry::resource_from_entry(base_entry),
            head_entry: MetadataEntry::from_commit_entry(repo, head_entry),
            base_entry: MetadataEntry::from_commit_entry(repo, base_entry),
        }
    }

    fn resource_from_entry(entry: Option<&CommitEntry>) -> Option<ResourceVersion> {
        entry.map(|entry| ResourceVersion {
            version: entry.commit_id.to_string(),
            path: entry.path.as_os_str().to_str().unwrap().to_string(),
        })
    }

    fn resource_from_dir(dir: Option<&PathBuf>, commit: &Commit) -> Option<ResourceVersion> {
        dir.map(|dir| ResourceVersion {
            version: commit.id.to_string(),
            path: dir.as_os_str().to_str().unwrap().to_string(),
        })
    }

    fn metadata_from_dir(
        repo: &LocalRepository,
        dir: Option<&PathBuf>,
        commit: &Commit,
    ) -> Option<MetadataEntry> {
        if let Some(dir) = dir {
            match api::local::entries::get_meta_entry(repo, commit, dir) {
                Ok(entry) => Some(entry),
                Err(_) => None,
            }
        } else {
            None
        }
    }
}
