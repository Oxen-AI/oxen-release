use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::model::{Commit, EntryDataType, MetadataEntry};
use crate::view::compare::AddRemoveModifyCounts;
use crate::view::entry::ResourceVersion;
use crate::{
    api,
    model::{CommitEntry, LocalRepository},
    util,
};

use super::diff_entry_status::DiffEntryStatus;
use super::dir_diff_summary::DirDiffSummary;
use super::generic_diff_summary::GenericDiffSummary;
use super::tabular_diff_summary::TabularDiffSummary;

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

    // Diff summary
    pub diff_summary: Option<GenericDiffSummary>,
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

        let diff_summary = DiffEntry::diff_summary_from_dir(repo, &base_entry, &head_entry);
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
            diff_summary,
        }
    }

    pub fn from_commit_entry(
        repo: &LocalRepository,
        base_entry: Option<CommitEntry>,
        base_commit: &Commit, // pass in commit objects for speed so we don't have to lookup later
        head_entry: Option<CommitEntry>,
        head_commit: &Commit,
        status: DiffEntryStatus,
    ) -> DiffEntry {
        // Need to check whether we have the head or base entry to check data about the file
        let (current_entry, version_path) = if let Some(entry) = &head_entry {
            (entry.clone(), util::fs::version_path(repo, entry))
        } else {
            (
                base_entry.clone().unwrap(),
                util::fs::version_path(repo, &base_entry.clone().unwrap()),
            )
        };
        let data_type = util::fs::file_data_type(&version_path);

        DiffEntry {
            status: status.to_string(),
            data_type: data_type.clone(),
            filename: current_entry.path.as_os_str().to_str().unwrap().to_string(),
            is_dir: false,
            size: current_entry.num_bytes,
            head_resource: DiffEntry::resource_from_entry(head_entry.clone()),
            base_resource: DiffEntry::resource_from_entry(base_entry.clone()),
            head_entry: MetadataEntry::from_commit_entry(repo, head_entry.clone(), head_commit),
            base_entry: MetadataEntry::from_commit_entry(repo, base_entry.clone(), base_commit),
            diff_summary: DiffEntry::diff_summary_from_file(
                repo,
                data_type,
                &base_entry,
                &head_entry,
            ),
        }
    }

    fn resource_from_entry(entry: Option<CommitEntry>) -> Option<ResourceVersion> {
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

    fn diff_summary_from_dir(
        _repo: &LocalRepository,
        _base_dir: &Option<MetadataEntry>,
        _head_dir: &Option<MetadataEntry>,
    ) -> Option<GenericDiffSummary> {
        Some(GenericDiffSummary::DirDiffSummary(DirDiffSummary {
            file_counts: AddRemoveModifyCounts {
                added: 0,
                removed: 0,
                modified: 0,
            },
        }))
    }

    fn diff_summary_from_file(
        repo: &LocalRepository,
        data_type: EntryDataType,
        base_entry: &Option<CommitEntry>,
        head_entry: &Option<CommitEntry>,
    ) -> Option<GenericDiffSummary> {
        // TODO match on type, and create the appropriate summary
        match data_type {
            EntryDataType::Tabular => Some(GenericDiffSummary::TabularDiffSummary(
                TabularDiffSummary::from_commit_entries(repo, base_entry, head_entry),
            )),
            _ => None,
        }
    }
}
