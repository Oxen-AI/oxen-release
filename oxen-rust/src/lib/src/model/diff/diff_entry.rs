use std::collections::HashSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::core::index::{CommitDirEntryReader, CommitEntryReader};
use crate::error::OxenError;
use crate::model::diff::dir_diff_summary::DirDiffSummaryImpl;
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
    ) -> Result<DiffEntry, OxenError> {
        // Get the metadata entries
        let base_entry = DiffEntry::metadata_from_dir(repo, base_dir, base_commit);
        let head_entry = DiffEntry::metadata_from_dir(repo, head_dir, head_commit);
        // Need to check whether we have the head or base entry to check data about the file
        let (current_dir, current_entry) = if let Some(dir) = head_dir {
            (dir, head_entry.to_owned().unwrap())
        } else {
            (base_dir.unwrap(), base_entry.to_owned().unwrap())
        };

        let diff_summary = DiffEntry::diff_summary_from_dir(repo, &base_entry, &head_entry)?;
        Ok(DiffEntry {
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
        })
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
        repo: &LocalRepository,
        base_dir: &Option<MetadataEntry>,
        head_dir: &Option<MetadataEntry>,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        log::debug!("diff_summary_from_dir base_dir: {:?}", base_dir);
        log::debug!("diff_summary_from_dir head_dir: {:?}", head_dir);

        // if both base_dir and head_dir are none, then there is no diff summary
        if base_dir.is_none() && head_dir.is_none() {
            return Ok(None);
        }

        // if base_dir is some and head_dir is none, then we deleted all the files
        if base_dir.is_some() && head_dir.is_none() {
            return DiffEntry::r_compute_removed_files(repo, base_dir.as_ref().unwrap());
        }

        // if head_dir is some and base_dir is none, then we added all the files
        if head_dir.is_some() && base_dir.is_none() {
            return DiffEntry::r_compute_added_files(repo, head_dir.as_ref().unwrap());
        }

        // if both base_dir and head_dir are some, then we need to compare the two
        let base_dir = base_dir.as_ref().unwrap();
        let head_dir = head_dir.as_ref().unwrap();

        DiffEntry::r_compute_diff_all_files(repo, base_dir, head_dir)
    }

    fn r_compute_diff_all_files(
        repo: &LocalRepository,
        base_dir: &MetadataEntry,
        head_dir: &MetadataEntry,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        let base_commit_id = &base_dir.latest_commit.as_ref().unwrap().id;
        let head_commit_id = &head_dir.latest_commit.as_ref().unwrap().id;

        // base and head path will be the same so just choose base
        let path = PathBuf::from(&base_dir.resource.clone().unwrap().path);

        let mut num_removed = 0;
        let mut num_added = 0;
        let mut num_modified = 0;

        // Find all the children of the dir and sum up their counts
        let commit_entry_reader = CommitEntryReader::new_from_commit_id(repo, base_commit_id)?;
        let mut dirs = commit_entry_reader.list_dir_children(&path)?;

        let commit_entry_reader = CommitEntryReader::new_from_commit_id(repo, head_commit_id)?;
        let mut other = commit_entry_reader.list_dir_children(&path)?;
        dirs.append(&mut other);
        dirs.push(path.clone());

        // Uniq them
        let dirs: HashSet<PathBuf> = HashSet::from_iter(dirs.into_iter());

        for dir in dirs {
            let base_dir_reader = CommitDirEntryReader::new(repo, base_commit_id, &dir)?;
            let head_dir_reader = CommitDirEntryReader::new(repo, head_commit_id, &dir)?;

            // List the entries in hash sets
            let head_entries = head_dir_reader.list_entries_set()?;
            let base_entries = base_dir_reader.list_entries_set()?;
            log::debug!(
                "diff_summary_from_dir head_entries: {:?}",
                head_entries.len()
            );
            log::debug!(
                "diff_summary_from_dir base_entries: {:?}",
                base_entries.len()
            );

            // Find the added entries
            let added_entries = head_entries
                .difference(&base_entries)
                .collect::<HashSet<_>>();
            num_added += added_entries.len();

            // Find the removed entries
            let removed_entries = base_entries
                .difference(&head_entries)
                .collect::<HashSet<_>>();
            num_removed += removed_entries.len();

            // Find the modified entries
            for base_entry in base_entries {
                if let Some(head_entry) = head_entries.get(&base_entry) {
                    if head_entry.hash != base_entry.hash {
                        num_modified += 1;
                    }
                }
            }
        }

        Ok(Some(GenericDiffSummary::DirDiffSummary(DirDiffSummary {
            dir: DirDiffSummaryImpl {
                file_counts: AddRemoveModifyCounts {
                    added: num_added,
                    removed: num_removed,
                    modified: num_modified,
                },
            },
        })))
    }

    fn r_compute_removed_files(
        repo: &LocalRepository,
        base_dir: &MetadataEntry,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        let commit_id = &base_dir.latest_commit.as_ref().unwrap().id;
        let path = PathBuf::from(&base_dir.resource.clone().unwrap().path);
        log::debug!("r_compute_removed_files base_dir: {:?}", path);

        // Count all removals in the directory and its children
        let commit_entry_reader = CommitEntryReader::new_from_commit_id(repo, commit_id)?;
        let mut dirs = commit_entry_reader.list_dir_children(&path)?;
        dirs.push(path);

        log::debug!("r_compute_removed_files got dirs: {:?}", dirs.len());

        let mut num_removed = 0;
        for dir in dirs {
            let dir_reader = CommitDirEntryReader::new(repo, commit_id, &dir)?;
            let count = dir_reader.num_entries();
            log::debug!("r_compute_removed_files dir: {:?} count: {}", dir, count);

            num_removed += count;
        }

        Ok(Some(GenericDiffSummary::DirDiffSummary(DirDiffSummary {
            dir: DirDiffSummaryImpl {
                file_counts: AddRemoveModifyCounts {
                    added: 0,
                    removed: num_removed,
                    modified: 0,
                },
            },
        })))
    }

    fn r_compute_added_files(
        repo: &LocalRepository,
        head_dir: &MetadataEntry,
    ) -> Result<Option<GenericDiffSummary>, OxenError> {
        let commit_id = &head_dir.latest_commit.as_ref().unwrap().id;
        let path = PathBuf::from(&head_dir.resource.clone().unwrap().path);
        log::debug!("r_compute_added_files base_dir: {:?}", path);

        // Count all removals in the directory and its children
        let commit_entry_reader = CommitEntryReader::new_from_commit_id(repo, commit_id)?;
        let mut dirs = commit_entry_reader.list_dir_children(&path)?;
        dirs.push(path);

        log::debug!("r_compute_added_files got dirs: {:?}", dirs.len());

        let mut num_added = 0;
        for dir in dirs {
            let dir_reader = CommitDirEntryReader::new(repo, commit_id, &dir)?;
            let count = dir_reader.num_entries();
            log::debug!("r_compute_added_files dir: {:?} count: {}", dir, count);

            num_added += count;
        }

        Ok(Some(GenericDiffSummary::DirDiffSummary(DirDiffSummary {
            dir: DirDiffSummaryImpl {
                file_counts: AddRemoveModifyCounts {
                    added: num_added,
                    removed: 0,
                    modified: 0,
                },
            },
        })))
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
