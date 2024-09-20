use crate::error::OxenError;
use crate::model::diff::diff_entries_counts::DiffEntriesCounts;
use crate::model::diff::diff_entry_status::DiffEntryStatus;
use crate::model::merkle_tree::node::FileNode;
use crate::model::{Commit, DiffEntry, LocalRepository};
use crate::opts::DFOpts;

use std::path::{Path, PathBuf};

pub fn list_diff_entries_in_dir_top_level(
    repo: &LocalRepository,
    dir: PathBuf,
    base_commit: &Commit,
    head_commit: &Commit,
    page: usize,
    page_size: usize,
) -> Result<DiffEntriesCounts, OxenError> {
    todo!()
}

pub fn list_diff_entries(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
    dir: PathBuf,
    page: usize,
    page_size: usize,
) -> Result<DiffEntriesCounts, OxenError> {
    todo!()
}

pub fn list_changed_dirs(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Vec<(PathBuf, DiffEntryStatus)>, OxenError> {
    todo!()
}

pub fn diff_entries(
    repo: &LocalRepository,
    file_path: impl AsRef<Path>,
    base_entry: Option<FileNode>,
    base_commit: &Commit,
    head_entry: Option<FileNode>,
    head_commit: &Commit,
    df_opts: DFOpts,
) -> Result<DiffEntry, OxenError> {
    // Assume both entries exist
    let mut status = DiffEntryStatus::Modified;

    // If base entry is none, then it was added
    if base_entry.is_none() && head_entry.is_some() {
        status = DiffEntryStatus::Added;
    }

    // If head entry is none, then it was removed
    if head_entry.is_none() && base_entry.is_some() {
        status = DiffEntryStatus::Removed;
    }

    let should_do_full_diff = true;

    let entry = DiffEntry::from_file_node(
        repo,
        file_path,
        base_entry,
        base_commit,
        head_entry,
        head_commit,
        status,
        should_do_full_diff,
        Some(df_opts),
    )?;

    Ok(entry)
}
