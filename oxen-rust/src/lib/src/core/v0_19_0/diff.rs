use crate::error::OxenError;
use crate::model::diff::diff_entries_counts::DiffEntriesCounts;
use crate::model::diff::diff_entry_status::DiffEntryStatus;
use crate::model::{Commit, LocalRepository};

use std::path::PathBuf;

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
