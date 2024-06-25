//! # oxen remote status
//!
//! Query a remote repository for the status of a branch
//!

use std::path::Path;

use std::collections::HashMap;
use std::path::PathBuf;

use crate::api;
use crate::error::OxenError;
use crate::model::StagedData;
use crate::model::{staged_data::StagedDataOpts, RemoteRepository, StagedEntry, StagedEntryStatus};

pub async fn status(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    directory: &Path,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    let page_size = opts.limit;
    let page_num = opts.skip / page_size;

    let remote_status =
        api::remote::workspaces::status(remote_repo, workspace_id, directory, page_num, page_size)
            .await?;

    let mut status = StagedData::empty();
    status.staged_dirs = remote_status.added_dirs;
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
    status.staged_files = added_files.into_iter().chain(added_mods).collect();

    Ok(status)
}
