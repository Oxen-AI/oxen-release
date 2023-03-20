use std::collections::HashMap;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};

use crate::error::OxenError;
use crate::model::staged_data::StagedDataOpts;
use crate::model::{
    Branch, LocalRepository, RemoteRepository, StagedData, StagedEntry, StagedEntryStatus,
};
use crate::{api, command};

pub async fn status_from_local(
    repo: &LocalRepository,
    directory: &Path,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    let branch = command::current_branch(repo)?.expect("Must be on branch.");
    status(&remote_repo, &branch, directory, opts).await
}

pub async fn status(
    remote_repo: &RemoteRepository,
    branch: &Branch,
    directory: &Path,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    let page_size = opts.limit;
    let page_num = opts.skip / page_size;

    let staged_files = api::remote::staging::list_staging_dir(
        remote_repo,
        &branch.name,
        directory,
        page_num,
        page_size,
    )
    .await?;

    let mut status = StagedData::empty();
    status.added_dirs = staged_files.added_dirs;
    status.added_files =
        HashMap::from_iter(staged_files.added_files.entries.into_iter().map(|e| {
            (
                PathBuf::from(e.filename),
                StagedEntry::empty_status(StagedEntryStatus::Added),
            )
        }));

    Ok(status)
}
