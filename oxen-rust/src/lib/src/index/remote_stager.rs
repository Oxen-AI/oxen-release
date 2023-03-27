use std::collections::HashMap;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};

use crate::config::UserConfig;
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
    let user_id = UserConfig::identifier()?;
    status(&remote_repo, &branch, &user_id, directory, opts).await
}

pub async fn status(
    remote_repo: &RemoteRepository,
    branch: &Branch,
    user_id: &str,
    directory: &Path,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    let page_size = opts.limit;
    let page_num = opts.skip / page_size;

    let staged_files = api::remote::staging::list_staging_dir(
        remote_repo,
        &branch.name,
        user_id,
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
    status.modified_files = Vec::from_iter(
        staged_files
            .modified_files
            .entries
            .into_iter()
            .map(|e| PathBuf::from(e.filename)),
    );

    Ok(status)
}
