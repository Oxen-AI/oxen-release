use std::collections::HashMap;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};

use crate::constants::DEFAULT_REMOTE_NAME;
use crate::error::OxenError;
use crate::model::staged_data::StagedDataOpts;
use crate::model::{LocalRepository, RemoteBranch, StagedData, StagedEntry, StagedEntryStatus};
use crate::{api, command};

pub async fn status(
    repo: &LocalRepository,
    directory: &Path,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    // Repo should be created before this step
    let branch = command::current_branch(repo)?.expect("Must be on branch.");
    let branch_name = branch.name;
    let rb = RemoteBranch {
        remote: DEFAULT_REMOTE_NAME.to_string(),
        branch: branch_name.to_owned(),
    };
    let remote = repo
        .get_remote(&rb.remote)
        .ok_or_else(OxenError::remote_not_set)?;
    let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    let page_size = opts.limit;
    let page_num = opts.skip / page_size;

    let staged_files = api::remote::staging::list_staging_dir(
        &remote_repo,
        &branch_name,
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
