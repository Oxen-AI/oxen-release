use std::collections::HashMap;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};

use crate::error::OxenError;
use crate::model::staged_data::StagedDataOpts;
use crate::model::{LocalRepository, StagedData, StagedEntry, StagedEntryStatus};
use crate::{api, command};

pub async fn status(
    repo: &LocalRepository,
    directory: &Path,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    // Remote Repo should be created before this step
    let branch = command::current_branch(repo)?.expect("Must be on branch.");
    let branch_name = branch.name;
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;

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
