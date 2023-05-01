//! # oxen remote ls
//!
//! List files in a remote repository branch
//!

use std::path::Path;

use crate::api;
use crate::error::OxenError;
use crate::model::{Branch, RemoteRepository};
use crate::opts::PaginateOpts;
use crate::view::PaginatedDirEntries;

pub async fn ls(
    remote_repo: &RemoteRepository,
    branch: &Branch,
    directory: &Path,
    opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    api::remote::dir::list_dir(
        remote_repo,
        &branch.name,
        directory,
        opts.page_num,
        opts.page_size,
    )
    .await
}
