//! # oxen remote status
//!
//! Query a remote repository for the status of a branch
//!

use std::path::Path;

use crate::config::UserConfig;
use crate::error::OxenError;
use crate::index::remote_stager;
use crate::model::StagedData;
use crate::model::{staged_data::StagedDataOpts, Branch, RemoteRepository};

pub async fn status(
    remote_repo: &RemoteRepository,
    branch: &Branch,
    directory: &Path,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    let user_id = UserConfig::identifier()?;
    remote_stager::status(remote_repo, branch, &user_id, directory, opts).await
}
