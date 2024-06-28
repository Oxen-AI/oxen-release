//! # oxen branch
//!
//! unlock remote branch
//!

use crate::api;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository};
use crate::view::StatusMessage;

pub fn current(repo: &LocalRepository) -> Result<Option<Branch>, OxenError> {
    let branch = api::local::branches::current_branch(repo)?;
    Ok(branch)
}

pub async fn unlock(
    repository: &LocalRepository,
    remote_name: &str,
    branch: &str,
) -> Result<StatusMessage, OxenError> {
    let remote = repository
        .get_remote(remote_name)
        .ok_or(OxenError::remote_not_set(remote_name))?;
    let remote_repo = api::remote::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;
    api::remote::branches::unlock(&remote_repo, branch).await
}
