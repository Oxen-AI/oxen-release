//! # oxen workspace delete
//!
//! Delete a workspace from a remote repository
//!

use crate::api;
use crate::error::OxenError;
use crate::model::LocalRepository;

pub async fn delete(
    repo: &LocalRepository,
    workspace_id: impl AsRef<str>,
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;

    api::remote::workspaces::delete(&remote_repo, &workspace_id).await?;

    Ok(())
}
