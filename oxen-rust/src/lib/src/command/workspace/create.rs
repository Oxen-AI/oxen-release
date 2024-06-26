//! # oxen workspace create
//!
//! Create a new workspace
//!

use crate::api;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::view::workspaces::WorkspaceResponse;

/// Create a new workspace
pub async fn create(
    repo: &LocalRepository,
    branch_name: impl AsRef<str>,
    workspace_id: impl AsRef<str>,
) -> Result<WorkspaceResponse, OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    let workspace =
        api::remote::workspaces::create(&remote_repo, branch_name.as_ref(), workspace_id.as_ref())
            .await?;
    Ok(workspace)
}
