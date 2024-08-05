//! # oxen workspace restore
//!
//! List files in a remote repository branch
//!

use crate::api;
use crate::config::UserConfig;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RestoreOpts;

/// Remove all staged changes from file on remote
pub async fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    let remote_repo = api::client::repositories::get_default_remote(repo).await?;
    let workspace_id = UserConfig::identifier()?;
    api::client::workspaces::data_frames::restore(&remote_repo, &workspace_id, opts.path.to_owned())
        .await
}
