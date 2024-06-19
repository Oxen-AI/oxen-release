//! # oxen remote restore
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
    let branch = api::local::branches::current_branch(repo)?;
    if branch.is_none() {
        return Err(OxenError::must_be_on_valid_branch());
    }
    let branch = branch.unwrap();
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    let user_id = UserConfig::identifier()?;
    api::remote::workspace::restore_df(&remote_repo, &branch.name, &user_id, opts.path.to_owned())
        .await
}
