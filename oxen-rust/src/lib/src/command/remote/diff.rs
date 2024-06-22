//! # oxen remote diff
//!
//! Compare remote files and directories between versions
//!

use std::path::Path;

use crate::api;
use crate::config::UserConfig;
use crate::constants::DEFAULT_PAGE_NUM;
use crate::constants::DEFAULT_PAGE_SIZE;
use crate::error::OxenError;
use crate::model::diff::DiffResult;
use crate::model::LocalRepository;

pub async fn diff(
    repo: &LocalRepository,
    branch_name: Option<impl AsRef<str>>,
    path: &Path,
) -> Result<DiffResult, OxenError> {
    let branch = api::local::branches::get_by_name_or_current(repo, branch_name)?;
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    let user_id = UserConfig::identifier()?;
    let diff = api::remote::workspaces::diff(
        &remote_repo,
        &branch.name,
        &user_id,
        path,
        DEFAULT_PAGE_NUM,
        DEFAULT_PAGE_SIZE,
    )
    .await?;
    Ok(diff)
}
