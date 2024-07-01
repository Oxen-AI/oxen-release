//! # oxen workspace diff
//!
//! Compare remote files and directories between versions
//!

use std::path::Path;

use polars::frame::DataFrame;

use crate::api;
use crate::constants::DEFAULT_PAGE_NUM;
use crate::constants::DEFAULT_PAGE_SIZE;
use crate::error::OxenError;
use crate::model::LocalRepository;

pub async fn diff(
    repo: &LocalRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<DataFrame, OxenError> {
    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    let diff = api::remote::workspaces::data_frames::diff(
        &remote_repo,
        workspace_id,
        path,
        DEFAULT_PAGE_NUM,
        DEFAULT_PAGE_SIZE,
    )
    .await?;
    let df = diff.view.to_df();
    Ok(df)
}
