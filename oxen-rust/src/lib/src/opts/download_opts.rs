use crate::{error::OxenError, model::RemoteRepository, opts::helpers};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct DownloadOpts {
    pub paths: Vec<PathBuf>,
    pub dst: PathBuf,
    pub host: String,
    pub remote: String,
    pub branch: Option<String>,
    pub commit_id: Option<String>,
}

impl DownloadOpts {
    /// Looks at branch or commit id and resolves to commit id. Falls back to main branch.
    pub async fn remote_commit_id(&self, repo: &RemoteRepository) -> Result<String, OxenError> {
        helpers::remote_commit_id(repo, &self.commit_id, &self.branch).await
    }
}
