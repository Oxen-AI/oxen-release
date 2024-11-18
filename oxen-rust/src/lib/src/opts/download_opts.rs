use crate::{error::OxenError, model::RemoteRepository, opts::helpers};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct DownloadOpts {
    pub url: String,
    pub paths: Vec<PathBuf>,
    pub dst: PathBuf,
    pub revision: Option<String>,
}

impl DownloadOpts {
    /// Looks at branch or commit id and resolves to commit id. Falls back to main branch.
    pub async fn remote_commit_id(&self, repo: &RemoteRepository) -> Result<String, OxenError> {
        helpers::remote_commit_id(repo, self.revision.clone()).await
    }
}
