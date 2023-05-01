//! # oxen pull
//!
//! Pull data from a remote branch
//!

use crate::error::OxenError;
use crate::core::index::EntryIndexer;
use crate::model::{LocalRepository, RemoteBranch};

/// Pull a repository's data from default branches origin/main
/// Defaults defined in
/// `constants::DEFAULT_REMOTE_NAME` and `constants::DEFAULT_BRANCH_NAME`
pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer.pull(&rb).await?;
    Ok(())
}

/// Pull a specific remote and branch
pub async fn pull_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch: &str,
) -> Result<(), OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch),
    };
    indexer.pull(&rb).await?;
    Ok(())
}
