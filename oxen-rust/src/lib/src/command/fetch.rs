//! # oxen fetch
//!
//! Download objects and refs from the remote repository
//!

use crate::api;
use crate::core::index::EntryIndexer;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository, RemoteBranch};

/// # Fetch the remote branches and objects
pub async fn fetch(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    for remote in repo.remotes.iter() {
        fetch_remote(repo, &remote.name).await?;
    }

    Ok(vec![])
}

pub async fn fetch_remote(
    repo: &LocalRepository,
    remote_name: &str,
) -> Result<Vec<Branch>, OxenError> {
    let remote = repo
        .get_remote(remote_name)
        .ok_or(OxenError::remote_not_set(remote_name))?;
    let remote_repo = api::remote::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    let remote_branches = api::remote::branches::list(&remote_repo).await?;
    let local_branches = api::local::branches::list(repo)?;

    // Find branches that are on the remote but not on the local
    let mut branches_to_create = vec![];
    let mut branches_to_fetch = vec![];
    for remote_branch in remote_branches {
        if !local_branches.iter().any(|b| b.name == remote_branch.name) {
            branches_to_create.push(remote_branch);
        } else {
            branches_to_fetch.push(remote_branch);
        }
    }

    // Pull the new branches
    let indexer = EntryIndexer::new(repo)?;
    for branch in branches_to_create {
        println!("Fetch remote branch: {}/{}", remote_name, branch.name);
        let rb = RemoteBranch {
            remote: remote.name.to_owned(),
            branch: branch.name.to_owned(),
        };
        indexer
            .pull_most_recent_commit_object(&remote_repo, &rb, false)
            .await?;
    }

    // TODO: Fetch the branches that already exist

    Ok(vec![])
}
