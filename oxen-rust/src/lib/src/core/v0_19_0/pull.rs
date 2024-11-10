use crate::api;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteBranch};
use crate::repositories;

use crate::core::v0_19_0::fetch;
use crate::opts::fetch_opts::FetchOpts;

pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let fetch_opts = FetchOpts::new();
    pull_remote_branch(repo, &fetch_opts).await
}

pub async fn pull_shallow(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch: impl AsRef<str>,
) -> Result<(), OxenError> {
    let remote = remote.as_ref();
    let branch = branch.as_ref();
    println!("🐂 oxen pull {} {}", remote, branch);

    let remote = repo
        .get_remote(remote)
        .ok_or(OxenError::remote_not_set(remote))?;

    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    let rb = RemoteBranch {
        remote: remote.to_string(),
        branch: branch.to_string(),
    };

    let Some(branch) = api::client::branches::get_by_name(&remote_repo, &rb.branch).await? else {
        return Err(OxenError::remote_branch_not_found(&rb.branch));
    };

    // Fetch all the tree nodes
    fetch::fetch_tree_and_hashes_for_commit_id(repo, &remote_repo, &branch.commit_id).await?;

    // Mark the repo as shallow, because we only fetched the commit history
    repo.write_is_shallow(true)?;

    Ok(())
}

pub async fn pull_all(repo: &LocalRepository) -> Result<(), OxenError> {
    let fetch_opts = FetchOpts {
        all: true,
        ..FetchOpts::new()
    };
    repositories::pull_remote_branch(repo, &fetch_opts).await
}

/// Pull a specific remote and branch
pub async fn pull_remote_branch(
    repo: &LocalRepository,
    fetch_opts: &FetchOpts,
) -> Result<(), OxenError> {
    let remote = &fetch_opts.remote;
    let branch = &fetch_opts.branch;
    println!("🐂 oxen pull {} {}", remote, branch);

    let remote = repo
        .get_remote(remote)
        .ok_or(OxenError::remote_not_set(remote))?;

    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    let rb = RemoteBranch {
        remote: remote.to_string(),
        branch: branch.to_string(),
    };

    let previous_head_commit = repositories::commits::head_commit_maybe(repo)?;

    // Fetch all the tree nodes and the entries
    fetch::fetch_remote_branch(repo, &remote_repo, &rb, fetch_opts).await?;

    let new_head_commit = repositories::revisions::get(repo, branch)?
        .ok_or(OxenError::revision_not_found(branch.to_owned().into()))?;

    // Merge if there are changes
    if let Some(previous_head_commit) = &previous_head_commit {
        log::debug!(
            "checking if we need to merge previous {} new {}",
            previous_head_commit.id,
            new_head_commit.id
        );
        if previous_head_commit.id != new_head_commit.id {
            repositories::merge::merge_commit_into_base(
                repo,
                &new_head_commit,
                previous_head_commit,
            )?;
        }
    }

    // TODO: this should ideally be in the repositories::pull module,
    // but I'm not sure how that will interact with the v0_10_0 code
    repositories::branches::checkout_branch_from_commit(repo, branch, &previous_head_commit)
        .await?;

    repositories::branches::set_head(repo, branch)?;

    Ok(())
}
