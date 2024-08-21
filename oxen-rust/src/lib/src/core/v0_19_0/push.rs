use crate::constants::DEFAULT_REMOTE_NAME;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository, RemoteRepository};
use crate::{api, repositories};

use super::index::merkle_tree::node::MerkleTreeNodeData;
use super::index::merkle_tree::CommitMerkleTree;

pub async fn push(repo: &LocalRepository) -> Result<Branch, OxenError> {
    let Some(current_branch) = repositories::branches::current_branch(repo)? else {
        return Err(OxenError::must_be_on_valid_branch());
    };
    push_remote_branch(repo, DEFAULT_REMOTE_NAME, current_branch.name).await
}

pub async fn push_remote_branch(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch_name: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    let remote = remote.as_ref();
    let branch_name = branch_name.as_ref();

    let Some(local_branch) = repositories::branches::get_by_name(repo, branch_name)? else {
        return Err(OxenError::local_branch_not_found(branch_name));
    };

    println!(
        "🐂 Oxen push {} {} -> {}",
        remote, local_branch.name, local_branch.commit_id
    );

    let remote = repo
        .get_remote(&remote)
        .ok_or(OxenError::remote_not_set(&remote))?;

    let remote_repo = match api::client::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    push_remote_repo(repo, &remote_repo, &local_branch).await?;
    Ok(local_branch)
}

async fn push_remote_repo(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    local_branch: &Branch,
) -> Result<(), OxenError> {
    // Check if the remote branch exists, and either push to it or create a new one
    match api::client::branches::get_by_name(remote_repo, &local_branch.name).await? {
        Some(remote_branch) => {
            push_to_existing_branch(repo, remote_repo, local_branch, &remote_branch).await?
        }
        None => push_to_new_branch(repo, remote_repo, local_branch).await?,
    }

    Ok(())
}

async fn push_to_new_branch(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    branch: &Branch,
) -> Result<(), OxenError> {
    // Get the commit from the branch
    let Some(commit) = repositories::commits::get_by_id(repo, &branch.commit_id)? else {
        return Err(OxenError::revision_not_found(
            branch.commit_id.clone().into(),
        ));
    };
    // Figure out which nodes we need to push
    let tree = CommitMerkleTree::from_commit(repo, &commit)?;

    //

    // Push each node, and all their file children
    r_push_node(repo, remote_repo, &tree.root).await?;
    // Push the commit

    // Set the branch to point to the commit
    Ok(())
}

async fn r_push_node(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    node: &MerkleTreeNodeData,
) -> Result<(), OxenError> {
    // Push the root last
    for child in &node.children {
        if child.has_children() {
            Box::pin(r_push_node(repo, remote_repo, child)).await?;
        }
    }

    println!("r_push_node: {}", node);
    // Check if the node exists on the remote
    let has_node = api::client::tree::has_node(remote_repo, node.hash).await?;
    println!("has_node: {:?}", has_node);

    // If not exists, create it
    if !has_node {
        // Create the node on the server
        println!("Creating node on the server: {}", node);
        api::client::tree::create_node(repo, remote_repo, node).await?;
    }

    // Find the missing children on the server
    // If we just created the node, all children will be missing
    // If the server has the node, but some of the children are missing, we need to push them
    // If the server has the node and all the children, we can move onto the next node

    // If all children exist, return
    // This way we don't have to push the same node twice

    push_node(repo, remote_repo, node).await?;

    Ok(())
}

async fn push_node(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    node: &MerkleTreeNodeData,
) -> Result<(), OxenError> {
    println!("Pushing node: {}", node);
    Ok(())
}

async fn push_to_existing_branch(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    local_branch: &Branch,
    remote_branch: &Branch,
) -> Result<(), OxenError> {
    // Check if the latest commit on the remote is the same as the local branch
    let head_commit = repositories::commits::head_commit(repo)?;

    if remote_branch.commit_id == head_commit.id {
        println!("Everything is up to date");
        return Ok(());
    }

    Ok(())
}
