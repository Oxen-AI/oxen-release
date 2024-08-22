use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use indicatif::ProgressBar;

use crate::constants::DEFAULT_REMOTE_NAME;
use crate::core;
use crate::error::OxenError;
use crate::model::entries::commit_entry::Entry;
use crate::model::{
    Branch, Commit, CommitEntry, LocalRepository, MerkleHash, MerkleTreeNodeType, RemoteRepository,
};
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
    // start a timer
    let start = std::time::Instant::now();

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

    let num_bytes = push_remote_repo(repo, &remote_repo, &local_branch).await?;
    println!(
        "🐂 pushed {} in {:?}",
        bytesize::ByteSize::b(num_bytes),
        start.elapsed()
    );
    Ok(local_branch)
}

async fn push_remote_repo(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    local_branch: &Branch,
) -> Result<u64, OxenError> {
    // Check if the remote branch exists, and either push to it or create a new one
    match api::client::branches::get_by_name(remote_repo, &local_branch.name).await? {
        Some(remote_branch) => {
            push_to_existing_branch(repo, remote_repo, local_branch, &remote_branch).await?
        }
        None => push_to_new_branch(repo, remote_repo, local_branch).await?,
    }

    Ok(0)
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
    r_push_node(repo, remote_repo, &commit, &tree.root).await?;
    // Push the commit

    // Set the branch to point to the commit
    Ok(())
}

async fn r_push_node(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit: &Commit,
    node: &MerkleTreeNodeData,
) -> Result<(), OxenError> {
    // Recursively push the node and all its children
    // We want to push all the children before the commit at the root
    for child in &node.children {
        if child.has_children() {
            Box::pin(r_push_node(repo, remote_repo, commit, child)).await?;
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

    // If the node is not a VNode, it does not have file children, so we can return
    if node.dtype != MerkleTreeNodeType::VNode {
        return Ok(());
    }

    // Find the missing files on the server
    // If we just created the node, all files will be missing
    // If the server has the node, but some of the files are missing, we need to push them
    let missing_file_hashes =
        api::client::tree::list_missing_file_hashes(remote_repo, &node.hash).await?;

    println!("got {} missing_file_hashes", missing_file_hashes.len());

    push_files(repo, remote_repo, commit, node, &missing_file_hashes).await?;

    Ok(())
}

async fn push_files(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit: &Commit,
    node: &MerkleTreeNodeData,
    hashes: &HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    // Get all the entries
    // Push them like we do in the old pusher
    let progress_bar = Arc::new(ProgressBar::new(hashes.len() as u64));
    let mut entries: Vec<Entry> = Vec::new();
    for child in &node.children {
        if child.dtype == MerkleTreeNodeType::File {
            if !hashes.contains(&child.hash) {
                continue;
            }

            let file_node = child.file()?;
            entries.push(Entry::CommitEntry(CommitEntry {
                commit_id: commit.id.clone(),
                path: PathBuf::from(file_node.name),
                hash: child.hash.to_string(),
                num_bytes: file_node.num_bytes,
                last_modified_seconds: file_node.last_modified_seconds,
                last_modified_nanoseconds: file_node.last_modified_nanoseconds,
            }));
        }
    }

    println!("pushing {} entries", entries.len());
    core::v0_10_0::index::pusher::push_entries(repo, remote_repo, &entries, &commit, &progress_bar)
        .await?;
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

    // Check if the remote branch is ahead or behind the local branch

    // If it is ahead - see if we can push the missing nodes...
    // I forget the logic here, but if we didn't touch the same nodes, we should
    // be able to push and merge the commits on the server?

    Ok(())
}
