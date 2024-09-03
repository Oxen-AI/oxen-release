use crate::core::refs::RefReader;
use crate::core::v0_19_0::index::commit_merkle_tree::CommitMerkleTree;
use crate::core::v0_19_0::{commits, restore};
use crate::error::OxenError;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::{Commit, CommitEntry, LocalRepository, MerkleTreeNodeType};
use crate::repositories;
use crate::util;

use std::path::Path;

pub fn list_entry_versions_for_commit(
    local_repo: &LocalRepository,
    commit_id: &str,
    path: &Path,
) -> Result<Vec<(Commit, CommitEntry)>, OxenError> {
    todo!()
}

pub async fn checkout(repo: &LocalRepository, branch_name: &str) -> Result<(), OxenError> {
    let branch = repositories::branches::get_by_name(repo, branch_name)?
        .ok_or(OxenError::local_branch_not_found(branch_name))?;

    checkout_commit_id(repo, &branch.commit_id).await?;
    // Pull changes if needed
    // TODO

    Ok(())
}

pub async fn checkout_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<(), OxenError> {
    let commit = repositories::commits::get_by_id(repo, &commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(&commit_id))?;

    // Set working repo to commit
    set_working_repo_to_commit(repo, &commit).await?;
    Ok(())
}

pub async fn set_working_repo_to_commit(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<(), OxenError> {
    let head_commit = commits::head_commit(repo)?;
    if head_commit.id == commit.id {
        log::debug!(
            "set_working_repo_to_commit, do nothing... head commit == commit_id {}",
            commit.id
        );
        return Ok(());
    }

    let tree = CommitMerkleTree::from_commit(repo, commit)?;

    // Cleanup removed files

    // Restore missing or modified files
    r_restore_missing_or_modified_files(repo, &tree.root, Path::new(""))?;

    // Remove untracked directories

    Ok(())
}

fn r_restore_missing_or_modified_files(
    repo: &LocalRepository,
    node: &MerkleTreeNode,
    path: &Path,
) -> Result<(), OxenError> {
    // Recursively iterate through the tree, checking each file against the working repo
    // If the file is not in the working repo, restore it from the commit
    // If the file is in the working repo, but the hash does not match, overwrite the file in the working repo with the file from the commit
    // If the file is in the working repo, and the hash matches, do nothing

    match &node.node.dtype() {
        MerkleTreeNodeType::File => {
            let file_node = node.file().unwrap();
            let rel_path = path.join(file_node.name.clone());
            let full_path = repo.path.join(&rel_path);
            if !full_path.exists() {
                // File doesn't exist, restore it
                log::debug!("Restoring missing file: {:?}", rel_path);
                restore::restore_file(repo, &file_node.hash, &rel_path)?;
            } else {
                // File exists, check if it needs to be updated
                let current_hash = util::hasher::hash_file_contents(&full_path)?;
                if current_hash != file_node.hash.to_string() {
                    log::debug!("Updating modified file: {:?}", rel_path);
                    restore::restore_file(repo, &file_node.hash, &rel_path)?;
                }
            }
        }
        MerkleTreeNodeType::Dir => {
            // Recursively call for each file and directory
            let children = CommitMerkleTree::node_files_and_folders(node)?;
            let dir_node = node.dir().unwrap();
            let dir_path = path.join(dir_node.name);
            for child_node in children {
                r_restore_missing_or_modified_files(repo, &child_node, &dir_path)?;
            }
        }
        MerkleTreeNodeType::Commit => {
            // If we get a commit node, we need to skip to the root directory
            let root_dir = CommitMerkleTree::get_root_dir_from_commit(node)?;
            r_restore_missing_or_modified_files(repo, root_dir, path)?;
        }
        _ => {
            return Err(OxenError::basic_str(
                "Got an unexpected node type during checkout",
            ));
        }
    }
    Ok(())
}
