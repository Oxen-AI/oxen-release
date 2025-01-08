use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNode};
use crate::model::{Commit, LocalRepository};
use std::path::Path;

use crate::core::v_latest::index::CommitMerkleTree;

pub fn get_file(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    let Some(file_node) = get_file_merkle_tree_node(repo, commit, path)? else {
        return Ok(None);
    };

    if let EMerkleTreeNode::File(file_node) = file_node.node {
        Ok(Some(file_node))
    } else {
        Ok(None)
    }
}

pub fn get_file_merkle_tree_node(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let parent = path.as_ref().parent().unwrap_or(Path::new(""));
    let parent_node = CommitMerkleTree::dir_with_children(repo, commit, parent)?;
    let Some(parent_node) = parent_node else {
        log::debug!("path has no parent: {:?}", path.as_ref());
        return Ok(None);
    };

    let Some(file_name) = path.as_ref().file_name() else {
        log::debug!("path has no file name: {:?}", path.as_ref());
        return Ok(None);
    };

    let file_node = parent_node.get_by_path(file_name)?;
    Ok(file_node)
}
