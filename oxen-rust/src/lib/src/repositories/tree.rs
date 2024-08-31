use std::collections::HashSet;

use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::{LocalRepository, MerkleHash};

pub fn get_node_by_id(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Option<EMerkleTreeNode>, OxenError> {
    let Some(node) = CommitMerkleTree::read_node(repo, hash, false)? else {
        return Ok(None);
    };
    Ok(Some(node.node))
}

pub fn list_missing_file_hashes(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let Some(node) = CommitMerkleTree::read_node(repo, hash, false)? else {
        return Err(OxenError::basic_str(format!("Node {} not found", hash)));
    };
    node.list_missing_file_hashes(repo)
}

pub fn child_hashes(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Vec<MerkleHash>, OxenError> {
    let Some(node) = CommitMerkleTree::read_node(repo, hash, false)? else {
        return Err(OxenError::basic_str(format!("Node {} not found", hash)));
    };
    let mut children = vec![];
    for child in node.children {
        children.push(child.hash);
    }
    Ok(children)
}
