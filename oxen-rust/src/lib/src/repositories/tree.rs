use crate::core::v0_19_0::index::merkle_tree::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash, MerkleTreeNode};

pub fn get_node_by_id(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let Some(node) = CommitMerkleTree::read_node(repo, hash, false)? else {
        return Ok(None);
    };
    Ok(Some(node.to_node()?))
}
