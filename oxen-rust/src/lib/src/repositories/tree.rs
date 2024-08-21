use crate::core::v0_19_0::index::merkle_tree::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash, MerkleTreeNode};

pub fn get_node_by_id(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<MerkleTreeNode, OxenError> {
    let node = CommitMerkleTree::read_node(repo, hash, false)?;
    Ok(node.to_node()?)
}
