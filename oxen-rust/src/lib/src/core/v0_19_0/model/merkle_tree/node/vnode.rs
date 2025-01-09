//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use crate::model::{MerkleHash, MerkleTreeNodeType};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VNodeData {
    pub hash: MerkleHash,
    pub node_type: MerkleTreeNodeType,
}
