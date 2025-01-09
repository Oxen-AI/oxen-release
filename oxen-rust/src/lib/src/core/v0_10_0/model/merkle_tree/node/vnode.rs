//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use crate::model::{MerkleHash, MerkleTreeNodeType};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VNodeData {
    pub hash: MerkleHash,
    pub node_type: MerkleTreeNodeType,
    pub random_field: String,
    pub random_field_2: u8,
    pub random_field_3: i128,
}
