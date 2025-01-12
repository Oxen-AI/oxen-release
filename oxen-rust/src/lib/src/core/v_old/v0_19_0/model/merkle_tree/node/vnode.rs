//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use crate::model::merkle_tree::node::vnode::TVNode;
use crate::model::{MerkleHash, MerkleTreeNodeType};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VNodeData {
    pub hash: MerkleHash,
    pub node_type: MerkleTreeNodeType,
}

impl TVNode for VNodeData {
    fn hash(&self) -> MerkleHash {
        self.hash
    }

    fn node_type(&self) -> MerkleTreeNodeType {
        self.node_type
    }

    fn num_entries(&self) -> u64 {
        panic!("VNodeData(0.19.0) does not have num_entries");
    }
}
