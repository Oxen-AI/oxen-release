//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

use super::{MerkleTreeNode, MerkleTreeNodeType};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VNode {
    pub id: u128,
    pub dtype: MerkleTreeNodeType,
}

impl Default for VNode {
    fn default() -> Self {
        VNode {
            dtype: MerkleTreeNodeType::VNode,
            id: 0,
        }
    }
}

impl MerkleTreeNode for VNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn id(&self) -> u128 {
        self.id
    }
}

impl Display for VNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VNode({:x})", self.id)
    }
}
