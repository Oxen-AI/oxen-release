//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};
use std::fmt;

use super::{MerkleTreeNode, MerkleTreeNodeIdType, MerkleTreeNodeType};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
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

impl MerkleTreeNodeIdType for VNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn id(&self) -> u128 {
        self.id
    }
}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for VNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VNode({:x})", self.id)
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for VNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // id and dtype already get printed by the node.rs println!("{:?}", node)
        write!(f, "")
    }
}

impl MerkleTreeNode for VNode {}
