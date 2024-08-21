//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::model::{MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VNode {
    pub id: MerkleHash,
    pub dtype: MerkleTreeNodeType,
}

impl Default for VNode {
    fn default() -> Self {
        VNode {
            dtype: MerkleTreeNodeType::VNode,
            id: MerkleHash::new(0),
        }
    }
}

impl MerkleTreeNodeIdType for VNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn id(&self) -> MerkleHash {
        self.id
    }
}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for VNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VNode({})", self.id.to_string())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for VNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // id and dtype already get printed by the node.rs println!("{:?}", node)
        write!(f, "")
    }
}

impl TMerkleTreeNode for VNode {}
