//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{
    LocalRepository, MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode,
};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum VNode {
    V0_19_0(VNodeImplV0_19_0),
    VLATEST(VNodeImplV0_25_0),
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VNodeImplV0_19_0 {
    pub hash: MerkleHash,
    pub node_type: MerkleTreeNodeType,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VNodeImplV0_25_0 {
    pub hash: MerkleHash,
    pub node_type: MerkleTreeNodeType,
    pub num_entries: u64,
}

impl VNode {
    pub fn new(
        repo: &LocalRepository,
        hash: MerkleHash,
        num_entries: u64,
    ) -> Result<VNode, OxenError> {
        match repo.min_version() {
            MinOxenVersion::V0_10_0 => Err(OxenError::basic_str("VNode not supported in v0.10.0")),
            MinOxenVersion::V0_19_0 => Ok(VNode::V0_19_0(VNodeImplV0_19_0 {
                hash,
                node_type: MerkleTreeNodeType::VNode,
            })),
            MinOxenVersion::LATEST => Ok(VNode::VLATEST(VNodeImplV0_25_0 {
                hash,
                node_type: MerkleTreeNodeType::VNode,
                num_entries,
            })),
        }
    }

    pub fn deserialize(repo: &LocalRepository, data: &[u8]) -> Result<VNode, OxenError> {
        match repo.min_version() {
            MinOxenVersion::V0_19_0 => {
                let vnode: VNodeImplV0_19_0 = rmp_serde::from_slice(data).map_err(|e| {
                    OxenError::basic_str(format!("Error deserializing vnode v0.19.0: {e}"))
                })?;
                Ok(VNode::V0_19_0(vnode))
            }
            MinOxenVersion::LATEST => {
                let vnode: VNodeImplV0_25_0 = rmp_serde::from_slice(data).map_err(|e| {
                    OxenError::basic_str(format!("Error deserializing vnode v0.25.0: {e}"))
                })?;
                Ok(VNode::VLATEST(vnode))
            }
            _ => Err(OxenError::basic_str("Unsupported version")),
        }
    }

    pub fn hash(&self) -> MerkleHash {
        match self {
            VNode::V0_19_0(vnode) => vnode.hash,
            VNode::VLATEST(vnode) => vnode.hash,
        }
    }

    pub fn num_entries(&self) -> u64 {
        match self {
            VNode::V0_19_0(_) => 0,
            VNode::VLATEST(vnode) => vnode.num_entries,
        }
    }
}

impl Default for VNode {
    fn default() -> Self {
        VNode::VLATEST(VNodeImplV0_25_0 {
            node_type: MerkleTreeNodeType::VNode,
            hash: MerkleHash::new(0),
            num_entries: 0,
        })
    }
}

impl MerkleTreeNodeIdType for VNode {
    fn node_type(&self) -> MerkleTreeNodeType {
        match self {
            VNode::V0_19_0(vnode) => vnode.node_type,
            VNode::VLATEST(vnode) => vnode.node_type,
        }
    }

    fn hash(&self) -> MerkleHash {
        self.hash()
    }
}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for VNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VNode({})", self.hash())
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
