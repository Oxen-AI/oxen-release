//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::core::v_latest::model::merkle_tree::node::vnode::VNodeData as VNodeImplV0_25_0;
use crate::core::v_old::v0_19_0::model::merkle_tree::node::vnode::VNodeData as VNodeImplV0_19_0;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{
    LocalRepository, MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode,
};

// TODO: ✅ Verify that we can add a new version and still deserialize old versions
// TODO: We should wrap all the old versions in an enum so we can extend them in the future
// TODO: v0.19.0 does not load now, so we need to route to the old merkle tree reader for backwards compatibility

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum VNode {
    V0_19_0(VNodeImplV0_19_0),
    V0_25_0(VNodeImplV0_25_0),
}

impl VNode {
    pub fn new(
        repo: &LocalRepository,
        hash: MerkleHash,
        num_entries: u64,
    ) -> Result<VNode, OxenError> {
        match repo.min_version() {
            MinOxenVersion::V0_19_0 => Ok(VNode::V0_19_0(VNodeImplV0_19_0 {
                hash,
                node_type: MerkleTreeNodeType::VNode,
            })),
            MinOxenVersion::LATEST => Ok(VNode::V0_25_0(VNodeImplV0_25_0 {
                hash,
                node_type: MerkleTreeNodeType::VNode,
                num_entries,
            })),
            _ => Err(OxenError::basic_str("VNode not supported in this version")),
        }
    }

    pub fn deserialize(data: &[u8]) -> Result<VNode, OxenError> {
        // In order to support versions that didn't have the enum,
        // if it fails we will fall back to the old struct, then populate the enum
        let vnode: VNode = match rmp_serde::from_slice(data) {
            Ok(vnode) => vnode,
            Err(_) => {
                // This is a fallback for old versions of the vnode
                let vnode: VNodeImplV0_19_0 = rmp_serde::from_slice(data)?;
                VNode::V0_19_0(vnode)
            }
        };
        Ok(vnode)
    }

    pub fn hash(&self) -> MerkleHash {
        match self {
            VNode::V0_19_0(vnode) => vnode.hash,
            VNode::V0_25_0(vnode) => vnode.hash,
        }
    }

    pub fn num_entries(&self) -> u64 {
        match self {
            VNode::V0_25_0(vnode) => vnode.num_entries,
            _ => panic!("{self:?} does not have num_entries"),
        }
    }
}

impl Default for VNode {
    fn default() -> Self {
        VNode::V0_25_0(VNodeImplV0_25_0 {
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
            VNode::V0_25_0(vnode) => vnode.node_type,
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
