use serde::Serialize;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;

use super::merkle_hash::MerkleHash;
use super::merkle_tree_node_type::MerkleTreeNodeType;
use crate::core::v0_19_0::index::merkle_tree::node::{
    CommitNode, DirNode, FileChunkNode, FileNode, SchemaNode, VNode,
};

pub trait MerkleTreeNodeIdType {
    fn dtype(&self) -> MerkleTreeNodeType;
    fn hash(&self) -> MerkleHash;
    fn children(&self) -> Vec<MerkleTreeNode>;
}

pub trait TMerkleTreeNode: MerkleTreeNodeIdType + Serialize + Debug + Display {}

pub enum MerkleTreeNode {
    Commit(CommitNode),
    Directory(DirNode),
    File(FileNode),
    VNode(VNode),
    FileChunk(FileChunkNode),
    Schema(SchemaNode),
}

impl MerkleTreeNode {
    pub fn can_have_children(&self) -> bool {
        match self {
            MerkleTreeNode::Commit(_) => true,
            MerkleTreeNode::Directory(_) => true,
            MerkleTreeNode::File(_) => false,
            MerkleTreeNode::VNode(_) => true,
            MerkleTreeNode::FileChunk(_) => false,
            MerkleTreeNode::Schema(_) => false,
        }
    }
}

impl fmt::Display for MerkleTreeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MerkleTreeNode::Commit(node) => write!(f, "{}", node),
            MerkleTreeNode::Directory(node) => write!(f, "{}", node),
            MerkleTreeNode::File(node) => write!(f, "{}", node),
            MerkleTreeNode::VNode(node) => write!(f, "{}", node),
            MerkleTreeNode::FileChunk(node) => write!(f, "{}", node),
            MerkleTreeNode::Schema(node) => write!(f, "{}", node),
        }
    }
}

impl fmt::Debug for MerkleTreeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MerkleTreeNode::Commit(node) => f.debug_tuple("Commit").field(node).finish(),
            MerkleTreeNode::Directory(node) => f.debug_tuple("Directory").field(node).finish(),
            MerkleTreeNode::File(node) => f.debug_tuple("File").field(node).finish(),
            MerkleTreeNode::VNode(node) => f.debug_tuple("VNode").field(node).finish(),
            MerkleTreeNode::FileChunk(node) => f.debug_tuple("FileChunk").field(node).finish(),
            MerkleTreeNode::Schema(node) => f.debug_tuple("Schema").field(node).finish(),
        }
    }
}
