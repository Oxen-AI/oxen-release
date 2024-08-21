use serde::Serialize;
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
