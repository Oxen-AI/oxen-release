//! This is the deserialized representation of a merkle tree node
//! that is read from disk and translated to it's type in memory
//!

use serde::{Deserialize, Serialize};

use super::dir_node::DirNode;
use super::file_chunk_node::FileChunkNode;
use super::file_node::FileNode;
use super::schema_node::SchemaNode;
use super::vnode::VNode;
use super::MerkleTreeNodeType;
use crate::error::OxenError;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct DeserializedMerkleTreeNode {
    pub dtype: MerkleTreeNodeType,
    pub hash: u128,
    pub data: Vec<u8>,
}

impl DeserializedMerkleTreeNode {
    pub fn vnode(&self) -> Result<VNode, OxenError> {
        rmp_serde::from_slice(&self.data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing vnode: {e}")))
    }

    pub fn dir(&self) -> Result<DirNode, OxenError> {
        rmp_serde::from_slice(&self.data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing dir node: {e}")))
    }

    pub fn file(&self) -> Result<FileNode, OxenError> {
        rmp_serde::from_slice(&self.data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing file node: {e}")))
    }

    pub fn file_chunk(&self) -> Result<FileChunkNode, OxenError> {
        rmp_serde::from_slice(&self.data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing file chunk node: {e}")))
    }

    pub fn schema(&self) -> Result<SchemaNode, OxenError> {
        rmp_serde::from_slice(&self.data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing schema node: {e}")))
    }
}
