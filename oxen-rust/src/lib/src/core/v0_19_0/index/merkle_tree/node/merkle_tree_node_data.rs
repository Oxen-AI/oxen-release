use rmp_serde::Serializer;
use serde::Serialize;
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};

use super::*;
use crate::core::db::merkle::merkle_node_db;
use crate::error::OxenError;
use crate::model::LocalRepository;

#[derive(Debug, Clone, Eq)]
pub struct MerkleTreeNodeData {
    pub hash: u128,
    pub dtype: MerkleTreeNodeType,
    pub data: Vec<u8>,
    pub children: HashSet<MerkleTreeNodeData>,
}

impl fmt::Display for MerkleTreeNodeData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} [{:?}] ({})",
            self.hash,
            self.dtype,
            self.children.len()
        )
    }
}

impl MerkleTreeNodeData {
    /// Create an empty root node with a hash
    pub fn root_commit(repo: &LocalRepository, hash: u128) -> Result<Self, OxenError> {
        let node_db = merkle_node_db::open_read_only(repo, hash)?;;
        Ok(MerkleTreeNodeData {
            hash,
            dtype: MerkleTreeNodeType::Commit,
            data: node_db.data(),
            children: HashSet::new(),
        })
    }

    /// Constant time lookup by hash
    pub fn get_by_hash(&self, hash: u128) -> Option<&MerkleTreeNodeData> {
        let lookup_node = MerkleTreeNodeData {
            hash,
            dtype: MerkleTreeNodeType::File, // Dummy value
            data: Vec::new(),                // Dummy value
            children: HashSet::new(),        // Dummy value
        };
        self.children.get(&lookup_node)
    }

    /// Check if the node is a leaf node (i.e. it has no children)
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    pub fn commit(&self) -> Result<CommitNode, OxenError> {
        rmp_serde::from_slice(&self.data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing commit: {e}")))
    }

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

impl PartialEq for MerkleTreeNodeData {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Hash for MerkleTreeNodeData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}
