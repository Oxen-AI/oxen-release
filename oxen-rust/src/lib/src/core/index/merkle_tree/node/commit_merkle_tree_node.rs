use rmp_serde::Serializer;
use serde::Serialize;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use super::*;
use crate::error::OxenError;

#[derive(Debug, Clone, Eq)]
pub struct CommitMerkleTreeNode {
    pub hash: String,
    pub dtype: MerkleTreeNodeType,
    pub data: Vec<u8>,
    pub children: HashSet<CommitMerkleTreeNode>,
}

impl CommitMerkleTreeNode {
    /// Create an empty root node with a hash
    pub fn root(hash: &str) -> Self {
        let dir_node = DirNode {
            path: "".to_string(),
        };
        let mut buf = Vec::new();
        dir_node.serialize(&mut Serializer::new(&mut buf)).unwrap();
        CommitMerkleTreeNode {
            hash: hash.to_string(),
            dtype: MerkleTreeNodeType::Dir,
            data: buf,
            children: HashSet::new(),
        }
    }

    /// Constant time lookup by hash
    pub fn get_by_hash(&self, hash: &str) -> Option<&CommitMerkleTreeNode> {
        let lookup_node = CommitMerkleTreeNode {
            hash: hash.to_string(),
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

impl PartialEq for CommitMerkleTreeNode {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Hash for CommitMerkleTreeNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}
