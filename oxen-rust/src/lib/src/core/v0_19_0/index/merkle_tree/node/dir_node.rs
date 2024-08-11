//! This is a compact representation of a directory merkle tree node
//! that is stored in on disk
//!

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::view::DataTypeCount;

use super::{MerkleTreeNode, MerkleTreeNodeType};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct DirNode {
    // The type of the node
    pub dtype: MerkleTreeNodeType,

    // The name of the directory
    pub name: String,

    // Hash of all the children
    pub hash: u128,
    // Number of bytes in the file
    pub num_bytes: u64,
    // Last commit id that modified the file
    pub last_commit_id: u128,
    // Last modified timestamp
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
    // Total number of files in the directory
    pub data_type_counts: HashMap<String, usize>,
}

impl DirNode {
    pub fn num_files(&self) -> usize {
        // sum up the data type counts
        self.data_type_counts.values().sum()
    }

    pub fn data_types(&self) -> Vec<DataTypeCount> {
        self.data_type_counts
            .iter()
            .map(|(k, v)| DataTypeCount {
                data_type: k.clone(),
                count: *v,
            })
            .collect()
    }
}

impl Default for DirNode {
    fn default() -> Self {
        DirNode {
            dtype: MerkleTreeNodeType::Dir,
            name: "".to_string(),
            hash: 0,
            num_bytes: 0,
            last_commit_id: 0,
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type_counts: HashMap::new(),
        }
    }
}

impl MerkleTreeNode for DirNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn id(&self) -> u128 {
        self.hash
    }
}
