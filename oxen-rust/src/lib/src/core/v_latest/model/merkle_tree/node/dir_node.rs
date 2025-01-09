//! This is a compact representation of a directory merkle tree node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::model::{MerkleHash, MerkleTreeNodeType};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct DirNodeData {
    // The type of the node
    pub node_type: MerkleTreeNodeType,

    // The name of the directory
    pub name: String,

    // Hash of all the children
    pub hash: MerkleHash,
    // Recursive size of the directory
    pub num_bytes: u64,
    // Last commit id that modified the file
    pub last_commit_id: MerkleHash,
    // Last modified timestamp
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
    // Recursive file counts in the directory
    pub data_type_counts: HashMap<String, u64>,
    pub data_type_sizes: HashMap<String, u64>,
}
