//! This is a compact representation of a directory merkle tree node
//! that is stored in on disk
//!

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct DirNode {
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
}
