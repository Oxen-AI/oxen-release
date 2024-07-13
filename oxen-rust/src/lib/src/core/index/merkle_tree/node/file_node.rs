//! This is a compact representation of a merkle tree file node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct FileNode {
    // pub file_indices: Vec<u64>,
    // pub chunk_hashes: Vec<String>,
    pub path: String,
    // These are nice metadata to have (should we also have on other nodes?)
    pub num_bytes: u64,
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
    // TODO: We should look at the stat for other data to have here. Such as file permissions, etc.
    // https://man7.org/linux/man-pages/man1/stat.1.html
}
