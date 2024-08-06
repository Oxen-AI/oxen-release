//! This is a compact representation of a merkle tree file node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};

use super::file_node_types::{FileChunkType, FileStorageType};
use crate::model::EntryDataType;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct FileNode {
    // The name of the file
    pub name: String,

    // Full file hash
    pub hash: u128,
    // Number of bytes in the file
    pub num_bytes: u64,
    // Last commit id that modified the file
    pub last_commit_id: u128,
    // Last modified timestamp
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,

    // Data Type
    pub data_type: EntryDataType,
    // Mime Type
    pub mime_type: String,
    // Extension
    pub extension: String,

    // File chunks, for single chunk files, this will be empty (and we can just use the hash)
    pub chunk_hashes: Vec<u128>,

    pub chunk_type: FileChunkType, // How the data is stored on disk
    pub storage_backend: FileStorageType, // Where the file is stored in the backend
}
