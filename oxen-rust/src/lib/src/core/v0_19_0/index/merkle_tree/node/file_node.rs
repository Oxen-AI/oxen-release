//! This is a compact representation of a merkle tree file node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};

use super::{file_node_types::{FileChunkType, FileStorageType}, MerkleTreeNodeType, MerkleTreeNode};
use crate::model::EntryDataType;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct FileNode {
    pub dtype: MerkleTreeNodeType,

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

impl Default for FileNode {
    fn default() -> Self {
        FileNode {
            dtype: MerkleTreeNodeType::File,
            name: "".to_string(),
            hash: 0,
            num_bytes: 0,
            last_commit_id: 0,
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type: EntryDataType::Binary,
            mime_type: "".to_string(),
            extension: "".to_string(),
            chunk_hashes: vec![],
            chunk_type: FileChunkType::SingleFile,
            storage_backend: FileStorageType::Disk,
        }
    }
}

impl MerkleTreeNode for FileNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn id(&self) -> u128 {
        self.hash
    }
}