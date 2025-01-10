//! This is a compact representation of a merkle tree file node
//! that is stored in on disk
//!

use crate::model::merkle_tree::node::file_node_types::{FileChunkType, FileStorageType};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{EntryDataType, MerkleHash, MerkleTreeNodeType};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct FileNodeData {
    pub node_type: MerkleTreeNodeType,

    // The name of the file
    pub name: String,

    // Full file hash
    pub metadata_hash: Option<MerkleHash>, // hash of the metadata
    pub hash: MerkleHash,
    pub combined_hash: MerkleHash, //hash of the content_hash and metadata_hash
    // Number of bytes in the file
    pub num_bytes: u64,
    // Last commit id that modified the file
    pub last_commit_id: MerkleHash,
    // Last modified timestamp
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,

    // Data Type
    pub data_type: EntryDataType,

    // Metadata
    pub metadata: Option<GenericMetadata>,

    // Mime Type
    pub mime_type: String,
    // Extension
    pub extension: String,

    // File chunks, for single chunk files, this will be empty (and we can just use the hash)
    pub chunk_hashes: Vec<u128>,

    pub chunk_type: FileChunkType, // How the data is stored on disk
    pub storage_backend: FileStorageType, // Where the file is stored in the backend
}
