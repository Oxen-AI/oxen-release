//! This is a compact representation of a merkle tree file node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};

// use super::file_node_types::{FileChunkType, FileStorageType};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct FileNode {
    // The name of the file
    pub name: String,

    // These are nice metadata to have (should we also have on other nodes?)
    pub num_bytes: u64,
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,

    // File chunks
    pub chunk_hashes: Vec<u128>,
    // TODO: We should look at the stat for other data to have here. Such as file permissions, etc.
    // https://man7.org/linux/man-pages/man1/stat.1.html

    // FUTURE IDEAS:
    // The data is always going to be chunked and stored locally
    // On the server it might be unpacked into a full file, duckdb database, or s3
    // On the server we want to ability to update how the file is stored in order
    // to query or save storage, etc

    // pub chunk_type: FileChunkType, // How the data is stored on disk
    // pub storage_backend: FileStorageType, // Where the file is stored in the backend
}
