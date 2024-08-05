//! Determines how the file is stored on disk to cloud storage
//!
//! * Full (the full file is stored in a contiguous chunk)
//! * Chunks (the file is stored in a series of chunks)
//!

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FileChunkType {
    SingleFile,
    Chunked,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FileStorageType {
    Disk,
    S3,
}
