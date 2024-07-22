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
    pub chunk_hashes: Vec<(u128, u32)>,
    // TODO: We should look at the stat for other data to have here. Such as file permissions, etc.
    // https://man7.org/linux/man-pages/man1/stat.1.html

    // FUTURE IDEAS:

    // CONFIGURE THE REPO FOR BLOB STORAGE (S3, etc) or LOCAL CHUNKS
    // * In order to support FineWeb scale data, we should just have the raw files
    //   offloaded to S3, then be able to piped back through the server

    // Another cool thing about chunks...is we can push them without saving them locally
    // We should always unpack to chunk dir instead of the tmp file as well

    /*

    Rocksdb is too slow with a single db after inserting over 100k nodes.

    Our goal is to have a quick way to look up where a u128 hash is stored
    in the chunks db.

    Maybe store the chunks in sharded files 16MB that they can quickly look up into.

    If we maintain one file that is a map from u128 hash to the chunk file and offset
    we can very quickly look up the chunk file and offset for a given hash.

    Say we had 1TB of unique data, we would have 6,2500,000 hashes which would be
    6,2500,000 * 16 bytes = 1GB of hashes in memory
    or if we have keys and values more like 2GB in memory. The values would be the files they live in.

    HashMap<u128, u32> where key is the hash and the value is file it lives in.
    TODO: Test if RocksDB can handle 6,250,000 entries of 16 bytes keys to 4 bytes values.

    Then we can iterate over the chunks, check if the current shard is too large,
    if it is, we close it and generate a new shard. The next unique chunks get
    written there.

    So now we have one index file at the top. That knows the mapping from hash -> shard.

    Then individual shard files that know the mapping from hash -> offset in the file.
      Should these have index and data files or just all be in one struct? I like the idea of separate.
    These individual shard files can be 16MB and live in memory until we flush them.

    */
    // The data is always going to be chunked and stored locally
    // On the server it might be unpacked into a full file, duckdb database, or s3
    // On the server we want to ability to update how the file is stored in order
    // to query or save storage, etc

    // pub chunk_type: FileChunkType, // How the data is stored on disk
    // pub storage_backend: FileStorageType, // Where the file is stored in the backend
}
