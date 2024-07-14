//! Chunks files in order to deduplicate chunks across large files that are changed
//!
//! The idea here is that we can split the file into chunks and hash the chunks
//! These chunks are stored at the bottom of the merkle tree
//!
//! It saves us:
//! * Storage across commits
//! * Time to upload changes
//!
//! Need to balance this with:
//! * Time to reconstruct the file
//! * Time to query the file
//!

use std::fs::File;
use std::io::Read;
use std::io::Write;

use crate::error::OxenError;
use crate::model::CommitEntry;
use crate::model::LocalRepository;
use crate::util;
use crate::util::hasher;

// static chunk size of 16kb
pub const CHUNK_SIZE: usize = 16 * 1024;

pub struct FileChunker {
    repo: LocalRepository,
}

impl FileChunker {
    pub fn new(repo: &LocalRepository) -> Self {
        Self { repo: repo.clone() }
    }

    pub fn save_chunks(&self, entry: &CommitEntry) -> Result<Vec<u128>, OxenError> {
        let version_file = util::fs::version_path(&self.repo, entry);
        let mut read_file = File::open(version_file)?;

        // Read/Write chunks
        let mut buffer = vec![0; CHUNK_SIZE]; // 16KB buffer
        let mut hashes: Vec<u128> = Vec::new();
        while let Ok(bytes_read) = read_file.read(&mut buffer) {
            if bytes_read == 0 {
                break; // End of file
            }
            // Shrink buffer to size of bytes read
            buffer.truncate(bytes_read);

            // Process the buffer here
            // println!("Read {} bytes from {:?}", bytes_read, version_file);
            let hash = hasher::hash_buffer_128bit(&buffer);
            let shash = format!("{:x}", hash);

            // Write the chunk to disk if it doesn't exist
            let output_path = util::fs::chunk_path(&self.repo, &shash);
            if let Some(parent) = output_path.parent() {
                if !parent.exists() {
                    util::fs::create_dir_all(parent)?;
                }
            }

            if !output_path.exists() {
                let mut output_file = File::create(output_path)?;
                let bytes_written = output_file.write(&buffer)?;
                if bytes_written != bytes_read {
                    return Err(OxenError::basic_str("Failed to write all bytes to chunk"));
                }
            }

            hashes.push(hash);
        }

        Ok(hashes)
    }
}
