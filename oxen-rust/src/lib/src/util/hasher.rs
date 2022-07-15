use crate::error::OxenError;
use crate::model::{NewCommit, StagedEntry};

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use xxhash_rust::xxh3::xxh3_128;

pub fn hash_buffer(buffer: &[u8]) -> String {
    let val = xxh3_128(buffer);
    format!("{val:x}")
}

pub fn hash_buffer_128bit(buffer: &[u8]) -> u128 {
    xxh3_128(buffer)
}

pub fn compute_commit_hash(commit_data: &NewCommit, entries: &[(PathBuf, StagedEntry)]) -> String {
    let mut commit_hasher = xxhash_rust::xxh3::Xxh3::new();

    for entry in entries.iter() {
        let input = entry.1.hash.as_bytes();
        commit_hasher.update(input);
    }

    let commit_str = format!("{:?}", commit_data);
    commit_hasher.update(commit_str.as_bytes());

    let val = commit_hasher.digest();
    format!("{val:x}")
}

pub fn hash_file_contents(path: &Path) -> Result<String, OxenError> {
    match File::open(path) {
        Ok(file) => {
            let mut reader = BufReader::new(file);
            let mut buffer = Vec::new();
            match reader.read_to_end(&mut buffer) {
                Ok(_) => {
                    let result = hash_buffer(&buffer);
                    Ok(result)
                }
                Err(_) => {
                    eprintln!("Could not read file to end {:?}", path);
                    Err(OxenError::basic_str("Could not read file to end"))
                }
            }
        }
        Err(_) => {
            let err = format!(
                "util::hasher::hash_file_contents Could not open file {:?}",
                path
            );
            Err(OxenError::basic_str(&err))
        }
    }
}

pub fn hash_file_contents_128bit(path: &Path) -> Result<u128, OxenError> {
    match File::open(path) {
        Ok(file) => {
            let mut reader = BufReader::new(file);
            let mut buffer = Vec::new();
            match reader.read_to_end(&mut buffer) {
                Ok(_) => {
                    let result = hash_buffer_128bit(&buffer);
                    Ok(result)
                }
                Err(_) => {
                    eprintln!("Could not read file to end {:?}", path);
                    Err(OxenError::basic_str("Could not read file to end"))
                }
            }
        }
        Err(_) => {
            let err = format!(
                "util::hasher::hash_file_contents Could not open file {:?}",
                path
            );
            Err(OxenError::basic_str(&err))
        }
    }
}
