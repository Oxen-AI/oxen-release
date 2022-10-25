use crate::constants;
use crate::error::OxenError;
use crate::model::{ContentHashable, NewCommit};

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;
use polars::prelude::DataFrame;
use xxhash_rust::xxh3::xxh3_128;

pub fn hash_buffer(buffer: &[u8]) -> String {
    let val = xxh3_128(buffer);
    format!("{val:x}")
}

pub fn hash_buffer_128bit(buffer: &[u8]) -> u128 {
    xxh3_128(buffer)
}

pub fn compute_tabular_hash(df: &DataFrame) -> String {
    let mut commit_hasher = xxhash_rust::xxh3::Xxh3::new();
    log::debug!("Combining row hashes for {}", df);
    let _results: Vec<Result<(), OxenError>> = df.column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
        .unwrap()
        .into_iter()
        .map(|hash| {
            commit_hasher.update(hash.unwrap().as_bytes());
            Ok(())
        })
        .collect();

    let val = commit_hasher.digest();
    format!("{val:x}")
}

pub fn compute_commit_hash(commit_data: &NewCommit, entries: &[impl ContentHashable]) -> String {
    let mut commit_hasher = xxhash_rust::xxh3::Xxh3::new();
    log::debug!("Hashing {} entries", entries.len());
    for entry in entries.iter() {
        let hash = entry.content_hash();
        let input = hash.as_bytes();
        commit_hasher.update(input);
    }

    log::debug!("Hashing commit data {:?}", commit_data);
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
