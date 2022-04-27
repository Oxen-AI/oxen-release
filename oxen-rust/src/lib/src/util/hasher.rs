use crate::error::OxenError;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use xxhash_rust::xxh3::xxh3_128;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;

pub fn hash_buffer(buffer: &[u8]) -> String {
    let mut hasher = DefaultHasher::new();
    let val = xxh3_128(buffer);
    val.hash(&mut hasher);
    format!("{:X}", hasher.finish())
}

pub fn hash_file_contents(path: &Path) -> Result<String, OxenError> {
    match File::open(path) {
        Ok(file) => {
            let mut reader = BufReader::new(file);
            let mut buffer = Vec::new();
            match reader.read_to_end(&mut buffer) {
                Ok(_) => {
                    // read hash digest and consume hasher
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
            let err = format!("util::hasher::hash_file_contents Could not open file {:?}", path);
            Err(OxenError::basic_str(&err))
        }
    }
}
