//! # oxen kvdb-inspect
//!
//! Print out values from a rocksdb key value database
//!

use crate::error::OxenError;

use bytevec::ByteDecodable;
use rocksdb::{IteratorMode, LogLevel, Options, DB};
use std::path::Path;
use std::str;

/// List the key -> value pairs in a database
pub fn list(path: impl AsRef<Path>) -> Result<Vec<(String, String)>, OxenError> {
    let path = path.as_ref();
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);

    let mut result: Vec<(String, String)> = Vec::new();

    let db = DB::open_for_read_only(&opts, dunce::simplified(path), false)?;
    let iter = db.iterator(IteratorMode::Start);
    for item in iter {
        match item {
            Ok((key, value)) => {
                // try to decode u32 first (hacky but only two types we inspect right now)
                if let (Ok(key), Ok(value)) = (str::from_utf8(&key), u32::decode::<u8>(&value)) {
                    result.push((key.to_string(), value.to_string()));
                } else if let (Ok(key), Ok(value)) = (str::from_utf8(&key), str::from_utf8(&value))
                {
                    result.push((key.to_string(), value.to_string()));
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }

    Ok(result)
}

// Get a value from a database
pub fn get(path: impl AsRef<Path>, key: String) -> Result<String, OxenError> {
    let path = path.as_ref();
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);

    let db = DB::open_for_read_only(&opts, dunce::simplified(path), false)?;
    if let Some(value) = db.get(&key)? {
        Ok(String::from_utf8(value)?)
    } else {
        Err(OxenError::basic_str(format!("Key {} not found", key)))
    }
}
