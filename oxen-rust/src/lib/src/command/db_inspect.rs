//! # oxen kvdb-inspect
//!
//! Print out values from a rocksdb key value database
//!

use crate::error::OxenError;

use bytevec::ByteDecodable;
use rocksdb::{IteratorMode, LogLevel, Options, DB};
use std::path::Path;
use std::str;

/// Inspect a key value database for debugging
pub fn inspect(path: &Path) -> Result<(), OxenError> {
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);
    let db = DB::open_for_read_only(&opts, dunce::simplified(path), false)?;
    let iter = db.iterator(IteratorMode::Start);
    for item in iter {
        match item {
            Ok((key, value)) => {
                // try to decode u32 first (hacky but only two types we inspect right now)
                if let (Ok(key), Ok(value)) = (str::from_utf8(&key), u32::decode::<u8>(&value)) {
                    println!("{key}\t{value}")
                } else if let (Ok(key), Ok(value)) = (str::from_utf8(&key), str::from_utf8(&value))
                {
                    println!("{key}\t{value}")
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(())
}
