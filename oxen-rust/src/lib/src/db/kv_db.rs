use crate::error::OxenError;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::str;

/// More efficient than get since it does not actual deserialize the value
pub fn has_key<S: AsRef<str>>(db: &DBWithThreadMode<MultiThreaded>, key: S) -> bool {
    let key = key.as_ref();
    let bytes = key.as_bytes();
    match db.get(bytes) {
        Ok(Some(_value)) => true,
        Ok(None) => false,
        Err(err) => {
            log::error!("Error checking for entry: {}", err);
            false
        }
    }
}

/// Remove key from database
pub fn delete<S: AsRef<str>>(
    db: &DBWithThreadMode<MultiThreaded>,
    key: S,
) -> Result<(), OxenError> {
    let key = key.as_ref();
    log::debug!("kv_db::delete {:?} from db: {:?}", key, db.path());

    db.delete(&key)?;
    Ok(())
}

/// More efficient than `list` since it does not deserialize the values
pub fn list_keys(db: &DBWithThreadMode<MultiThreaded>) -> Result<Vec<String>, OxenError> {
    let iter = db.iterator(IteratorMode::Start);
    let mut keys: Vec<String> = vec![];
    for (key, _value) in iter {
        match str::from_utf8(&*key) {
            Ok(key) => {
                // return full path
                keys.push(String::from(key));
            }
            _ => {
                log::error!("list_keys() Could not decode key {:?}", key)
            }
        }
    }
    Ok(keys)
}

/// Remove all values from the db
pub fn clear(db: &DBWithThreadMode<MultiThreaded>) -> Result<(), OxenError> {
    let iter = db.iterator(IteratorMode::Start);
    for (key, _) in iter {
        db.delete(key)?;
    }
    Ok(())
}
