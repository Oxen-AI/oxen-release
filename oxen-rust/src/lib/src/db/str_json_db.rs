use crate::db::kv_db;
use crate::error::OxenError;
use serde::{de, Serialize};

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::str;

/// More efficient than get since it does not actual deserialize the entry
pub fn has_key<S: AsRef<str>>(db: &DBWithThreadMode<MultiThreaded>, key: S) -> bool {
    kv_db::has_key(db, key)
}

/// Remove all values from the db
pub fn clear(db: &DBWithThreadMode<MultiThreaded>) -> Result<(), OxenError> {
    kv_db::clear(db)
}

/// # Removes key from database
pub fn delete<S: AsRef<str>>(
    db: &DBWithThreadMode<MultiThreaded>,
    key: S,
) -> Result<(), OxenError> {
    kv_db::delete(db, key)
}

/// More efficient than `list` since it does not deserialize the values
pub fn list_keys(db: &DBWithThreadMode<MultiThreaded>) -> Result<Vec<String>, OxenError> {
    kv_db::list_keys(db)
}

/// # Get the value from the key
pub fn get<S: AsRef<str>, T>(
    db: &DBWithThreadMode<MultiThreaded>,
    key: S,
) -> Result<Option<T>, OxenError>
where
    T: de::DeserializeOwned,
{
    let key = key.as_ref();
    log::debug!("kv_json_db::get({:?}) from db {:?}", key, db.path());

    let bytes = key.as_bytes();
    match db.get(bytes) {
        Ok(Some(value)) => {
            // found it
            let str_val = str::from_utf8(&*value)?;
            let entry = serde_json::from_str(str_val)?;
            Ok(Some(entry))
        }
        Ok(None) => {
            // did not get val
            Ok(None)
        }
        Err(err) => {
            // error from the DB
            let err = format!(
                "Err could not fetch value {:?} from db: {} from db {:?}",
                key,
                err,
                db.path()
            );
            Err(OxenError::basic_str(err))
        }
    }
}

/// # Serializes the entry to json and writes to db
pub fn put<S: AsRef<str>, T>(
    db: &DBWithThreadMode<MultiThreaded>,
    key: S,
    entry: &T,
) -> Result<(), OxenError>
where
    T: Serialize,
{
    let key = key.as_ref();
    let json_val = serde_json::to_string(entry)?;

    log::debug!(
        "kv_json_db::put {:?} -> {:?} -> db: {:?}",
        key,
        json_val,
        db.path()
    );

    db.put(&key, json_val.as_bytes())?;
    Ok(())
}

/// List Values
pub fn list_vals<T>(db: &DBWithThreadMode<MultiThreaded>) -> Result<Vec<T>, OxenError>
where
    T: de::DeserializeOwned,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut values: Vec<T> = vec![];
    for (_key, value) in iter {
        match str::from_utf8(&*value) {
            Ok(value) => {
                // Full path given the dir it is in
                let entry: Result<T, serde_json::error::Error> = serde_json::from_str(value);
                if let Ok(entry) = entry {
                    values.push(entry);
                }
            }
            _ => {
                log::error!("list_added_path_entries() Could not decoded keys and values.")
            }
        }
    }
    Ok(values)
}

/// # List keys and attached values
pub fn list<T>(db: &DBWithThreadMode<MultiThreaded>) -> Result<Vec<(String, T)>, OxenError>
where
    T: de::DeserializeOwned,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut results: Vec<(String, T)> = vec![];
    for (key, value) in iter {
        match (str::from_utf8(&*key), str::from_utf8(&*value)) {
            (Ok(key), Ok(value)) => {
                let key = String::from(key);
                let entry: Result<T, serde_json::error::Error> = serde_json::from_str(value);
                if let Ok(entry) = entry {
                    results.push((key, entry));
                }
            }
            (Ok(key), _) => {
                log::error!("kv_json_db::list() Could not values for key {}.", key)
            }
            (_, Ok(val)) => {
                log::error!("kv_json_db::list() Could not key for value {}.", val)
            }
            _ => {
                log::error!("kv_json_db::list() Could not decoded keys and values.")
            }
        }
    }
    Ok(results)
}
