use crate::error::OxenError;
use serde::{de, Serialize};

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;
use std::hash::Hash;

/// # Checks if the file exists in this directory
/// More efficient than get_entry since it does not actual deserialize the entry
pub fn has_entry<P: AsRef<Path>>(db: &DBWithThreadMode<MultiThreaded>, path: P) -> bool {
    let path = path.as_ref();
    if let Some(path_str) = path.to_str() {
        let bytes = path_str.as_bytes();
        match db.get(bytes) {
            Ok(Some(_value)) => true,
            Ok(None) => false,
            Err(err) => {
                log::error!("Error checking for entry: {}", err);
                false
            }
        }
    } else {
        false
    }
}

/// # Get the staged entry object from the file path
pub fn get_entry<P: AsRef<Path>, T>(
    db: &DBWithThreadMode<MultiThreaded>,
    path: P,
) -> Result<Option<T>, OxenError>
where
    T: de::DeserializeOwned,
{
    let path = path.as_ref();
    log::debug!("path_db::get_entry({:?}) from db {:?}", path, db.path());
    if let Some(path_str) = path.to_str() {
        let bytes = path_str.as_bytes();
        match db.get(bytes) {
            Ok(Some(value)) => {
                // found it
                let str_val = str::from_utf8(&*value)?;
                let entry = serde_json::from_str(str_val)?;
                log::debug!("path_db::get_entry({:?}) GOT IT from db {:?}", path, db.path());
                Ok(Some(entry))
            }
            Ok(None) => {
                // did not get val
                log::debug!("path_db::get_entry({:?}) don't got it....  from db {:?}", path, db.path());
                Ok(None)
            }
            Err(err) => {
                // error from the DB
                let err = format!("Err could not fetch value from db: {} from db {:?}", err, db.path());
                Err(OxenError::basic_str(err))
            }
        }
    } else {
        Err(OxenError::basic_str(
            "Err: get_entry could not convert path to str",
        ))
    }
}

/// # Serializes the entry to json and writes to db
pub fn add_to_db<P: AsRef<Path>, T>(
    db: &DBWithThreadMode<MultiThreaded>,
    path: P,
    entry: &T,
) -> Result<(), OxenError>
where
    T: Serialize,
{
    let path = path.as_ref();
    let key = path.to_str().unwrap();
    let entry_json = serde_json::to_string(entry)?;

    log::debug!("add_to_db {:?} -> {:?} -> db: {:?}", path, entry_json, db.path());

    db.put(&key, entry_json.as_bytes())?;
    Ok(())
}

/// # List the file paths in the staged dir
/// More efficient than list_added_path_entries since it does not deserialize the entries
pub fn list_paths(
    db: &DBWithThreadMode<MultiThreaded>,
    base_dir: &Path,
) -> Result<Vec<PathBuf>, OxenError> {
    log::debug!("path_db::list_paths({:?})", base_dir);
    let iter = db.iterator(IteratorMode::Start);
    let mut paths: Vec<PathBuf> = vec![];
    for (key, _value) in iter {
        match str::from_utf8(&*key) {
            Ok(key) => {
                // return full path
                paths.push(base_dir.join(String::from(key)));
            }
            _ => {
                log::error!("list_added_paths() Could not decode key {:?}", key)
            }
        }
    }
    Ok(paths)
}

/// # List file names and attached entries
pub fn list_path_entries<T>(
    db: &DBWithThreadMode<MultiThreaded>,
    base_dir: &Path,
) -> Result<Vec<(PathBuf, T)>, OxenError>
where
    T: de::DeserializeOwned,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut paths: Vec<(PathBuf, T)> = vec![];
    for (key, value) in iter {
        match (str::from_utf8(&*key), str::from_utf8(&*value)) {
            (Ok(key), Ok(value)) => {
                // Full path given the dir it is in
                let path = base_dir.join(String::from(key));
                let entry: Result<T, serde_json::error::Error> = serde_json::from_str(value);
                if let Ok(entry) = entry {
                    paths.push((path, entry));
                }
            }
            (Ok(key), _) => {
                log::error!(
                    "list_added_path_entries() Could not values for key {}.",
                    key
                )
            }
            (_, Ok(val)) => {
                log::error!("list_added_path_entries() Could not key for value {}.", val)
            }
            _ => {
                log::error!("list_added_path_entries() Could not decoded keys and values.")
            }
        }
    }
    Ok(paths)
}

/// # List entries without file names
pub fn list_entries<T>(
    db: &DBWithThreadMode<MultiThreaded>,
    base_dir: &Path,
) -> Result<Vec<T>, OxenError>
where
    T: de::DeserializeOwned,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut paths: Vec<T> = vec![];
    for (key, value) in iter {
        match str::from_utf8(&*value) {
            Ok(value) => {
                // Full path given the dir it is in
                let entry: Result<T, serde_json::error::Error> = serde_json::from_str(value);
                if let Ok(entry) = entry {
                    paths.push(entry);
                }
            }
            _ => {
                log::error!("list_added_path_entries() Could not decoded keys and values.")
            }
        }
    }
    Ok(paths)
}

pub fn list_entries_set<T>(
    db: &DBWithThreadMode<MultiThreaded>,
    base_dir: &Path,
) -> Result<HashSet<T>, OxenError>
where
    T: de::DeserializeOwned,
    T: Hash,
    T: Eq
{
    let iter = db.iterator(IteratorMode::Start);
    let mut paths: HashSet<T> = HashSet::new();
    for (key, value) in iter {
        match str::from_utf8(&*value) {
            Ok(value) => {
                // Full path given the dir it is in
                let entry: Result<T, serde_json::error::Error> = serde_json::from_str(value);
                if let Ok(entry) = entry {
                    paths.insert(entry);
                }
            }
            _ => {
                log::error!("list_added_path_entries() Could not decoded keys and values.")
            }
        }
    }
    Ok(paths)
}

pub fn clear(db: &DBWithThreadMode<MultiThreaded>) -> Result<(), OxenError> {
    let iter = db.iterator(IteratorMode::Start);
    for (key, _) in iter {
        db.delete(key)?;
    }
    Ok(())
}
