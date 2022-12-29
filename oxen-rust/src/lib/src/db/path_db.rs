use crate::error::OxenError;
use serde::{de, Serialize};

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::collections::HashSet;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::str;

use crate::db::str_json_db;

/// # Checks if the file exists in this directory
/// More efficient than get_entry since it does not actual deserialize the entry
pub fn has_entry<P: AsRef<Path>>(db: &DBWithThreadMode<MultiThreaded>, path: P) -> bool {
    let path = path.as_ref();
    // log::debug!("path_db::has_entry?({:?}) from db {:?}", path, db.path());
    if let Some(key) = path.to_str() {
        return str_json_db::has_key(db, key);
    }

    false
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
    // log::debug!("path_db::get_entry({:?}) from db {:?}", path, db.path());
    if let Some(key) = path.to_str() {
        return str_json_db::get(db, key);
    }
    Err(OxenError::could_not_convert_path_to_str(path))
}

/// # Serializes the entry to json and writes to db
pub fn put<P: AsRef<Path>, T>(
    db: &DBWithThreadMode<MultiThreaded>,
    path: P,
    entry: &T,
) -> Result<(), OxenError>
where
    T: Serialize,
{
    let path = path.as_ref();
    if let Some(key) = path.to_str() {
        str_json_db::put(db, key, entry)
    } else {
        Err(OxenError::could_not_convert_path_to_str(path))
    }
}

/// # Removes path entry from database
pub fn delete<P: AsRef<Path>>(
    db: &DBWithThreadMode<MultiThreaded>,
    path: P,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    if let Some(key) = path.to_str() {
        str_json_db::delete(db, key)
    } else {
        Err(OxenError::could_not_convert_path_to_str(path))
    }
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
        match str::from_utf8(&key) {
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
        match (str::from_utf8(&key), str::from_utf8(&value)) {
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
pub fn list_entries<T>(db: &DBWithThreadMode<MultiThreaded>) -> Result<Vec<T>, OxenError>
where
    T: de::DeserializeOwned,
{
    str_json_db::list_vals(db)
}

pub fn list_entries_set<T>(db: &DBWithThreadMode<MultiThreaded>) -> Result<HashSet<T>, OxenError>
where
    T: de::DeserializeOwned,
    T: Hash,
    T: Eq,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut paths: HashSet<T> = HashSet::new();
    for (_key, value) in iter {
        match str::from_utf8(&value) {
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
    str_json_db::clear(db)
}

pub fn list_entry_page<T>(
    db: &DBWithThreadMode<MultiThreaded>,
    page_num: usize,
    page_size: usize,
) -> Result<Vec<T>, OxenError>
where
    T: de::DeserializeOwned,
{
    // The iterator doesn't technically have a skip method as far as I can tell
    // so we are just going to manually do it
    let mut paths: Vec<T> = vec![];
    let iter = db.iterator(IteratorMode::Start);
    // Do not go negative, and start from 0
    let start_page = if page_num == 0 { 0 } else { page_num - 1 };
    let start_idx = start_page * page_size;
    for (entry_i, (_key, value)) in iter.enumerate() {
        // limit to page_size
        if paths.len() >= page_size {
            break;
        }

        // only grab values after start_idx based on page_num and page_size
        if entry_i >= start_idx {
            let entry: T = serde_json::from_str(str::from_utf8(&value)?)?;
            paths.push(entry);
        }
    }
    Ok(paths)
}
