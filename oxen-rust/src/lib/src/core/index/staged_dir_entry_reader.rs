//! # StagedDirEntryReader
//!
//! Facade around the StagedDirEntryDB
//! Faster for lookups since it does not allow writing, hence no locking
//!

use crate::core::index::StagedDirEntryDB;
use crate::error::OxenError;
use crate::model::{LocalRepository, StagedEntry};

use rocksdb::MultiThreaded;
use std::path::{Path, PathBuf};

pub struct StagedDirEntryReader {
    // https://docs.rs/rocksdb/latest/rocksdb/type.DB.html
    // SingleThreaded does not have the RwLock overhead inside the DB
    // Even with SingleThreaded, almost all of RocksDB operations is
    // multi-threaded unless the underlying RocksDB
    // instance is specifically configured otherwise
    db: StagedDirEntryDB<MultiThreaded>,
}

impl StagedDirEntryReader {
    /// # Create new staged dir reader
    pub fn new(
        repository: &LocalRepository,
        dir: &Path,
    ) -> Result<StagedDirEntryReader, OxenError> {
        let db = StagedDirEntryDB::new_read_only(repository, dir)?;
        Ok(StagedDirEntryReader { db })
    }

    /// # Checks if the file exists in this directory
    /// More efficient than get_entry since it does not actual deserialize the entry
    pub fn has_entry<P: AsRef<Path>>(&self, path: P) -> bool {
        self.db.has_entry(path)
    }

    /// # Get the staged entry object from the file path
    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<StagedEntry>, OxenError> {
        self.db.get_entry(path)
    }

    /// # List the file paths in the staged dir
    /// More efficient than list_added_path_entries since it does not deserialize the entries
    pub fn list_added_paths(&self) -> Result<Vec<PathBuf>, OxenError> {
        log::debug!("trying to list added paths");
        self.db.list_added_paths()
    }

    /// # List file names and attached entries
    pub fn list_added_path_entries(&self) -> Result<Vec<(PathBuf, StagedEntry)>, OxenError> {
        self.db.list_added_path_entries()
    }
}
