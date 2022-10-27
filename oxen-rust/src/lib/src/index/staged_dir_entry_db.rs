use crate::db;
use crate::db::path_db;
use crate::error::OxenError;
use crate::index::stager::STAGED_DIR;
use crate::model::{CommitEntry, EntryType, LocalRepository, StagedEntry, StagedEntryStatus};
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{Path, PathBuf};

/// # StagedDirEntryDB
/// We keep a list of all the staged files in a directory for fast lookup
pub struct StagedDirEntryDB {
    db: DBWithThreadMode<MultiThreaded>,
    dir: PathBuf,
    pub repository: LocalRepository,
}

impl StagedDirEntryDB {
    pub fn staging_dir(repo: &LocalRepository, dir: &Path) -> PathBuf {
        log::debug!("StagedDirEntryDB got repo path {:?}", repo.path);
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(STAGED_DIR))
            .join("files")
            .join(dir)
    }

    /// # Create new staged dir
    /// Contains all the staged files within that dir, for faster filtering during `oxen status`
    pub fn new(repository: &LocalRepository, dir: &Path) -> Result<StagedDirEntryDB, OxenError> {
        log::debug!("StagedDirEntryDB got dir {:?}", dir);
        let db_path = StagedDirEntryDB::staging_dir(repository, dir);

        log::debug!("StagedDirEntryDB db_path {:?}", db_path);
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }
        let opts = db::opts::default();
        Ok(StagedDirEntryDB {
            db: DBWithThreadMode::open(&opts, &db_path)?,
            dir: dir.to_owned(),
            repository: repository.clone(),
        })
    }

    /// # Stages a file that has been removed from the index
    /// Flags it as removed, so we know when committing that we have to remove it from the commit db
    pub fn add_removed_file<P: AsRef<Path>>(
        &self,
        path: P,
        entry: &CommitEntry,
    ) -> Result<StagedEntry, OxenError> {
        let path = path.as_ref();
        let entry = StagedEntry {
            hash: entry.hash.clone(),
            status: StagedEntryStatus::Removed,
            entry_type: EntryType::Regular,
        };

        path_db::put(&self.db, path, &entry)?;

        Ok(entry)
    }

    /// # Checks if the file exists in this directory
    /// More efficient than get_entry since it does not actual deserialize the entry
    pub fn has_entry<P: AsRef<Path>>(&self, path: P) -> bool {
        path_db::has_entry(&self.db, path)
    }

    /// # Get the staged entry object from the file path
    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<StagedEntry>, OxenError> {
        path_db::get_entry(&self.db, path)
    }

    /// # Serializes the entry to json and writes to db
    pub fn add_staged_entry_to_db<T: AsRef<Path>>(
        &self,
        path: T,
        staged_entry: &StagedEntry,
    ) -> Result<(), OxenError> {
        path_db::put(&self.db, path, staged_entry)
    }

    /// # List the file paths in the staged dir
    /// More efficient than list_added_path_entries since it does not deserialize the entries
    pub fn list_added_paths(&self) -> Result<Vec<PathBuf>, OxenError> {
        path_db::list_paths(&self.db, &self.dir)
    }

    /// # List file names and attached entries
    pub fn list_added_path_entries(&self) -> Result<Vec<(PathBuf, StagedEntry)>, OxenError> {
        path_db::list_path_entries(&self.db, &self.dir)
    }

    pub fn unstage(&self) -> Result<(), OxenError> {
        path_db::clear(&self.db)
    }
}
