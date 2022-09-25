use crate::constants::HISTORY_DIR;
use crate::db;
use crate::error::OxenError;
use crate::index::CommitEntryDBReader;
use crate::model::{CommitEntry, LocalRepository};
use crate::util;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;

/// # CommitDirEntryReader
/// We keep a list of all the committed files in a subdirectory directory for fast lookup
pub struct CommitDirEntryReader {
    db: DBWithThreadMode<MultiThreaded>,
    dir: PathBuf,
    pub repository: LocalRepository,
}

impl CommitDirEntryReader {
    pub fn history_dir(repo: &LocalRepository, dir: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(dir)
    }

    /// # Create new staged dir
    /// Contains all the staged files within that dir, for faster filtering during `oxen status`
    pub fn new(repository: &LocalRepository, dir: &Path) -> Result<CommitDirEntryReader, OxenError> {
        let dbpath = CommitDirEntryReader::history_dir(repository, dir);
        log::debug!("CommitDirEntryReader db_path {:?}", dbpath);
        if !dbpath.exists() {
            std::fs::create_dir_all(&dbpath)?;
        }
        let opts = db::opts::default();
        Ok(CommitDirEntryReader {
            db: DBWithThreadMode::open_for_read_only(&opts, &dbpath, true)?,
            dir: dir.to_owned(),
            repository: repository.clone(),
        })
    }

    pub fn num_entries(&self) -> usize {
        self.db.iterator(IteratorMode::Start).count()
    }

    pub fn has_file<T: AsRef<Path>>(&self, path: T) -> bool {
        let path = path.as_ref();
        CommitEntryDBReader::has_file(&self.db, path)
    }

    pub fn get_entry<T: AsRef<Path>>(&self, path: T) -> Result<Option<CommitEntry>, OxenError> {
        let path = path.as_ref();
        CommitEntryDBReader::get_entry(&self.db, path)
    }

    pub fn get_path_hash<T: AsRef<Path>>(&self, path: T) -> Result<String, OxenError> {
        let path = path.as_ref();
        let key = path.to_str().unwrap();
        let bytes = key.as_bytes();
        match self.db.get(bytes) {
            Ok(Some(value)) => {
                let value = str::from_utf8(&*value)?;
                let entry: CommitEntry = serde_json::from_str(value)?;
                Ok(entry.hash)
            }
            Ok(None) => Ok(String::from("")), // no hash, empty string
            Err(err) => {
                let err = format!("get_path_hash() Err: {}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn contains_path<T: AsRef<Path>>(&self, path: T) -> Result<bool, OxenError> {
        // Check if path is in this commit
        let path = path.as_ref();
        let key = path.to_str().unwrap();
        let bytes = key.as_bytes();
        match self.db.get(bytes) {
            Ok(Some(_value)) => Ok(true),
            Ok(None) => Ok(false),
            Err(err) => {
                let err = format!("contains_path Error reading db\nErr: {}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut paths: Vec<PathBuf> = vec![];
        let iter = self.db.iterator(IteratorMode::Start);
        for (key, _value) in iter {
            paths.push(PathBuf::from(str::from_utf8(&*key)?));
        }
        Ok(paths)
    }

    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        let mut paths: Vec<CommitEntry> = vec![];
        let iter = self.db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            let full_path = self.dir.join(str::from_utf8(&*key)?);
            let mut entry: CommitEntry = serde_json::from_str(str::from_utf8(&*value)?)?;
            entry.path = full_path;
            paths.push(entry);
        }
        Ok(paths)
    }

    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        let mut paths: HashSet<CommitEntry> = HashSet::new();
        let iter = self.db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            let full_path = self.dir.join(str::from_utf8(&*key)?);
            let mut entry: CommitEntry = serde_json::from_str(str::from_utf8(&*value)?)?;
            entry.path = full_path;
            paths.insert(entry);
        }
        Ok(paths)
    }
}
