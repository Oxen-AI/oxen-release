use crate::constants::HISTORY_DIR;
use crate::db;
use crate::error::OxenError;
use crate::index::path_db;
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
    pub fn db_dir(repo: &LocalRepository, commit_id: &str, dir: &Path) -> PathBuf {
        // .oxen/history/COMMIT_ID/path/to/dir
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(commit_id)
            .join(dir)
    }

    /// # Create new staged dir
    /// Contains all the staged files within that dir, for faster filtering during `oxen status`
    pub fn new(repository: &LocalRepository, commit_id: &str, dir: &Path) -> Result<CommitDirEntryReader, OxenError> {
        let dbpath = CommitDirEntryReader::db_dir(repository, commit_id, dir);
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

    pub fn has_file<P: AsRef<Path>>(&self, path: P) -> bool {
        let path = path.as_ref();
        path_db::has_entry(&self.db, path)
    }

    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<CommitEntry>, OxenError> {
        let path = path.as_ref();
        path_db::get_entry(&self.db, path)
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        path_db::list_paths(&self.db, &self.dir)
    }

    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        path_db::list_entries(&self.db, &self.dir)
    }

    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        path_db::list_entries_set(&self.db, &self.dir)
    }
}
