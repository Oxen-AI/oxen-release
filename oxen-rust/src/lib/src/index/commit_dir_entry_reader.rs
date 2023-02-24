use crate::constants::{FILES_DIR, HISTORY_DIR};
use crate::db;
use crate::db::path_db;
use crate::error::OxenError;
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
        // .oxen/history/COMMIT_ID/files/path/to/dir
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(FILES_DIR)
            .join(dir)
    }

    /// # Create new commit dir
    /// Contains all the committed files within that dir, for faster filtering per dir
    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
        dir: &Path,
    ) -> Result<CommitDirEntryReader, OxenError> {
        let db_path = CommitDirEntryReader::db_dir(repository, commit_id, dir);
        log::debug!(
            "CommitDirEntryReader::new() dir {:?} db_path {:?}",
            dir,
            db_path
        );
        let opts = db::opts::default();
        // Must check the CURRENT file since the .oxen/history/COMMIT_ID/files/ path
        // may have already been created by a deeper object
        if !db_path.join("CURRENT").exists() {
            if std::fs::create_dir_all(&db_path).is_err() {
                log::error!("CommitDirEntryReader could not create dir {:?}", db_path);
            }
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
        }

        Ok(CommitDirEntryReader {
            db: DBWithThreadMode::open_for_read_only(&opts, &db_path, true)?,
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
        path_db::list_entries(&self.db)
    }

    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        path_db::list_entries_set(&self.db)
    }

    pub fn list_entry_page(
        &self,
        page: usize,
        page_size: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        path_db::list_entry_page(&self.db, page, page_size)
    }
}
