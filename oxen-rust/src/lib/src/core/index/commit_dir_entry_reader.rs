//! Reader to find entries within a commit directory
//!

use crate::constants::{FILES_DIR, HISTORY_DIR};
use crate::core::db;
use crate::core::db::path_db;
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
}

impl CommitDirEntryReader {
    /// .oxen/history/commit_id/files/path/to/dir
    pub fn db_dir(base_path: &Path, commit_id: &str, dir: &Path) -> PathBuf {
        if dir == Path::new("") {
            return util::fs::oxen_hidden_dir(base_path)
                .join(HISTORY_DIR)
                .join(commit_id)
                .join(FILES_DIR);
        }

        util::fs::oxen_hidden_dir(base_path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(FILES_DIR)
            .join(dir)
    }

    pub fn db_exists(base_path: &Path, commit_id: &str, dir: &Path) -> bool {
        // Must check the CURRENT file since the .oxen/history/COMMIT_ID/files/ path
        // may have already been created by a deeper object
        let db_path = CommitDirEntryReader::db_dir(base_path, commit_id, dir);
        db_path.join("CURRENT").exists()
    }

    /// # Create new commit dir
    /// Contains all the committed files within that dir, for faster filtering per dir
    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
        dir: &Path,
    ) -> Result<CommitDirEntryReader, OxenError> {
        CommitDirEntryReader::new_from_path(&repository.path, commit_id, dir)
    }

    pub fn new_from_path(
        base_path: &Path,
        commit_id: &str,
        dir: &Path,
    ) -> Result<CommitDirEntryReader, OxenError> {
        let db_path = CommitDirEntryReader::db_dir(base_path, commit_id, dir);
        log::debug!(
            "CommitDirEntryReader::new() dir {:?} db_path {:?}",
            dir,
            db_path
        );

        let opts = db::opts::default();
        if !CommitDirEntryReader::db_exists(base_path, commit_id, dir) {
            if let Err(err) = std::fs::create_dir_all(&db_path) {
                log::error!("CommitDirEntryReader could not create dir {db_path:?}\nErr: {err:?}");
            }
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
        }

        Ok(CommitDirEntryReader {
            db: DBWithThreadMode::open_for_read_only(&opts, &db_path, true)?,
            dir: dir.to_owned(),
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
        // log::debug!("CommitDirEntryReader::get_entry({:?})", path);
        let result = path_db::get_entry(&self.db, path);
        // log::debug!(
        //     "CommitDirEntryReader::get_entry({:?}) -> {:?}",
        //     path,
        //     result
        // );
        result
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

    pub fn list_entry_page_with_offset(
        &self,
        page: usize,
        page_size: usize,
        offset: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        path_db::list_entry_page_with_offset(&self.db, page, page_size, offset)
    }
}
