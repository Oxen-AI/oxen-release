use crate::constants::HISTORY_DIR;
use crate::db;
use crate::error::OxenError;
use crate::index::path_db;
use crate::model::{CommitEntry, LocalRepository};
use crate::util;

use filetime::FileTime;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{Path, PathBuf};
use std::str;

/// # CommitDirEntryWriter
/// We keep a list of all the committed files in a subdirectory directory for fast lookup
pub struct CommitDirEntryWriter {
    db: DBWithThreadMode<MultiThreaded>,
    pub repository: LocalRepository,
}

impl CommitDirEntryWriter {
    pub fn db_dir(repo: &LocalRepository, commit_id: &str, dir: &Path) -> PathBuf {
        // .oxen/history/COMMIT_ID/files/path/to/dir
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(commit_id)
            .join("files")
            .join(dir)
    }

    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
        dir: &Path,
    ) -> Result<CommitDirEntryWriter, OxenError> {
        let dbpath = CommitDirEntryWriter::db_dir(repository, commit_id, dir);
        log::debug!("CommitDirEntryWriter db_path {:?}", dbpath);
        if !dbpath.exists() {
            std::fs::create_dir_all(&dbpath)?;
        }
        let opts = db::opts::default();
        Ok(CommitDirEntryWriter {
            db: DBWithThreadMode::open(&opts, &dbpath)?,
            repository: repository.clone(),
        })
    }

    pub fn set_file_timestamps(
        &self,
        entry: &CommitEntry,
        time: &FileTime,
    ) -> Result<(), OxenError> {
        let entry = CommitEntry {
            commit_id: entry.commit_id.to_owned(),
            path: entry.path.to_owned(),
            hash: entry.hash.to_owned(),
            num_bytes: entry.num_bytes,
            last_modified_seconds: time.unix_seconds(),
            last_modified_nanoseconds: time.nanoseconds(),
        };

        path_db::put(&self.db, &entry.path, &entry)
    }

    pub fn add_commit_entry(&self, entry: &CommitEntry) -> Result<(), OxenError> {
        path_db::put(&self.db, &entry.path, &entry)
    }

    pub fn remove_path_from_db(&self, path: &Path) -> Result<(), OxenError> {
        path_db::delete(&self.db, path)
    }
}
