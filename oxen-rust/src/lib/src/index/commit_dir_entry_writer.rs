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

/// # CommitDirEntryWriter
/// We keep a list of all the committed files in a subdirectory directory for fast lookup
pub struct CommitDirEntryWriter {
    db: DBWithThreadMode<MultiThreaded>,
    dir: PathBuf,
    pub repository: LocalRepository,
}

impl CommitDirEntryWriter {
    pub fn db_dir(repo: &LocalRepository, commit_id: &str, dir: &Path) -> PathBuf {
        // .oxen/history/COMMIT_ID/path/to/dir
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(commit_id)
            .join(dir)
    }

    pub fn new(repository: &LocalRepository, commit_id: &str, dir: &Path) -> Result<CommitDirEntryWriter, OxenError> {
        let dbpath = CommitDirEntryWriter::db_dir(repository, commit_id, dir);
        log::debug!("CommitDirEntryWriter db_path {:?}", dbpath);
        if !dbpath.exists() {
            std::fs::create_dir_all(&dbpath)?;
        }
        let opts = db::opts::default();
        Ok(CommitDirEntryWriter {
            db: DBWithThreadMode::open(&opts, &dbpath)?,
            dir: dir.to_owned(),
            repository: repository.clone(),
        })
    }
}