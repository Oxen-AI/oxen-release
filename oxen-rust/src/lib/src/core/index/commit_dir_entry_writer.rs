use crate::constants::{FILES_DIR, HISTORY_DIR, OBJECTS_DIR, OBJECT_FILES_DIR};
use crate::core::db;
use crate::core::db::path_db;
use crate::core::db::tree_db::TreeObject;
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository};
use crate::util;

use filetime::FileTime;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{Path, PathBuf};
use std::str;

/// # CommitDirEntryWriter
/// We keep a list of all the committed files in a subdirectory directory for fast lookup
pub struct CommitDirEntryWriter {
    pub db: DBWithThreadMode<MultiThreaded>,
    // pub files_db: DBWithThreadMode<MultiThreaded>,
    pub repository: LocalRepository,
}

impl CommitDirEntryWriter {
    pub fn db_dir(repo: &LocalRepository, commit_id: &str, dir: &Path) -> PathBuf {
        // .oxen/history/COMMIT_ID/files/path/to/dir
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(commit_id)
            .join(FILES_DIR)
            .join(dir)
    }

    pub fn files_db_dir(repo: &LocalRepository, commit_id: &str, dir: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(OBJECTS_DIR).join(Path::new(OBJECT_FILES_DIR)))
    }

    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
        dir: &Path,
    ) -> Result<CommitDirEntryWriter, OxenError> {
        let dbpath = CommitDirEntryWriter::db_dir(repository, commit_id, dir);
        let files_dbpath = CommitDirEntryWriter::files_db_dir(repository, commit_id, dir);
        log::debug!("CommitDirEntryWriter db_path {:?}", dbpath);
        if !dbpath.exists() {
            std::fs::create_dir_all(&dbpath)?;
        }
        let opts = db::opts::default();
        Ok(CommitDirEntryWriter {
            // files_db: DBWithThreadMode::open(&opts, dunce::simplified(&files_dbpath))?,
            db: DBWithThreadMode::open(&opts, dunce::simplified(&dbpath))?,
            repository: repository.clone(),
        })
    }

    pub fn set_file_timestamps(
        &self,
        entry: &CommitEntry,
        time: &FileTime,
        files_db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        // Get the entry
        let file_entry: Option<TreeObject> = path_db::get_entry(files_db, entry.hash.clone())?;

        match file_entry {
            Some(entry) => match entry {
                TreeObject::File {
                    hash,
                    num_bytes,
                    last_modified_seconds,
                    last_modified_nanoseconds,
                } => {
                    let updated_entry = TreeObject::File {
                        hash: hash.clone(),
                        num_bytes,
                        last_modified_seconds: time.unix_seconds(),
                        last_modified_nanoseconds: time.nanoseconds(),
                    };
                    path_db::put(files_db, hash, &updated_entry)?;
                }
                _ => {
                    log::error!("Attempting to set timestamps for invalid entry type");
                }
            },
            None => {
                log::error!(
                    "Could not find file for setting timestamps: {:?}",
                    entry.path
                );
            }
        }
        Ok(())
    }

    pub fn add_commit_entry(&self, entry: &CommitEntry) -> Result<(), OxenError> {
        path_db::put(&self.db, entry.path.file_name().unwrap(), &entry)
    }

    pub fn remove_path_from_db(&self, path: &Path) -> Result<(), OxenError> {
        path_db::delete(&self.db, path.file_name().unwrap())
    }
}
