use crate::constants::{DEFAULT_BRANCH_NAME, HISTORY_DIR, VERSIONS_DIR};
use crate::db;
use crate::error::OxenError;
use crate::index::{RefReader, RefWriter};
use crate::model::{Commit, CommitEntry, LocalRepository, StagedEntry, StagedEntryStatus};
use crate::util;

use filetime::FileTime;
use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::fs;
use std::path::{Path, PathBuf};

pub struct CommitEntryWriter {
    repository: LocalRepository,
    db: DBWithThreadMode<MultiThreaded>,
}

impl CommitEntryWriter {
    pub fn versions_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(VERSIONS_DIR))
    }

    pub fn commit_db_dir(path: &Path, commit_id: &str) -> PathBuf {
        util::fs::oxen_hidden_dir(path)
            .join(Path::new(HISTORY_DIR))
            .join(commit_id)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitEntryWriter, OxenError> {
        log::debug!("CommitEntryWriter::new() commit_id: {}", commit.id);
        let db_path = CommitEntryWriter::commit_db_dir(&repository.path, &commit.id);
        if !db_path.exists() {
            CommitEntryWriter::create_db_dir_for_commit_id(repository, &commit.id)?;
        }

        let opts = db::opts::default();
        Ok(CommitEntryWriter {
            repository: repository.clone(),
            db: DBWithThreadMode::open(&opts, &db_path)?,
        })
    }

    fn create_db_dir_for_commit_id(
        repo: &LocalRepository,
        commit_id: &str,
    ) -> Result<PathBuf, OxenError> {
        // either copy over parent db as a starting point, or start new
        match CommitEntryWriter::head_commit_id(repo) {
            Ok(Some(parent_id)) => {
                log::debug!(
                    "CommitEntryWriter::create_db_dir_for_commit_id have parent_id {}",
                    parent_id
                );
                // We have a parent, we have to copy over last db, and continue
                let parent_commit_db_path =
                    CommitEntryWriter::commit_db_dir(&repo.path, &parent_id);
                let current_commit_db_path =
                    CommitEntryWriter::commit_db_dir(&repo.path, commit_id);
                log::debug!(
                    "COPY DB from {:?} => {:?}",
                    parent_commit_db_path,
                    current_commit_db_path
                );

                util::fs::copy_dir_all(&parent_commit_db_path, &current_commit_db_path)?;
                // return current commit path, so we can add to it
                Ok(current_commit_db_path)
            }
            _ => {
                log::debug!(
                    "CommitEntryWriter::create_db_dir_for_commit_id does not have parent id",
                );
                // We are creating initial commit, no parent
                let commit_db_path = CommitEntryWriter::commit_db_dir(&repo.path, commit_id);
                if !commit_db_path.exists() {
                    std::fs::create_dir_all(&commit_db_path)?;
                }

                let ref_writer = RefWriter::new(repo)?;
                // Set head to default name -> first commit
                ref_writer.create_branch(DEFAULT_BRANCH_NAME, commit_id)?;
                // Make sure head is pointing to that branch
                ref_writer.set_head(DEFAULT_BRANCH_NAME);

                // return current commit path, so we can insert into it
                Ok(commit_db_path)
            }
        }
    }

    fn head_commit_id(repo: &LocalRepository) -> Result<Option<String>, OxenError> {
        let ref_reader = RefReader::new(repo)?;
        ref_reader.head_commit_id()
    }

    pub fn set_file_timestamps(
        &self,
        entry: &CommitEntry,
        time: &FileTime,
    ) -> Result<(), OxenError> {
        let key = entry.path.to_str().unwrap();
        let bytes = key.as_bytes();
        let entry = CommitEntry {
            commit_id: entry.commit_id.to_owned(),
            path: entry.path.to_owned(),
            hash: entry.hash.to_owned(),
            num_bytes: entry.num_bytes,
            last_modified_seconds: time.unix_seconds(),
            last_modified_nanoseconds: time.nanoseconds(),
        };

        let json_str = serde_json::to_string(&entry)?;
        let data = json_str.as_bytes();
        match self.db.put(bytes, data) {
            Ok(_) => Ok(()),
            Err(err) => {
                let err = format!("set_file_timestamps() Err: {}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    fn add_path_to_db(
        &self,
        new_commit: &Commit,
        staged_entry: &StagedEntry,
        path: &Path,
    ) -> Result<(), OxenError> {
        log::debug!("Commit [{}] add file {:?}", new_commit.id, path);

        // then hash the actual file contents
        let full_path = self.repository.path.join(path);

        // Get last modified time
        let metadata = fs::metadata(&full_path).unwrap();
        let mtime = FileTime::from_last_modification_time(&metadata);

        let metadata = fs::metadata(&full_path)?;

        // Create entry object to as json
        let entry = CommitEntry {
            commit_id: new_commit.id.to_owned(),
            path: path.to_path_buf(),
            hash: staged_entry.hash.to_owned(),
            num_bytes: metadata.len(),
            last_modified_seconds: mtime.unix_seconds(),
            last_modified_nanoseconds: mtime.nanoseconds(),
        };

        // Write to db & backup
        self.add_commit_entry(&entry)?;
        Ok(())
    }

    pub fn add_commit_entry(&self, entry: &CommitEntry) -> Result<(), OxenError> {
        self.backup_file_to_versions_dir(entry)?;

        let path_str = entry.path.to_str().unwrap();
        let key = path_str.as_bytes();
        let entry_json = serde_json::to_string(&entry)?;
        log::debug!(
            "ADD ENTRY to db[{:?}] {} -> {}",
            self.db.path(),
            path_str,
            entry_json
        );
        self.db.put(&key, entry_json.as_bytes())?;

        Ok(())
    }

    fn backup_file_to_versions_dir(&self, new_entry: &CommitEntry) -> Result<(), OxenError> {
        let full_path = self.repository.path.join(&new_entry.path);
        // create a copy to our versions directory
        // .oxen/versions/ENTRY_HASH/COMMIT_ID.ext
        // where ENTRY_HASH is something like subdirs: 59/E029D4812AEBF0

        let versions_entry_path = util::fs::version_path(&self.repository, new_entry);
        let versions_entry_dir = versions_entry_path.parent().unwrap();

        if !versions_entry_dir.exists() {
            // it's the first time
            log::debug!(
                "Creating version dir for file: {:?} -> {:?}",
                new_entry.path,
                versions_entry_dir
            );

            // Create version dir
            std::fs::create_dir_all(versions_entry_dir)?;
        }

        if !versions_entry_path.exists() {
            log::debug!(
                "Copying commit entry for file: {:?} -> {:?}",
                new_entry.path,
                versions_entry_path
            );
            std::fs::copy(full_path, versions_entry_path)?;
        }

        Ok(())
    }

    fn remove_path_from_db(&self, path: &Path) -> Result<(), OxenError> {
        let path_str = path.to_str().unwrap();
        let key = path_str.as_bytes();
        self.db.delete(key)?;
        Ok(())
    }

    pub fn add_staged_entries(
        &self,
        commit: &Commit,
        added_files: &[(PathBuf, StagedEntry)],
    ) -> Result<(), OxenError> {
        // len kind of arbitrary right now...just nice to see progress on big sets of files
        if added_files.len() > 1000 {
            self.add_staged_entries_with_prog(commit, added_files)
        } else {
            self.add_staged_entries_without_prog(commit, added_files)
        }
    }

    fn add_staged_entries_with_prog(
        &self,
        commit: &Commit,
        added_files: &[(PathBuf, StagedEntry)],
    ) -> Result<(), OxenError> {
        let size: u64 = unsafe { std::mem::transmute(added_files.len()) };
        let bar = ProgressBar::new(size);
        added_files.par_iter().for_each(|(path, entry)| {
            self.commit_staged_entry(commit, path, entry);
            bar.inc(1);
        });
        bar.finish();
        Ok(())
    }

    fn add_staged_entries_without_prog(
        &self,
        commit: &Commit,
        added_files: &[(PathBuf, StagedEntry)],
    ) -> Result<(), OxenError> {
        added_files
            .par_iter()
            .for_each(|(path, entry)| self.commit_staged_entry(commit, path, entry));
        Ok(())
    }

    fn commit_staged_entry(&self, commit: &Commit, path: &Path, entry: &StagedEntry) {
        match entry.status {
            StagedEntryStatus::Removed => match self.remove_path_from_db(path) {
                Ok(_) => {}
                Err(err) => {
                    let err = format!("Failed to remove file: {}", err);
                    eprintln!("{}", err)
                }
            },
            StagedEntryStatus::Modified => match self.add_path_to_db(commit, entry, path) {
                Ok(_) => {}
                Err(err) => {
                    let err = format!("Failed to commit MODIFIED file: {}", err);
                    eprintln!("{}", err)
                }
            },
            StagedEntryStatus::Added => match self.add_path_to_db(commit, entry, path) {
                Ok(_) => {}
                Err(err) => {
                    let err = format!("Failed to ADD file: {}", err);
                    eprintln!("{}", err)
                }
            },
        }
    }
}
