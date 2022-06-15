
use crate::constants::{HISTORY_DIR, VERSIONS_DIR, DEFAULT_BRANCH_NAME};
use crate::db;
use crate::error::OxenError;
use crate::index::{CommitEntryDBReader, RefReader, RefWriter};
use crate::model::{Commit, CommitEntry, LocalRepository, StagedEntry, StagedEntryStatus};
use crate::util;

use indicatif::ProgressBar;
use filetime::FileTime;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{Path, PathBuf};
use std::fs;

pub struct CommitEntryWriter {
    repository: LocalRepository,
    db: DBWithThreadMode<MultiThreaded>,
}

impl CommitEntryWriter {
    pub fn versions_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(VERSIONS_DIR))
    }

    pub fn commit_db_dir(path: &Path, commit_id: &str) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(HISTORY_DIR)).join(commit_id)
    }

    pub fn new(repository: &LocalRepository, commit: &Commit) -> Result<CommitEntryWriter, OxenError> {
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

    fn create_db_dir_for_commit_id(repo: &LocalRepository, commit_id: &str) -> Result<PathBuf, OxenError> {
        // either copy over parent db as a starting point, or start new
        match CommitEntryWriter::head_commit_id(repo) {
            Ok(parent_id) => {
                log::debug!("CommitEntryWriter::create_db_dir_for_commit_id have parent_id {}", parent_id);
                // We have a parent, we have to copy over last db, and continue
                let parent_commit_db_path = CommitEntryWriter::commit_db_dir(&repo.path, &parent_id);
                let current_commit_db_path = CommitEntryWriter::commit_db_dir(&repo.path, &commit_id);
                log::debug!("COPY DB from {:?} => {:?}", parent_commit_db_path, current_commit_db_path);

                util::fs::copy_dir_all(&parent_commit_db_path, &current_commit_db_path)?;
                // return current commit path, so we can add to it
                Ok(current_commit_db_path)
            }
            Err(err) => {
                log::debug!("CommitEntryWriter::create_db_dir_for_commit_id do not have parent id {:?}", err);
                // We are creating initial commit, no parent
                let commit_db_path = CommitEntryWriter::commit_db_dir(&repo.path, &commit_id);
                if !commit_db_path.exists() {
                    std::fs::create_dir_all(&commit_db_path)?;
                }

                let ref_writer = RefWriter::new(&repo)?;
                // Set head to default name -> first commit
                ref_writer.create_branch(DEFAULT_BRANCH_NAME, commit_id)?;
                // Make sure head is pointing to that branch
                ref_writer.set_head(DEFAULT_BRANCH_NAME);

                // return current commit path, so we can insert into it
                Ok(commit_db_path)
            }
        }
    }

    fn head_commit_id(repo: &LocalRepository) -> Result<String, OxenError> {
        let ref_reader = RefReader::new(repo)?;
        ref_reader.head_commit_id()
    }

    pub fn set_file_timestamps(
        &self,
        entry: &CommitEntry,
        time: &FileTime
    ) -> Result<(), OxenError> {
        let key = entry.path.to_str().unwrap();
        let bytes = key.as_bytes();
        let entry = CommitEntry {
            id: entry.id.to_owned(),
            commit_id: entry.commit_id.to_owned(),
            path: entry.path.to_owned(),
            is_synced: entry.is_synced,
            hash: entry.hash.to_owned(),
            last_modified_seconds: time.unix_seconds(),
            last_modified_nanoseconds: time.nanoseconds()
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

    pub fn set_is_synced(
        &self,
        entry: &CommitEntry,
    ) -> Result<(), OxenError> {
        let key = entry.path.to_str().unwrap();
        let bytes = key.as_bytes();
        let entry = entry.to_synced();
        let json_str = serde_json::to_string(&entry)?;
        let data = json_str.as_bytes();
        match self.db.put(bytes, data) {
            Ok(_) => Ok(()),
            Err(err) => {
                let err = format!("set_is_synced() Err: {}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    fn add_modified_to_db(
        &self,
        new_commit: &Commit,
        path: &Path,
    ) -> Result<(), OxenError> {

        log::debug!("Commit new id [{}] modify file {:?}", new_commit.id, path);
        // entry_id will be the relative path of the file hashed
        let entry_id = util::hasher::hash_filename(path);

        // then hash the actual file contents
        let full_path = self.repository.path.join(path);
        let hash = util::hasher::hash_file_contents(&full_path)?;

        // Get last modified time
        let metadata = fs::metadata(full_path).unwrap();
        let mtime = FileTime::from_last_modification_time(&metadata);

        // Create entry object to as json
        let entry = CommitEntry {
            id: entry_id,
            commit_id: new_commit.id.to_owned(),
            path: path.to_path_buf(),
            hash,
            is_synced: false, // so we know to sync
            last_modified_seconds: mtime.unix_seconds(),
            last_modified_nanoseconds: mtime.nanoseconds(),
        };

        // Write to db & backup
        self.add_commit_entry(&new_commit.id, &entry)?;
        
        Ok(())
    }

    fn add_path_to_db(
        &self,
        new_commit: &Commit,
        path: &Path,
    ) -> Result<(), OxenError> {
        log::debug!("Commit [{}] add file {:?}", new_commit.id, path);
        // entry_id will be the relative path of the file hashed
        log::debug!("add_path_to_commit_db hash_filename: {:?}", path);
        let entry_id = util::hasher::hash_filename(path);

        // then hash the actual file contents
        let full_path = self.repository.path.join(path);
        let hash = util::hasher::hash_file_contents(&full_path)?;

        // Get last modified time
        let metadata = fs::metadata(full_path).unwrap();
        let mtime = FileTime::from_last_modification_time(&metadata);

        // Create entry object to as json
        let entry = CommitEntry {
            id: entry_id,
            commit_id: new_commit.id.to_owned(),
            path: path.to_path_buf(),
            hash,
            is_synced: false, // so we know to sync
            last_modified_seconds: mtime.unix_seconds(),
            last_modified_nanoseconds: mtime.nanoseconds(),
        };

        // Write to db & backup
        self.add_commit_entry(&new_commit.id, &entry)?;
        Ok(())
    }

    pub fn add_commit_entry(
        &self,
        commit_id: &str,
        entry: &CommitEntry,
    ) -> Result<(), OxenError> {
        self.backup_file_to_versions_dir(commit_id, entry)?;

        let path_str = entry.path.to_str().unwrap();
        let key = path_str.as_bytes();
        let entry_json = serde_json::to_string(&entry)?;
        log::debug!("ADD ENTRY to db[{:?}] {} -> {}", self.db.path(), path_str, entry_json);
        self.db.put(&key, entry_json.as_bytes())?;

        Ok(())
    }

    fn backup_file_to_versions_dir(
        &self,
        commit_id: &str,
        new_entry: &CommitEntry,
    ) -> Result<(), OxenError> {
        let full_path = self.repository.path.join(&new_entry.path);
        // create a copy to our versions directory
        // .oxen/versions/ENTRY_ID/COMMIT_ID.ext
        let name = format!("{}.{}", commit_id, new_entry.extension());
        let versions_entry_dir = CommitEntryWriter::versions_dir(&self.repository.path).join(&new_entry.id);
        let versions_path = versions_entry_dir.join(name);

        if !versions_entry_dir.exists() {
            // it's the first time
            log::debug!("Creating version dir for file: {:?}", new_entry.path);

            // Create version dir
            std::fs::create_dir_all(versions_entry_dir)?;
            std::fs::copy(full_path, versions_path)?;
        } else {
            // Make sure we only copy it if it hasn't changed
            if let Some(old_entry) = CommitEntryDBReader::get_entry(&self.db, &new_entry.path)? {
                log::debug!("got entry from db {:?}", new_entry);
                if new_entry.hash != old_entry.hash {
                    let filename = new_entry.filename();
                    let versions_path = versions_entry_dir.join(filename);
                    log::debug!(
                        "Commit new commit copying file {:?} to {:?}",
                        new_entry.path,
                        versions_path
                    );
                    std::fs::copy(full_path, versions_path)?;
                }
            } else {
                log::debug!("COULD NOT FIND ENTRY in db[{:?}] {:?}", self.db.path(), new_entry.path);
            }
        }

        Ok(())
    }

    fn remove_path_from_db(
        &self,
        path: &Path,
    ) -> Result<(), OxenError> {
        let path_str = path.to_str().unwrap();
        let key = path_str.as_bytes();
        self.db.delete(key)?;
        Ok(())
    }

    pub fn add_staged_entries(
        &self,
        commit: &Commit,
        added_files: &[(PathBuf, StagedEntry)]
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

    fn add_staged_files(
        &self,
        commit: &Commit,
        added_files: &[PathBuf],
    ) -> Result<(), OxenError> {
        // len kind of arbitrary right now...just nice to see progress on big sets of files
        if added_files.len() > 1000 {
            self.add_staged_files_with_prog(commit, added_files)
        } else {
            self.add_staged_files_without_prog(commit, added_files)
        }
    }

    fn commit_staged_entry(
        &self,
        commit: &Commit,
        path: &Path,
        entry: &StagedEntry,
    ) {
        match entry.status {
            StagedEntryStatus::Removed => {
                match self.remove_path_from_db(path) {
                    Ok(_) => {}
                    Err(err) => {
                        let err = format!(
                            "Failed to remove file: {}",
                            err
                        );
                        eprintln!("{}", err)
                    }
                }
            },
            StagedEntryStatus::Modified => {
                match self.add_modified_to_db(commit, path) {
                    Ok(_) => {}
                    Err(err) => {
                        let err = format!(
                            "Failed to commit MODIFIED file: {}",
                            err
                        );
                        eprintln!("{}", err)
                    }
                }
            },
            StagedEntryStatus::Added => {
                match self.add_path_to_db(commit, path) {
                    Ok(_) => {}
                    Err(err) => {
                        let err = format!(
                            "Failed to ADD file: {}",
                            err
                        );
                        eprintln!("{}", err)
                    }
                }
            }
        }
    }

    fn add_staged_files_without_prog(
        &self,
        commit: &Commit,
        added_files: &[PathBuf],
    ) -> Result<(), OxenError> {
        added_files.par_iter().for_each(|path| {
            if self.add_path_to_db(commit, path).is_err() {
                eprintln!("Error staging file... {:?}", path);
            }
        });
        Ok(())
    }

    fn add_staged_files_with_prog(
        &self,
        commit: &Commit,
        added_files: &[PathBuf],
    ) -> Result<(), OxenError> {
        let size: u64 = unsafe { std::mem::transmute(added_files.len()) };
        let bar = ProgressBar::new(size);
        added_files.par_iter().for_each(|path| {
            if self.add_path_to_db(commit, path).is_err() {
                eprintln!("Error staging file... {:?}", path);
            }
            bar.inc(1);
        });
        bar.finish();
        Ok(())
    }

    pub fn add_staged_dirs(
        &self,
        commit: &Commit,
        added_dirs: &[(PathBuf, usize)],
    ) -> Result<(), OxenError> {
        for (dir, _) in added_dirs.iter() {
            // println!("Commit [{}] files in dir: {:?}", commit.id, dir);
            let full_path = self.repository.path.join(dir);
            let files: Vec<PathBuf> = util::fs::rlist_files_in_dir(&full_path)
                .into_iter()
                .map(|path| util::fs::path_relative_to_dir(&path, &self.repository.path).unwrap())
                .filter(|path| !CommitEntryDBReader::has_file(&self.db, path))
                .collect();
            self.add_staged_files(commit, &files)?;
        }
        Ok(())
    }
}