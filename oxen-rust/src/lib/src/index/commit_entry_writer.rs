use crate::constants::{self, DEFAULT_BRANCH_NAME, HISTORY_DIR, VERSIONS_DIR};
use crate::db;
use crate::db::path_db;
use crate::error::OxenError;
use crate::index::{CommitDirEntryWriter, RefReader, RefWriter, SchemaWriter};
use crate::model::schema::Schema;
use crate::model::{
    Commit, CommitEntry, LocalRepository, StagedData, StagedEntry, StagedEntryStatus,
};
use crate::util;

use filetime::FileTime;
use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

// type Vec2DStr = Vec<Vec<String>>;

pub struct CommitEntryWriter {
    repository: LocalRepository,
    dir_db: DBWithThreadMode<MultiThreaded>,
    commit: Commit,
}

impl CommitEntryWriter {
    pub fn versions_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(VERSIONS_DIR))
    }

    pub fn commit_dir(path: &Path, commit_id: &str) -> PathBuf {
        util::fs::oxen_hidden_dir(path)
            .join(Path::new(HISTORY_DIR))
            .join(commit_id)
    }

    pub fn commit_dir_db(path: &Path, commit_id: &str) -> PathBuf {
        CommitEntryWriter::commit_dir(path, commit_id).join(constants::DIRS_DIR)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitEntryWriter, OxenError> {
        log::debug!("CommitEntryWriter::new() commit_id: {}", commit.id);
        let db_path = CommitEntryWriter::commit_dir_db(&repository.path, &commit.id);
        if !db_path.exists() {
            CommitEntryWriter::create_db_dir_for_commit_id(repository, &commit.id)?;
        }

        let opts = db::opts::default();
        Ok(CommitEntryWriter {
            repository: repository.clone(),
            dir_db: DBWithThreadMode::open(&opts, &db_path)?,
            commit: commit.to_owned(),
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
                let parent_commit_db_path = CommitEntryWriter::commit_dir(&repo.path, &parent_id);
                let current_commit_db_path = CommitEntryWriter::commit_dir(&repo.path, commit_id);
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
                let commit_db_path = CommitEntryWriter::commit_dir_db(&repo.path, commit_id);
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
        if let Some(parent) = entry.path.parent() {
            let writer = CommitDirEntryWriter::new(&self.repository, &self.commit.id, parent)?;
            writer.set_file_timestamps(entry, time)
        } else {
            Err(OxenError::file_has_no_parent(&entry.path))
        }
    }

    fn add_staged_entry_to_db(
        &self,
        writer: &CommitDirEntryWriter,
        new_commit: &Commit,
        staged_entry: &StagedEntry,
        path: &Path,
    ) -> Result<(), OxenError> {
        self.add_regular_staged_entry_to_db(writer, new_commit, staged_entry, path)
    }

    fn add_regular_staged_entry_to_db(
        &self,
        writer: &CommitDirEntryWriter,
        new_commit: &Commit,
        staged_entry: &StagedEntry,
        path: &Path,
    ) -> Result<(), OxenError> {
        // log::debug!("Commit [{}] add file {:?}", new_commit.id, path);

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
        self.add_commit_entry(writer, entry)?;
        Ok(())
    }

    fn add_commit_entry(
        &self,
        writer: &CommitDirEntryWriter,
        entry: CommitEntry,
    ) -> Result<(), OxenError> {
        let entry = self.backup_file_to_versions_dir(entry)?;
        log::debug!(
            "add_commit_entry with hash {:?} -> {}",
            entry.path,
            entry.hash
        );

        writer.add_commit_entry(&entry)
    }

    fn backup_file_to_versions_dir(&self, entry: CommitEntry) -> Result<CommitEntry, OxenError> {
        let full_path = self.repository.path.join(&entry.path);
        log::debug!("backup_file_to_versions_dir {:?}", entry.path);

        // if util::fs::is_tabular(&entry.path) {
        //     // We save off an .arrow file for tabular data for faster access and optimized DF commands
        //     entry = self.backup_arrow_file(commit, entry, &full_path)?;
        // } else {
        // create a copy to our versions directory
        // .oxen/versions/ENTRY_HASH/COMMIT_ID.ext
        // where ENTRY_HASH is something like subdirs: 59/E029D4812AEBF0
        let versions_entry_path = util::fs::version_path(&self.repository, &entry);
        let versions_entry_dir = versions_entry_path.parent().unwrap();

        log::debug!(
            "Copying commit entry for file: {:?} -> {:?}",
            entry.path,
            versions_entry_path
        );

        // Create dir if not exists
        if !versions_entry_dir.exists() {
            std::fs::create_dir_all(versions_entry_dir)?;
        }

        std::fs::copy(full_path, versions_entry_path)?;
        // }

        Ok(entry)
    }

    pub fn commit_staged_entries(
        &self,
        commit: &Commit,
        staged_data: &StagedData,
    ) -> Result<(), OxenError> {
        self.commit_staged_entries_with_prog(commit, staged_data)?;
        self.commit_schemas(commit, &staged_data.added_schemas)
    }

    fn commit_schemas(
        &self,
        commit: &Commit,
        schemas: &HashMap<PathBuf, Schema>,
    ) -> Result<(), OxenError> {
        log::debug!("commit_schemas got {} schemas", schemas.len());

        let schema_writer = SchemaWriter::new(&self.repository, &commit.id)?;
        for (path, schema) in schemas.iter() {
            // Add schema if it does not exist
            if !schema_writer.has_schema(schema) {
                schema_writer.put_schema(schema)?;
            }

            // Map the file to the schema
            schema_writer.put_schema_for_file(path, schema)?;
        }

        Ok(())
    }

    fn group_staged_files_to_dirs(
        &self,
        files: &HashMap<PathBuf, StagedEntry>,
    ) -> HashMap<PathBuf, Vec<(PathBuf, StagedEntry)>> {
        let mut results: HashMap<PathBuf, Vec<(PathBuf, StagedEntry)>> = HashMap::new();

        for (path, entry) in files.iter() {
            if let Some(parent) = path.parent() {
                results
                    .entry(parent.to_path_buf())
                    .or_default()
                    .push((path.clone(), entry.clone()));
            }
        }

        results
    }

    fn commit_staged_entries_with_prog(
        &self,
        commit: &Commit,
        staged_data: &StagedData,
    ) -> Result<(), OxenError> {
        let size: u64 = unsafe { std::mem::transmute(staged_data.added_files.len()) };
        let bar = ProgressBar::new(size);
        let grouped = self.group_staged_files_to_dirs(&staged_data.added_files);
        log::debug!(
            "commit_staged_entries_with_prog got groups {}",
            grouped.len()
        );

        // Track dirs
        for (_path, staged_dirs) in staged_data.added_dirs.paths.iter() {
            for staged_dir in staged_dirs.iter() {
                log::debug!(
                    "commit_staged_entries_with_prog adding dir {:?}",
                    staged_dir.path
                );
                path_db::put(&self.dir_db, &staged_dir.path, &0)?;
            }
        }

        // Do regular before tabular
        for (dir, files) in grouped.iter() {
            // Write entries per dir
            let entry_writer = CommitDirEntryWriter::new(&self.repository, &self.commit.id, dir)?;
            path_db::put(&self.dir_db, dir, &0)?;

            // Commit entries data
            files.par_iter().for_each(|(path, entry)| {
                self.commit_staged_entry(&entry_writer, commit, path, entry);
                bar.inc(1);
            });
        }
        bar.finish();

        Ok(())
    }

    fn commit_staged_entry(
        &self,
        writer: &CommitDirEntryWriter,
        commit: &Commit,
        path: &Path,
        entry: &StagedEntry,
    ) {
        match entry.status {
            StagedEntryStatus::Removed => match writer.remove_path_from_db(path) {
                Ok(_) => {}
                Err(err) => {
                    let err = format!("Failed to remove file: {}", err);
                    panic!("{}", err)
                }
            },
            StagedEntryStatus::Modified => {
                match self.add_staged_entry_to_db(writer, commit, entry, path) {
                    Ok(_) => {}
                    Err(err) => {
                        let err = format!("Failed to commit MODIFIED file: {}", err);
                        panic!("{}", err)
                    }
                }
            }
            StagedEntryStatus::Added => {
                match self.add_staged_entry_to_db(writer, commit, entry, path) {
                    Ok(_) => {}
                    Err(err) => {
                        let err = format!("Failed to ADD file: {}", err);
                        panic!("{}", err)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {}
