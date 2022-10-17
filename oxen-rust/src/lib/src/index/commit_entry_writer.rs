use crate::constants::{self, DEFAULT_BRANCH_NAME, HISTORY_DIR, VERSIONS_DIR};
use crate::db;
use crate::error::OxenError;
use crate::index::{
    path_db, CommitDirEntryReader, CommitDirEntryWriter, RefReader, RefWriter, SchemaWriter,
};
use crate::media::{tabular, tabular_datafusion};
use crate::model::schema;
use crate::model::{
    Commit, CommitEntry, EntryType, LocalRepository, StagedData, StagedEntry, StagedEntryStatus,
};
use crate::util;

use filetime::FileTime;
use futures::executor::block_on;
use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

type Vec2DStr = Vec<Vec<String>>;

pub struct CommitEntryWriter {
    repository: LocalRepository,
    dir_db: DBWithThreadMode<MultiThreaded>,
    commit_id: String,
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
        CommitEntryWriter::commit_dir(path, commit_id).join("dirs/")
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
            commit_id: commit.id.to_owned(),
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
            let writer = CommitDirEntryWriter::new(&self.repository, &self.commit_id, parent)?;
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
        match staged_entry.entry_type {
            EntryType::Regular => {
                self.add_regular_staged_entry_to_db(writer, new_commit, staged_entry, path)
            }
            EntryType::Tabular => {
                self.save_row_level_data(new_commit, path)?;
                self.add_regular_staged_entry_to_db(writer, new_commit, staged_entry, path)
            }
        }
    }

    fn save_row_level_data(&self, commit: &Commit, path: &Path) -> Result<(), OxenError> {
        log::debug!("save_row_level_data....");
        let path = self.repository.path.join(path);
        let results = tabular_datafusion::group_rows_by_key(path, "file");
        match block_on(results) {
            Ok((groups, schema)) => {
                println!("Saving annotations for {} files", groups.len());
                let size = groups.len() as u64;
                let bar = ProgressBar::new(size);

                let dir_groups = self.group_annotations_to_dirs(&groups);
                for (dir, group) in dir_groups.iter() {
                    let commit_id = &commit.id;
                    let commit_entry_reader =
                        CommitDirEntryReader::new(&self.repository, commit_id, dir)?;
                    group.par_iter().for_each(|(file, data)| {
                        let filename = file.file_name().unwrap();
                        log::debug!("save_row_level_data checking for file: {:?}", filename);
                        if let Ok(Some(entry)) = commit_entry_reader.get_entry(Path::new(filename))
                        {
                            let version_dir =
                                util::fs::version_dir_from_hash(&self.repository, entry.hash);
                            let annotation_dir = version_dir.join(commit_id);
                            if !annotation_dir.exists() {
                                fs::create_dir_all(&annotation_dir).unwrap();
                            }
                            let annotation_file =
                                annotation_dir.join(constants::ANNOTATIONS_FILENAME);
                            if tabular_datafusion::save_rows(annotation_file, data, schema.clone())
                                .is_err()
                            {
                                log::error!("Could not save annotations for {:?}", file);
                            }
                        } else {
                            log::warn!("save_row_level_data could not get file: {:?}", file);
                        }
                        bar.inc(1);
                    });
                }

                bar.finish();
                println!("Done.");
            }
            Err(e) => {
                log::error!("Could not save low level data: {e}");
            }
        }

        Ok(())
    }

    fn add_regular_staged_entry_to_db(
        &self,
        writer: &CommitDirEntryWriter,
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
        self.add_commit_entry(writer, &entry)?;
        Ok(())
    }

    fn add_commit_entry(
        &self,
        writer: &CommitDirEntryWriter,
        entry: &CommitEntry,
    ) -> Result<(), OxenError> {
        self.backup_file_to_versions_dir(entry)?;

        writer.add_commit_entry(entry)
    }

    fn backup_file_to_versions_dir(&self, new_entry: &CommitEntry) -> Result<(), OxenError> {
        let full_path = self.repository.path.join(&new_entry.path);
        // create a copy to our versions directory
        // .oxen/versions/ENTRY_HASH/COMMIT_ID.ext
        // where ENTRY_HASH is something like subdirs: 59/E029D4812AEBF0

        let versions_entry_path = util::fs::version_path(&self.repository, new_entry);
        let versions_entry_dir = versions_entry_path.parent().unwrap();

        // Create dir if not exists
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
            if util::fs::is_tabular(&full_path) {
                self.backup_to_arrow_file(new_entry, &full_path, &versions_entry_path)?;
            } else {
                std::fs::copy(full_path, versions_entry_path)?;
            }
        }

        Ok(())
    }

    fn backup_to_arrow_file(
        &self,
        entry: &CommitEntry,
        full_path: &Path,
        version_entry_path: &Path,
    ) -> Result<(), OxenError> {
        let df = tabular::copy_df(full_path, version_entry_path)?;
        let schema = schema::Schema::from_polars(df.schema());

        // Save the schema if it does not exist
        let schema_version_dir = util::fs::schema_version_dir(&self.repository, &schema);
        if !schema_version_dir.exists() {
            std::fs::create_dir_all(&schema_version_dir)?;
            let schema_writer = SchemaWriter::new(&self.repository, &entry.commit_id)?;
            schema_writer.put_schema(&schema)?;
        }

        Ok(())
    }

    pub fn commit_staged_entries(
        &self,
        commit: &Commit,
        staged_data: &StagedData,
    ) -> Result<(), OxenError> {
        self.commit_staged_entries_with_prog(commit, staged_data)
    }

    fn group_staged_files_to_dirs(
        &self,
        files: &HashMap<PathBuf, StagedEntry>,
        entry_type: EntryType,
    ) -> HashMap<PathBuf, Vec<(PathBuf, StagedEntry)>> {
        let mut results: HashMap<PathBuf, Vec<(PathBuf, StagedEntry)>> = HashMap::new();

        for (path, entry) in files.iter() {
            if entry.entry_type == entry_type {
                if let Some(parent) = path.parent() {
                    results
                        .entry(parent.to_path_buf())
                        .or_insert(vec![])
                        .push((path.clone(), entry.clone()));
                }
            }
        }

        results
    }

    fn group_annotations_to_dirs(
        &self,
        annotations: &HashMap<String, Vec2DStr>,
    ) -> HashMap<PathBuf, Vec<(PathBuf, Vec2DStr)>> {
        let mut results: HashMap<PathBuf, Vec<(PathBuf, Vec2DStr)>> = HashMap::new();

        for (file_str, entry) in annotations.iter() {
            let path = Path::new(file_str);
            if let Some(parent) = path.parent() {
                results
                    .entry(parent.to_path_buf())
                    .or_insert(vec![])
                    .push((path.to_path_buf(), entry.to_vec()));
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
        let regular = self.group_staged_files_to_dirs(&staged_data.added_files, EntryType::Regular);
        let tabular = self.group_staged_files_to_dirs(&staged_data.added_files, EntryType::Tabular);

        // Do regular befor tabular
        for grouped in vec![regular, tabular] {
            for (dir, files) in grouped.iter() {
                // Track the dir
                path_db::put(&self.dir_db, dir, &0)?;

                // Write entries per dir
                let entry_writer =
                    CommitDirEntryWriter::new(&self.repository, &self.commit_id, dir)?;

                // Commit entries data
                files.par_iter().for_each(|(path, entry)| {
                    self.commit_staged_entry(&entry_writer, commit, path, entry);
                    bar.inc(1);
                });
            }
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
