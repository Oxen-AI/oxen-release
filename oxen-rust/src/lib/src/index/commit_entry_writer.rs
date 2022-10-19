use crate::constants::{self, DEFAULT_BRANCH_NAME, HISTORY_DIR, VERSIONS_DIR};
use crate::db;
use crate::db::path_db;
use crate::error::OxenError;
use crate::index::{
    CommitDirEntryReader, CommitDirEntryWriter, CommitSchemaTableIndex, RefReader, RefWriter,
    SchemaWriter,
};
use crate::media::{tabular, tabular_datafusion, DFOpts};
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
        self.add_commit_entry(writer, new_commit, entry)?;
        Ok(())
    }

    fn add_commit_entry(
        &self,
        writer: &CommitDirEntryWriter,
        commit: &Commit,
        entry: CommitEntry,
    ) -> Result<(), OxenError> {
        let entry = self.backup_file_to_versions_dir(commit, entry)?;

        writer.add_commit_entry(&entry)
    }

    fn backup_file_to_versions_dir(
        &self,
        commit: &Commit,
        mut entry: CommitEntry,
    ) -> Result<CommitEntry, OxenError> {
        let full_path = self.repository.path.join(&entry.path);
        // create a copy to our versions directory
        // .oxen/versions/ENTRY_HASH/COMMIT_ID.ext
        // where ENTRY_HASH is something like subdirs: 59/E029D4812AEBF0

        let versions_entry_path = util::fs::version_path(&self.repository, &entry);
        let versions_entry_dir = versions_entry_path.parent().unwrap();

        // Create dir if not exists
        if !versions_entry_dir.exists() {
            // it's the first time
            log::debug!(
                "Creating version dir for file: {:?} -> {:?}",
                entry.path,
                versions_entry_dir
            );

            // Create version dir
            std::fs::create_dir_all(versions_entry_dir)?;
        }

        if !versions_entry_path.exists() {
            log::debug!(
                "Copying commit entry for file: {:?} -> {:?}",
                entry.path,
                versions_entry_path
            );
            if util::fs::is_tabular(&full_path) {
                entry =
                    self.backup_to_arrow_file(commit, entry, &full_path, &versions_entry_path)?;
            } else {
                std::fs::copy(full_path, versions_entry_path)?;
            }
        }

        Ok(entry)
    }

    fn backup_to_arrow_file(
        &self,
        commit: &Commit,
        entry: CommitEntry,
        full_path: &Path,
        version_entry_path: &Path,
    ) -> Result<CommitEntry, OxenError> {
        log::debug!("Backup to arrow {:?}", commit);
        std::fs::copy(full_path, version_entry_path)?;

        let opts = DFOpts::empty();
        let df = tabular::read_df(version_entry_path, &opts)?;
        let schema = schema::Schema::from_polars(df.schema());

        // Compute row level hashes and row num
        let df = tabular::df_hash_rows(df)?;
        let mut df = tabular::df_add_row_num(df)?;

        // Save the schema if it does not exist
        let schema_version_dir = util::fs::schema_version_dir(&self.repository, &schema);
        if !schema_version_dir.exists() {
            log::debug!("Create new schema! {:?}", schema);

            std::fs::create_dir_all(&schema_version_dir)?;
            let schema_writer = SchemaWriter::new(&self.repository, &entry.commit_id)?;
            schema_writer.put_schema(&schema)?;

            // save to first version of the big data.arrow file
            let path = util::fs::schema_df_path(&self.repository, &schema);
            tabular::write_df(&mut df, path)?;

            // Write the row_hash -> row_num index
            CommitSchemaTableIndex::index_hash_row_nums(
                self.repository.clone(),
                commit.clone(),
                schema,
                constants::COMMIT_INDEX_KEY.to_string(),
                df,
            )?;
        } else {
            log::debug!("Add to existing schema! {:?}", schema);
            // Get handle on the old DF
            let schema_df_path = util::fs::schema_df_path(&self.repository, &schema);
            let opts = DFOpts::empty();
            let old_df = tabular::read_df(&schema_df_path, &opts)?;

            log::debug!("OLD DF: {}", old_df);

            // TODO: we don't want to look for unique rows, because you could totally have multiple
            // people label the same image multiple times

            // I think we want...

            // - Diff the file against the last commit's version of that file (which we already do based on hash...)
            // - If two people modified the same file....we'll have to merge the changes
            // - So check if the file is tabular, and just merge the changes? I feel like there's no such thing as a modified row?

            // What you do care about is "who added what"?

            // I think we should add this to the arrow table in hidden columns
            //   _created_by
            //   _created_at

            // Add `oxen index <optional:COMMIT_ID> -n INDEX_NAME` command to view an index at a commit
            // Add `oxen index -c(reate) -n INDEX_NAME` command to create an index on a field name, simply scans and inserts row nums

            // TODO:
            // How do we merge this giant arrow file?
            // Ex)
            //   - Greg adds annotations in branch to schema
            //   - Josh adds annotations in branch to schema
            //   - We hash all the hashes, notice our schemas are out of sync
            //   - How do you merge? Fast Forward taking both?
            //       - Yes I think this is probably always the case...?
            //       - When is it not in a regular commit..? When we disagree on a line in a file, or we both modify a file.
            //       - We are saying there is no such thing in this land as just modifying a row, you have to delete and re-add?
            //       - ^^ Think this statement through with a concrete example

            // Create new DF from new rows
            // Loop over the hashes and filter to ones that do not exist
            let new_df = CommitSchemaTableIndex::compute_new_rows(
                self.repository.clone(),
                commit.clone(),
                schema.clone(),
                constants::COMMIT_INDEX_KEY.to_string(),
                df,
            )?;

            let start: u32 = old_df.height() as u32;
            let new_df = tabular::df_add_row_num_starting_at(new_df, start)?;
            log::debug!("NEW ROWS: {}", new_df);

            // append to big .arrow file with new indices that start at num_rows
            let mut full_df = old_df.vstack(&new_df).expect("could not vstack");
            // println!("TOTAL: {}", full_df);

            // write the new row hashes to index
            std::fs::remove_file(&schema_df_path)?;
            tabular::write_df(&mut full_df, schema_df_path)?;

            // Write the row_hash -> row_num index
            CommitSchemaTableIndex::index_hash_row_nums(
                self.repository.clone(),
                commit.clone(),
                schema,
                constants::COMMIT_INDEX_KEY.to_string(),
                new_df,
            )?;
        }

        Ok(entry)
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
