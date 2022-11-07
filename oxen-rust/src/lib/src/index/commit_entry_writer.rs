use crate::constants::{self, DEFAULT_BRANCH_NAME, HISTORY_DIR, VERSIONS_DIR};
use crate::db;
use crate::db::path_db;
use crate::error::OxenError;
use crate::index::{
    CommitDirEntryWriter, CommitDirReader, CommitSchemaRowIndex, RefReader, RefWriter, SchemaWriter,
};
use crate::media::{tabular, DFOpts};
// use crate::model::schema;
use crate::model::{
    schema, Commit, CommitEntry, LocalRepository, StagedData, StagedEntry, StagedEntryStatus,
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
        if util::fs::is_tabular(&entry.path) {
            // We compute the hash on tabular data different
            entry = self.compute_row_level_changes(commit, entry, &full_path)?;
        } else {
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
        }

        Ok(entry)
    }

    fn compute_row_level_changes(
        &self,
        commit: &Commit,
        mut entry: CommitEntry,
        full_path: &Path,
    ) -> Result<CommitEntry, OxenError> {
        log::debug!("Backup to arrow {:?}", commit);

        // Want to be able to commit all these changes in parallel so
        // We just hash the rows and project row numbers at this point

        let df = tabular::read_df(full_path, DFOpts::empty())?;

        // Compute row level hashes for this table
        println!("Committing {} rows...", df.height());
        let df = tabular::df_hash_rows(df)?;
        // Project row num as a col
        let mut df = tabular::df_add_row_num(df)?;

        // Hash is based off of row content, not the full file content
        let hash = util::hasher::compute_tabular_hash(&df);
        entry.hash = hash;

        let version_entry_path = util::fs::version_path(&self.repository, &entry);
        let version_dir = version_entry_path.parent().unwrap();
        if !version_dir.exists() {
            std::fs::create_dir_all(version_dir)?;
        }

        // Save off in a .arrow file we will aggregate and collect at the end of the commit
        // into the global .arrow file
        let hash_results_file = version_dir.join("data.arrow");
        tabular::write_df(&mut df, hash_results_file)?;

        Ok(entry)
    }

    pub fn commit_staged_entries(
        &self,
        commit: &Commit,
        staged_data: &StagedData,
    ) -> Result<(), OxenError> {
        self.commit_staged_entries_with_prog(commit, staged_data)?;
        self.aggregate_row_level_results(false)
    }

    pub fn aggregate_row_level_results(
        &self,
        should_copy_to_working_dir: bool,
    ) -> Result<(), OxenError> {
        let commit_dir_reader = CommitDirReader::new(&self.repository, &self.commit)?;

        let tabular_entries: Vec<CommitEntry> = commit_dir_reader
            .list_entries()?
            .into_iter()
            .filter(|e| util::fs::is_tabular(&e.path))
            .collect();

        log::debug!(
            "aggregate_row_level_results got {} tabular entries",
            tabular_entries.len()
        );
        // TODO: should probably group based on schema, and just do one big write at the end,
        // but this works for now
        for entry in tabular_entries.iter() {
            log::debug!("Merging tabular entry {:?}", entry.path);
            // Only merge newly added files, it's only newly added if it has this data.arrow file
            let version_dir = util::fs::version_dir_from_hash(&self.repository, entry.hash.clone());
            let hash_results_file = version_dir.join("data.arrow");
            if !hash_results_file.exists() {
                log::debug!("No tmp data.arrow file for entry {:?}", entry.path);
                continue;
            }

            let full_path = &self.repository.path.join(&entry.path);
            if should_copy_to_working_dir {
                // TODO: reading data many times, probably shouldn't...?
                let mut df = tabular::read_df(&hash_results_file, DFOpts::empty())?;
                // Need to restore parent dir
                if let Some(parent) = full_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                log::debug!("Restoring path {:?}", entry.path);
                tabular::write_df(&mut df, full_path)?;
            }

            // TODO: should only read data once and filter to get schema, we're reading many times...
            let df = tabular::read_df(full_path, DFOpts::empty())?;

            let schema = schema::Schema::from_polars(&df.schema());
            log::debug!("aggregate_row_level_results got OG DF {}", df);

            // This is another read, just want to make sure this all works first
            let mut df = tabular::read_df(&hash_results_file, DFOpts::empty())?;

            // After we've read this data.arrow file we should clean it up
            // since all the data will be copied into the master schema/data.arrow file
            std::fs::remove_file(hash_results_file)?;

            log::debug!("Add to existing schema! {:?}", schema);
            // Get handle on the full data.arrow
            let schema_df_path = util::fs::schema_df_path(&self.repository, &schema);

            let schema_version_dir = util::fs::schema_version_dir(&self.repository, &schema);
            if !schema_version_dir.exists() {
                log::debug!("Create new schema! {:?}", schema);

                std::fs::create_dir_all(&schema_version_dir)?;
                let schema_writer = SchemaWriter::new(&self.repository, &entry.commit_id)?;
                schema_writer.put_schema(&schema)?;
                schema_writer.put_schema_for_file(&entry.path, &schema)?;

                // save to first version of the big data.arrow file
                tabular::write_df(&mut df, &schema_df_path)?;

                // Write the row_hash -> row_num index
                println!("Creating index for {} rows...", df.height());
                CommitSchemaRowIndex::index_hash_row_nums(
                    self.repository.clone(),
                    self.commit.clone(),
                    schema.clone(),
                    entry.path.to_path_buf(),
                    df,
                )?;

                // TODO: double read again, fix....
                let df = tabular::read_df(&schema_df_path, DFOpts::empty())?;
                let old_df = tabular::read_df(&schema_df_path, DFOpts::empty())?;

                // Need to save off indices too
                println!("Saving index...");
                CommitSchemaRowIndex::compute_new_rows(
                    self.repository.clone(),
                    self.commit.clone(),
                    schema.clone(),
                    entry.clone(),
                    df,
                    &old_df,
                )?;
            } else {
                let old_df = tabular::read_df(&schema_df_path, DFOpts::empty())?;

                log::debug!("OLD DF: {}", old_df);

                // Create new DF from new rows
                // Loop over the hashes and filter to ones that do not exist
                println!("Computing new rows...");
                let new_df = CommitSchemaRowIndex::compute_new_rows(
                    self.repository.clone(),
                    self.commit.clone(),
                    schema.clone(),
                    entry.clone(),
                    df,
                    &old_df,
                )?;

                let start: u32 = old_df.height() as u32;
                let new_df = tabular::df_add_row_num_starting_at(new_df, start)?;
                log::debug!("NEW ROWS: {}", new_df);

                // append to big .arrow file with new indices that start at num_rows
                let mut full_df = old_df.vstack(&new_df).expect("could not vstack");
                // println!("TOTAL: {}", full_df);

                // write the new row hashes to index
                println!("Updating index for {} rows...", full_df.height());
                std::fs::remove_file(&schema_df_path)?;
                tabular::write_df(&mut full_df, schema_df_path)?;

                // Write the row_hash -> row_num index
                CommitSchemaRowIndex::index_hash_row_nums(
                    self.repository.clone(),
                    self.commit.clone(),
                    schema,
                    entry.path.to_path_buf(),
                    full_df,
                )?;
            }
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

    // fn group_annotations_to_dirs(
    //     &self,
    //     annotations: &HashMap<String, Vec2DStr>,
    // ) -> HashMap<PathBuf, Vec<(PathBuf, Vec2DStr)>> {
    //     let mut results: HashMap<PathBuf, Vec<(PathBuf, Vec2DStr)>> = HashMap::new();

    //     for (file_str, entry) in annotations.iter() {
    //         let path = Path::new(file_str);
    //         if let Some(parent) = path.parent() {
    //             results
    //                 .entry(parent.to_path_buf())
    //                 .or_insert(vec![])
    //                 .push((path.to_path_buf(), entry.to_vec()));
    //         }
    //     }

    //     results
    // }

    fn commit_staged_entries_with_prog(
        &self,
        commit: &Commit,
        staged_data: &StagedData,
    ) -> Result<(), OxenError> {
        let size: u64 = unsafe { std::mem::transmute(staged_data.added_files.len()) };
        let bar = ProgressBar::new(size);
        let grouped = self.group_staged_files_to_dirs(&staged_data.added_files);

        // Do regular befor tabular
        for (dir, files) in grouped.iter() {
            // Track the dir
            path_db::put(&self.dir_db, dir, &0)?;

            // Write entries per dir
            let entry_writer = CommitDirEntryWriter::new(&self.repository, &self.commit.id, dir)?;

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
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::media::tabular;
    use crate::media::DFOpts;
    use crate::test;
    use crate::util;

    use std::path::{Path, PathBuf};

    #[test]
    fn test_commit_tabular_data_first_time() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);

            let og_df = tabular::read_df(&bbox_path, DFOpts::empty())?;
            command::add(&repo, &bbox_path)?;
            let commit = command::commit(&repo, "Committing bbox data")?.unwrap();

            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas.first().unwrap();

            let path = util::fs::schema_df_path(&repo, schema);
            assert!(path.exists());

            let version_df = tabular::read_df(path, DFOpts::empty())?;
            assert_eq!(og_df.height(), version_df.height());

            // Adding _row_num and _row_hash
            assert_eq!(og_df.width() + 2, version_df.width());

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_data_add_data_same_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);

            // Add a row to the data (should already have been committed once since run_training_data_repo_test_fully_committed)
            let mut opts = DFOpts::empty();
            opts.add_row = Some(String::from("train/new.jpg,1.0,2.0,3,4"));
            opts.output = Some(PathBuf::from(&bbox_path));
            command::df(&bbox_path, opts)?;

            let og_df = tabular::read_df(&bbox_path, DFOpts::empty())?;
            command::add(&repo, &bbox_path)?;
            let commit = command::commit(&repo, "Committing bbox data")?.unwrap();

            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas.first().unwrap();

            let path = util::fs::schema_df_path(&repo, schema);
            assert!(path.exists());

            let version_df = tabular::read_df(path, DFOpts::empty())?;
            assert_eq!(og_df.height(), version_df.height());

            // Adding _row_num and _row_hash
            assert_eq!(og_df.width() + 2, version_df.width());

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_data_add_data_different_file_same_schema() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Create a new data file with some annotations
            let og_bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let og_bbox_path = repo.path.join(&og_bbox_file);
            let my_bbox_file = Path::new("annotations")
                .join("train")
                .join("my_bounding_box.csv");
            let my_bbox_path = repo.path.join(&my_bbox_file);
            test::write_txt_file_to_path(
                &my_bbox_path,
                r#"
file,min_x,min_y,width,height
train/new.jpg,1.0,2.0,3,4
train/new.jpg,5.0,6.0,7,8
"#,
            )?;

            let og_df = tabular::read_df(&og_bbox_path, DFOpts::empty())?;
            let my_df = tabular::read_df(&my_bbox_path, DFOpts::empty())?;
            command::add(&repo, &my_bbox_path)?;
            let commit =
                command::commit(&repo, "Committing my bbox data, to append onto og data")?.unwrap();

            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas.first().unwrap();

            let path = util::fs::schema_df_path(&repo, schema);
            assert!(path.exists());

            let version_df = tabular::read_df(path, DFOpts::empty())?;
            assert_eq!(og_df.height() + my_df.height(), version_df.height());

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_data_add_data_different_file_same_schema_duplicate_content(
    ) -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Create a new data file with some annotations
            let og_bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let og_bbox_path = repo.path.join(&og_bbox_file);
            let my_bbox_file = Path::new("annotations")
                .join("train")
                .join("my_bounding_box.csv");
            let my_bbox_path = repo.path.join(&my_bbox_file);
            // This is the same row content that already exists, so we shouldn't add it again to the version file
            test::write_txt_file_to_path(
                &my_bbox_path,
                r#"
file,min_x,min_y,width,height
train/dog_1.jpg,101.5,32.0,385,330
train/dog_2.jpg,7.0,29.5,246,247
"#,
            )?;

            let og_df = tabular::read_df(&og_bbox_path, DFOpts::empty())?;
            command::add(&repo, &my_bbox_path)?;
            let commit =
                command::commit(&repo, "Committing my bbox data, to append onto og data")?.unwrap();

            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas.first().unwrap();

            let path = util::fs::schema_df_path(&repo, schema);
            assert!(path.exists());

            let version_df = tabular::read_df(path, DFOpts::empty())?;
            assert_eq!(og_df.height(), version_df.height());

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_data_commit_many_tabular_files_same_schema() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Create a new data file with some annotations
            let og_bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let og_bbox_path = repo.path.join(&og_bbox_file);
            let og_df = tabular::read_df(&og_bbox_path, DFOpts::empty())?;

            // Add many new bbox files
            let num_files = 10;
            let num_new_rows_per_file = 100;
            for i in 0..num_files {
                let bbox_name = format!("my_bounding_box_{}.csv", i);
                let my_bbox_file = Path::new("annotations").join("train").join(bbox_name);
                let mut my_bbox_path = repo.path.join(&my_bbox_file);
                // This is the same row content that already exists, so we shouldn't add it again to the version file
                test::write_txt_file_to_path(
                    &my_bbox_path,
                    r#"
file,min_x,min_y,width,height
train/dog_1.jpg,101.5,32.0,385,330
train/dog_2.jpg,7.0,29.5,246,247
"#,
                )?;

                // Add random extra rows to each file
                for _ in 0..num_new_rows_per_file {
                    my_bbox_path = test::add_random_bbox_to_file(my_bbox_path)?;
                }

                // Stage the file
                command::add(&repo, &my_bbox_path)?;
            }

            // Commit all the new bbox files
            let commit =
                command::commit(&repo, "Committing my bbox data, to append onto og data")?.unwrap();

            let schemas = command::schema_list(&repo, Some(&commit.id))?;
            let schema = schemas.first().unwrap();

            let path = util::fs::schema_df_path(&repo, schema);
            assert!(path.exists());

            let version_df = tabular::read_df(path, DFOpts::empty())?;
            assert_eq!(
                og_df.height() + (num_files * num_new_rows_per_file),
                version_df.height()
            );

            Ok(())
        })
    }
}
