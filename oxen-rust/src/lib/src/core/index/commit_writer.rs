use crate::config::UserConfig;
use crate::constants::{COMMITS_DIR, MERGE_HEAD_FILE, ORIG_HEAD_FILE};
use crate::core::db::path_db;
use crate::core::df::tabular;
use crate::core::index::{
    self, mod_stager, remote_dir_stager, CommitDBReader, CommitDirEntryReader,
    CommitDirEntryWriter, CommitEntryReader, CommitEntryWriter, EntryIndexer, RefReader, RefWriter, ObjectDBReader,
};
use crate::core::{db, df};
use crate::error::OxenError;
use crate::model::{Branch, Commit, CommitEntry, NewCommit, Schema, StagedData, StagedEntry};
use crate::opts::DFOpts;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{command, util};

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str;
use time::OffsetDateTime;

use crate::model::LocalRepository;

pub struct CommitWriter {
    pub commits_db: DBWithThreadMode<MultiThreaded>,
    repository: LocalRepository,
}

impl CommitWriter {
    pub fn commit_db_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(COMMITS_DIR))
    }

    pub fn new(repository: &LocalRepository) -> Result<CommitWriter, OxenError> {
        let db_path = CommitWriter::commit_db_dir(&repository.path);

        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }

        let opts = db::opts::default();
        Ok(CommitWriter {
            commits_db: DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?,
            repository: repository.clone(),
        })
    }

    fn create_new_commit_data(&self, message: &str) -> Result<NewCommit, OxenError> {
        let cfg = UserConfig::get()?;
        let timestamp = OffsetDateTime::now_utc();
        let ref_reader = RefReader::new(&self.repository)?;
        // Commit
        //  - parent_ids (can be empty if root)
        //  - message
        //  - date
        //  - author
        match ref_reader.head_commit_id() {
            Ok(Some(parent_id)) => {
                // We might be in a merge commit, in which case we would have multiple parents
                if self.is_merge_commit() {
                    log::debug!("Create merge commit...");
                    self.create_merge_commit(message)
                } else {
                    // We have one parent
                    log::debug!("Create commit with parent {:?}", parent_id);
                    Ok(NewCommit {
                        parent_ids: vec![parent_id],
                        message: String::from(message),
                        author: cfg.name,
                        email: cfg.email,
                        timestamp,
                    })
                }
            }
            _ => {
                // We are creating initial commit, no parents
                log::debug!("Create initial commit...");
                Ok(NewCommit {
                    parent_ids: vec![],
                    message: String::from(message),
                    author: cfg.name,
                    email: cfg.email,
                    timestamp,
                })
            }
        }
    }

    // Reads commit ids from merge commit files then removes them
    fn create_merge_commit(&self, message: &str) -> Result<NewCommit, OxenError> {
        let cfg = UserConfig::get()?;
        let timestamp = OffsetDateTime::now_utc();
        let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
        let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
        let orig_head_path = hidden_dir.join(ORIG_HEAD_FILE);

        // Read parent commit ids
        let merge_commit_id = util::fs::read_from_path(&merge_head_path)?;
        let head_commit_id = util::fs::read_from_path(&orig_head_path)?;

        // Cleanup
        util::fs::remove_file(merge_head_path)?;
        util::fs::remove_file(orig_head_path)?;

        Ok(NewCommit {
            parent_ids: vec![merge_commit_id, head_commit_id],
            message: String::from(message),
            author: cfg.name,
            email: cfg.email,
            timestamp,
        })
    }

    fn is_merge_commit(&self) -> bool {
        let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
        let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
        merge_head_path.exists()
    }

    // Create a db in the history/ dir under the id
    // We will have something like:
    // history/
    //   d7966d81ab35ffdf/
    //     annotations.txt -> b"{entry_json}"
    //     train/
    //       image_1.png -> b"{entry_json}"
    //       image_2.png -> b"{entry_json}"
    //       image_2.png -> b"{entry_json}"
    pub fn commit(&self, status: &StagedData, message: &str) -> Result<Commit, OxenError> {
        // Create a commit object, that either points to parent or not
        // must create this before anything else so that we know if it has parent or not.
        log::debug!("---COMMIT START---"); // for debug logging / timing purposes
        let new_commit = self.create_new_commit_data(message)?;
        log::debug!("Created commit obj {:?}", new_commit);
        let commit = self.commit_from_new(&new_commit, status, &self.repository.path)?;
        log::debug!("COMMIT_COMPLETE {} -> {}", commit.id, commit.message);

        // Mark as synced so we know we don't need to pull versions files again
        index::commit_sync_status::mark_commit_as_synced(&self.repository, &commit)?;

        // User output
        println!("Commit {} done.", commit.id);
        log::debug!("---COMMIT END {} -> '{}'---", commit.id, message); // for debug logging / timing purposes
        Ok(commit)
    }

    pub fn commit_from_new(
        &self,
        new_commit: &NewCommit,
        status: &StagedData,
        origin_path: &Path,
    ) -> Result<Commit, OxenError> {
        let commit = self.gen_commit(new_commit, status);
        log::debug!(
            "commit_from_new commit Id computed {} -> [{}]",
            commit.id,
            commit.message
        );

        self.add_commit_from_status(&commit, status, origin_path)?;

        Ok(commit)
    }

    pub fn commit_from_new_on_remote_branch(
        &self,
        new_commit: &NewCommit,
        status: &StagedData,
        origin_path: &Path,
        branch: &Branch,
        user_id: &str,
    ) -> Result<Commit, OxenError> {
        let commit = self.gen_commit(new_commit, status);

        let entries = mod_stager::list_mod_entries(&self.repository, branch, user_id)?;
        let object_reader = ObjectDBReader::new(&self.repository)?;
        let commit_entry_reader =
            CommitEntryReader::new_from_commit_id(&self.repository, &branch.commit_id, object_reader)?;
        // TODO: this is not the most efficient, but easiest way right now
        //       might want to make helper for dir entry reader
        let entries: Vec<CommitEntry> = entries
            .iter()
            .map(|path| commit_entry_reader.get_entry(path).unwrap().unwrap())
            .collect();

        log::debug!(
            "commit_from_new listing entries {} -> {}",
            commit.id,
            entries.len()
        );
        let staged = self.apply_mods(branch, user_id, &entries)?;
        // Write entries
        self.add_commit_from_status_on_remote_branch(&commit, &staged, origin_path, branch)?;
        Ok(commit)
    }

    pub fn apply_mods(
        &self,
        branch: &Branch,
        user_id: &str,
        entries: &Vec<CommitEntry>,
    ) -> Result<StagedData, OxenError> {
        let branch_staging_dir =
            remote_dir_stager::branch_staging_dir(&self.repository, branch, user_id);
        let branch_repo =
            remote_dir_stager::init_or_get(&self.repository, branch, user_id).unwrap();

        log::debug!("apply_mods CommitWriter Apply {} mods", entries.len());
        for entry in entries.iter() {
            // Copy the version file to the staging dir and make the mods
            let version_path = util::fs::version_path(&self.repository, entry);
            let entry_path = branch_staging_dir.join(&entry.path);
            if let Some(parent) = entry_path.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                }
            }

            log::debug!(
                "apply_mods Copy file to mod {:?} -> {:?}",
                version_path,
                entry_path
            );
            util::fs::copy(&version_path, &entry_path)?;

            self.apply_mods_to_file(branch, user_id, entry, &entry_path)?;
            remote_dir_stager::stage_file(
                &self.repository,
                &branch_repo,
                branch,
                user_id,
                &entry_path,
            )?;
        }

        // Have to recompute staged data
        let staged_data = command::status(&branch_repo)?;
        log::debug!("APPLY MODS got new staged_data");
        staged_data.print_stdout();

        Ok(staged_data)
    }

    fn apply_mods_to_file(
        &self,
        branch: &Branch,
        user_id: &str,
        entry: &CommitEntry,
        path: &Path,
    ) -> Result<String, OxenError> {
        log::debug!(
            "apply_mods_to_file [{}] {:?} -> {:?}",
            branch.name,
            entry.path,
            path
        );
        if util::fs::is_tabular(path) {
            self.apply_tabular_mods(branch, user_id, entry, path)
        } else {
            Err(OxenError::basic_str(
                "File type not supported for modifications",
            ))
        }
    }

    fn apply_tabular_mods(
        &self,
        branch: &Branch,
        user_id: &str,
        entry: &CommitEntry,
        path: &Path,
    ) -> Result<String, OxenError> {
        let mut df = df::tabular::read_df(path, DFOpts::empty())?;
        let schema_fields = Schema::from_polars(&df.schema()).fields_names().join(",");
        let mods_df = mod_stager::list_mods_df(&self.repository, branch, user_id, entry)?;
        log::debug!("apply_tabular_mods [{}] {:?}", branch.name, path);
        log::debug!("og df {:?}", df);
        if let Some(rows) = mods_df.added_rows {
            // TODO: More robust check for schema match
            if rows.width() != df.width() + 1 {
                return Err(OxenError::basic_str(format!(
                    "Could not commit, schema has changed on file {}",
                    entry.path.to_string_lossy()
                )));
            }

            let mut df_opts = DFOpts::empty();
            df_opts.columns = Some(schema_fields);
            let filtered = tabular::transform(rows, df_opts)?;
            log::debug!("Add filtered {}", filtered);
            df = df.vstack(&filtered).unwrap();
        }
        log::debug!("Full DF {}", df);
        df::tabular::write_df(&mut df, path)?;
        let new_hash = util::hasher::hash_file_contents(path)?;
        Ok(new_hash)
    }

    fn gen_commit(&self, commit_data: &NewCommit, status: &StagedData) -> Commit {
        log::debug!("gen_commit from {} files", status.staged_files.len());
        let entries: Vec<StagedEntry> = status.staged_files.values().cloned().collect();
        let id = util::hasher::compute_commit_hash(commit_data, &entries);
        log::debug!("gen_commit id {}", id);
        Commit::from_new_and_id(commit_data, id)
    }

    pub fn commit_with_parent_ids(
        &self,
        status: &StagedData,
        parent_ids: Vec<String>,
        message: &str,
    ) -> Result<Commit, OxenError> {
        let cfg = UserConfig::get()?;
        let timestamp = OffsetDateTime::now_utc();

        let commit = NewCommit {
            parent_ids,
            message: String::from(message),
            author: cfg.name,
            email: cfg.email,
            timestamp,
        };
        let entries: Vec<StagedEntry> = status.staged_files.values().cloned().collect();
        let id = util::hasher::compute_commit_hash(&commit, &entries);
        let commit = Commit::from_new_and_id(&commit, id);
        self.add_commit_from_status(&commit, status, &self.repository.path)?;
        Ok(commit)
    }

    pub fn add_commit_from_empty_status(&self, commit: &Commit) -> Result<(), OxenError> {
        // Empty Status
        let status = StagedData::empty();
        self.add_commit_from_status(commit, &status, &self.repository.path)
    }

    pub fn add_commit_from_status(
        &self,
        commit: &Commit,
        status: &StagedData,
        origin_path: &Path,
    ) -> Result<(), OxenError> {
        // Write entries
        let entry_writer = CommitEntryWriter::new(&self.repository, commit)?;

        // Commit all staged files from db
        entry_writer.commit_staged_entries(commit, status, origin_path)?;

        // Add to commits db id -> commit_json
        log::debug!("add_commit_from_status add commit [{}] to db", commit.id);

        // TODONOW: THIS IS A BIG BAD HACK, FIX LATER 

        let mut commit = commit.clone();
        let temp_commit_hashes_db = CommitEntryWriter::temp_commit_hashes_db_dir(&self.repository);
        let opts = db::opts::default();
        let temp_commit_hashes_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only( &opts, temp_commit_hashes_db, false)?;

        // Get the hash for this commit id 
        let hash: String = path_db::get_entry(&temp_commit_hashes_db, &commit.id)?.unwrap();
        commit.update_root_hash(hash.clone());


        log::debug!("commit we're on right now {:?}", commit);
        log::debug!("got hash {} for commit {}", hash, commit.message);


        self.add_commit_to_db(&commit)?;

        let ref_writer = RefWriter::new(&self.repository)?;
        ref_writer.set_head_commit_id(&commit.id)?;

        Ok(())
    }

    pub fn add_commit_from_status_on_remote_branch(
        &self,
        commit: &Commit,
        status: &StagedData,
        origin_path: &Path,
        branch: &Branch,
    ) -> Result<(), OxenError> {
        // Write entries
        let entry_writer = CommitEntryWriter::new(&self.repository, commit)?;

        log::debug!("add_commit_from_status about to commit staged entries...");
        // Commit all staged files from db
        entry_writer.commit_staged_entries(commit, status, origin_path)?;

        let mut commit = commit.clone();
        let temp_commit_hashes_db = CommitEntryWriter::temp_commit_hashes_db_dir(&self.repository);
        let opts = db::opts::default();
        let temp_commit_hashes_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only( &opts, temp_commit_hashes_db, false)?;

        // Get the hash for this commit id 
        let hash: String = path_db::get_entry(&temp_commit_hashes_db, &commit.id)?.unwrap();
        commit.update_root_hash(hash.clone());

        log::debug!("got hash {} for commit {}", hash, commit.id);

        // Add to commits db id -> commit_json
        log::debug!("add_commit_from_status add commit [{}] to db", commit.id);
        self.add_commit_to_db(&commit)?;

        let ref_writer = RefWriter::new(&self.repository)?;
        log::debug!(
            "add_commit_from_status got branch {} updating branch commit id {}",
            branch.name,
            commit.id
        );
        ref_writer.set_branch_commit_id(&branch.name, &commit.id)?;

        Ok(())
    }

    pub fn add_commit_on_remote_branch(
        &self,
        commit: &Commit,
        status: &StagedData,
        origin_path: &Path,
        branch: &Branch,
    ) -> Result<(), OxenError> {
        // Write entries
        let entry_writer = CommitEntryWriter::new(&self.repository, commit)?;

        log::debug!("add_commit_from_status about to commit staged entries...");
        // Commit all staged files from db
        entry_writer.commit_staged_entries(commit, status, origin_path)?;


        let mut commit = commit.clone();
        let temp_commit_hashes_db = CommitEntryWriter::temp_commit_hashes_db_dir(&self.repository);
        let opts = db::opts::default();
        let temp_commit_hashes_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only( &opts, temp_commit_hashes_db, false)?;

        // Get the hash for this commit id 
        let hash: String = path_db::get_entry(&temp_commit_hashes_db, &commit.id)?.unwrap();
        commit.update_root_hash(hash.clone());

        log::debug!("got hash {} for commit {}", hash, commit.id);

        // Add to commits db id -> commit_json
        log::debug!("add_commit_from_status add commit [{}] to db", commit.id);
        self.add_commit_to_db(&commit)?;

        let ref_writer = RefWriter::new(&self.repository)?;
        log::debug!(
            "add_commit_from_status got branch {} updating branch commit id {}",
            branch.name,
            commit.id
        );
        ref_writer.set_branch_commit_id(&branch.name, &commit.id)?;

        Ok(())
    }

    pub fn add_commit_to_db(&self, commit: &Commit) -> Result<(), OxenError> {
        // Write commit json to db
        let commit_json = serde_json::to_string(&commit)?;
        log::debug!("add_commit_to_db [{}] -> {}", commit.id, commit_json);
        self.commits_db.put(&commit.id, commit_json.as_bytes())?;
        Ok(())
    }

    // TODO: rethink this logic and make it cleaner
    pub async fn set_working_repo_to_commit(&self, commit: &Commit) -> Result<(), OxenError> {
        let head_commit = CommitDBReader::head_commit(&self.repository, &self.commits_db)?;
        if head_commit.id == commit.id {
            log::debug!(
                "set_working_repo_to_commit_id, do nothing... head commit == commit_id {}",
                commit.id
            );

            // Don't do anything if we tried to switch to same commit
            return Ok(());
        }

        // Iterate over files in that are in *current head* and make sure they should all be there
        // if they aren't in commit db we are switching to, remove them
        // Safe to unwrap because we check if it exists above
        log::debug!(
            "set_working_repo_to_commit_id: Commit: {} => '{}'",
            commit.id,
            commit.message
        );

        // Two readers, one for HEAD and one for this current commit
        let head_entry_reader = CommitEntryReader::new_from_head(&self.repository)?;
        let head_entries = head_entry_reader.list_files()?;
        log::debug!(
            "set_working_repo_to_commit_id got {} entries in commit",
            head_entries.len()
        );

        // Keep track of directories, since we do not explicitly store which ones are tracked...
        // we will remove them later if no files exist in them.
        let mut candidate_dirs_to_rm = self.cleanup_removed_files(&commit.id, &head_entries)?;

        log::debug!("Setting working directory to {}", commit.id);
        log::debug!("got {} candidate dirs", candidate_dirs_to_rm.len());

        // If we do not have the commit entry db locally, fetch it
        let path = CommitEntryReader::db_path(&self.repository.path, &commit.id);
        if !path.exists() {
            log::debug!(
                "set_working_repo_to_commit_id: Commit entry db does not exist, fetching..."
            );

            // Pull the commit entry db and the data objects
            let indexer = EntryIndexer::new(&self.repository)?;
            indexer.pull_commit(commit).await?;
        }

        // Iterate over files in current commit db, and make sure the hashes match,
        // if different, copy the correct version over
        let commit_entry_reader = CommitEntryReader::new(&self.repository, commit)?;
        let commit_entries = commit_entry_reader.list_entries()?;

        self.restore_missing_files(&commit.id, &commit_entries, &mut candidate_dirs_to_rm)?;

        log::debug!("candidate_dirs_to_rm {}", candidate_dirs_to_rm.len());
        if !candidate_dirs_to_rm.is_empty() {
            println!("Cleaning up...");
        }

        // Remove un-tracked directories
        for dir in candidate_dirs_to_rm.iter() {
            let full_dir = self.repository.path.join(dir);
            log::debug!(
                "set_working_repo_to_commit_id remove dis dir {:?}",
                full_dir
            );
            if full_dir.exists() {
                util::fs::remove_dir_all(full_dir)?;
            }
        }

        Ok(())
    }

    fn restore_missing_files(
        &self,
        commit_id: &str,
        entries: &[CommitEntry],
        candidate_dirs_to_rm: &mut HashSet<PathBuf>,
    ) -> Result<(), OxenError> {
        println!("Setting working directory to {commit_id}");
        let size: u64 = unsafe { std::mem::transmute(entries.len()) };
        let bar = oxen_progress_bar(size, ProgressBarType::Counter);

        let dir_entries = self.group_entries_to_dirs(entries);

        for (dir, entries) in dir_entries.iter() {
            let committer = CommitDirEntryWriter::new(&self.repository, commit_id, dir)?;
            for entry in entries.iter() {
                bar.inc(1);
                let path = &entry.path;
                log::debug!("Checking committed entry: {:?} => {:?}", path, entry);

                let dst_path = self.repository.path.join(path);
                let version_path = util::fs::version_path(&self.repository, entry);

                // If we do not have the file, restore it from our versioned history
                if !dst_path.exists() {
                    log::debug!(
                        "set_working_repo_to_commit_id restore file [{:?}] she new 🙏 {:?} -> {:?}",
                        entry.path,
                        version_path,
                        dst_path
                    );

                    // mkdir if not exists for the parent
                    if let Some(parent) = dst_path.parent() {
                        if !parent.exists() {
                            match std::fs::create_dir_all(parent) {
                                Ok(_) => {}
                                Err(err) => {
                                    log::error!("Error creating directory: {}", err);
                                }
                            }
                        }
                    }

                    match index::restore::restore_file_with_commit_writer(
                        &self.repository,
                        &entry.path,
                        entry,
                        &committer,
                    ) {
                        Ok(_) => {}
                        Err(err) => {
                            log::error!("Error restoring file: {}", err);
                        }
                    }
                } else {
                    // we do have it, check if we need to update it
                    let dst_hash =
                        util::hasher::hash_file_contents(&dst_path).expect("Could not hash file");

                    // let old_contents = util::fs::read_from_path(&version_path)?;
                    // let current_contents = util::fs::read_from_path(&dst_path)?;
                    // log::debug!("old_contents {:?}\n{}", version_path, old_contents);
                    // log::debug!("current_contents {:?}\n{}", dst_path, current_contents);

                    // If the hash of the file from the commit is different than the one on disk, update it
                    if entry.hash != dst_hash {
                        // we need to update working dir
                        log::debug!(
                            "set_working_repo_to_commit_id restore file diff hash 🙏 {:?} -> {:?}",
                            version_path,
                            dst_path
                        );

                        match index::restore::restore_file_with_commit_writer(
                            &self.repository,
                            &entry.path,
                            entry,
                            &committer,
                        ) {
                            Ok(_) => {}
                            Err(err) => {
                                log::error!("Error restoring file: {}", err);
                            }
                        }
                    } else {
                        log::debug!(
                            "set_working_repo_to_commit_id hashes match! {:?} -> {:?}",
                            version_path,
                            dst_path
                        );
                    }
                }

                if let Some(parent) = path.parent() {
                    // Check if parent directory exists, if it does, we no longer have
                    // it as a candidate to remove
                    if candidate_dirs_to_rm.contains(parent) {
                        log::debug!("We aren't going to delete candidate {:?}", parent);
                        candidate_dirs_to_rm.remove(parent);
                    }
                }
            }
        }
        bar.finish();
        Ok(())
    }

    fn cleanup_removed_files(
        &self,
        commit_id: &str,
        paths: &[PathBuf],
    ) -> Result<HashSet<PathBuf>, OxenError> {
        println!("Checking index...");
        let size: u64 = unsafe { std::mem::transmute(paths.len()) };
        let bar = oxen_progress_bar(size, ProgressBarType::Counter);

        let mut candidate_dirs_to_rm: HashSet<PathBuf> = HashSet::new();

        let dirs_to_paths = self.group_paths_to_dirs(paths);

        let object_reader = ObjectDBReader::new(&self.repository)?;

        for (dir, paths) in dirs_to_paths.iter() {
            let entry_reader = CommitDirEntryReader::new(&self.repository, commit_id, dir, object_reader.clone())?;
            for path in paths.iter() {
                let full_path = self.repository.path.join(path);
                log::debug!(
                    "set_working_repo_to_commit_id commit_entries[{:?}]",
                    full_path
                );
                if full_path.is_file() {
                    log::debug!("set_working_repo_to_commit_id path {:?}", path);

                    // TODO: Why are we doing...parent.parent here?
                    // Keep track of parents to see if we clear them
                    if let Some(parent) = path.parent() {
                        log::debug!("adding candidate dir {:?}", parent);

                        if parent.parent().is_some() {
                            // only add one directory below top level
                            // println!("set_working_repo_to_commit_id candidate dir {:?}", parent);
                            candidate_dirs_to_rm.insert(parent.to_path_buf());
                        }
                    }

                    if entry_reader.has_file(path) {
                        // We already have file ✅
                        log::debug!(
                            "set_working_repo_to_commit_id we already have file ✅ {:?}",
                            full_path
                        );
                    } else {
                        // sorry, we don't know you, bye
                        log::debug!("set_working_repo_to_commit_id see ya 💀 {:?}", full_path);
                        if full_path.exists() {
                            util::fs::remove_file(full_path)?;
                        }
                    }
                }
                bar.inc(1);
            }
        }
        bar.finish();

        Ok(candidate_dirs_to_rm)
    }

    fn group_entries_to_dirs(&self, entries: &[CommitEntry]) -> HashMap<PathBuf, Vec<CommitEntry>> {
        let mut results: HashMap<PathBuf, Vec<CommitEntry>> = HashMap::new();

        for entry in entries.iter() {
            if let Some(parent) = entry.path.parent() {
                results
                    .entry(parent.to_path_buf())
                    .or_default()
                    .push(entry.to_owned());
            }
        }

        results
    }

    fn group_paths_to_dirs(&self, files: &[PathBuf]) -> HashMap<PathBuf, Vec<PathBuf>> {
        let mut results: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();

        for path in files.iter() {
            if let Some(parent) = path.parent() {
                results
                    .entry(parent.to_path_buf())
                    .or_default()
                    .push(path.to_owned());
            }
        }

        results
    }

    pub fn get_commit_by_id(&self, commit_id: &str) -> Result<Option<Commit>, OxenError> {
        // Check if the id is in the DB
        let key = commit_id.as_bytes();
        match self.commits_db.get(key) {
            Ok(Some(value)) => {
                let commit: Commit = serde_json::from_str(str::from_utf8(&value)?)?;
                Ok(Some(commit))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!("Error commits_db to find commit_id {commit_id:?}\nErr: {err}");
                Err(OxenError::basic_str(err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::config::UserConfig;
    use crate::core::df;
    use crate::core::index::{
        self, remote_dir_stager, CommitDBReader, CommitEntryReader, CommitWriter,
    };
    use crate::error::OxenError;
    use crate::model::entry::mod_entry::{ModType, NewMod};
    use crate::model::{ContentType, NewCommitBody, StagedData};
    use crate::opts::DFOpts;
    use crate::{api, test, util};

    // This is how we initialize
    #[test]
    fn test_commit_no_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            let status = StagedData::empty();
            log::debug!("run_empty_stager_test before CommitWriter::new...");
            let commit_writer = CommitWriter::new(&repo)?;
            commit_writer.commit(&status, "Init")?;
            stager.unstage()?;

            Ok(())
        })
    }

    #[test]
    fn test_commit() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create committer with no commits
            let repo_path = &repo.path;
            let entry_reader = CommitEntryReader::new_from_head(&repo)?;
            let commit_writer = CommitWriter::new(&repo)?;

            let train_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&train_dir)?;
            let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 1")?;
            let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 2")?;
            let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 3")?;
            let annotation_file = test::add_txt_file_to_dir(repo_path, "some annotations...")?;

            let test_dir = repo_path.join("test_data");
            std::fs::create_dir_all(&test_dir)?;
            let _ = test::add_txt_file_to_dir(&test_dir, "Test Ex 1")?;
            let _ = test::add_txt_file_to_dir(&test_dir, "Test Ex 2")?;

            // Add a file and a directory
            stager.add_file(&annotation_file, &entry_reader)?;
            stager.add_dir(&train_dir, &entry_reader)?;

            let message = "Adding training data to 🐂";
            let status = stager.status(&entry_reader)?;
            let commit = commit_writer.commit(&status, message)?;
            stager.unstage()?;

            let commit_history =
                CommitDBReader::history_from_commit(&commit_writer.commits_db, &commit)?;

            // should be two commits now
            assert_eq!(commit_history.len(), 2);

            // Check that the files are no longer staged
            let status = stager.status(&entry_reader)?;
            let files = status.staged_files;
            assert_eq!(files.len(), 0);
            let dirs = stager.list_staged_dirs()?;
            assert_eq!(dirs.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_append_invalid_schema() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Try stage an append
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let branch = api::local::branches::current_branch(&repo)?.unwrap();
            let identity = UserConfig::identifier()?;

            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &path)?.unwrap();

            let append_contents = "{\"file\": \"images/test.jpg\"}".to_string();
            let new_mod = NewMod {
                entry: commit_entry,
                data: append_contents,
                mod_type: ModType::Append,
                content_type: ContentType::Json,
            };
            let result = index::mod_stager::create_mod(&repo, &branch, &identity, &new_mod);
            // Should be an error
            assert!(result.is_err());

            Ok(())
        })
    }

    #[test]
    fn test_commit_tabular_appends_staged() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Stage an append
            let branch = api::local::branches::current_branch(&repo)?.unwrap();
            let user = UserConfig::get()?.to_user();
            let identity = UserConfig::identifier()?;
            let branch_repo =
                index::remote_dir_stager::init_or_get(&repo, &branch, &identity).unwrap();

            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &path)?.unwrap();
            let append_contents = "{\"file\": \"images/test.jpg\", \"label\": \"dog\", \"min_x\": 2.0, \"min_y\": 3.0, \"width\": 100, \"height\": 120}".to_string();
            let new_mod = NewMod {
                entry: commit_entry,
                data: append_contents,
                mod_type: ModType::Append,
                content_type: ContentType::Json,
            };
            index::mod_stager::create_mod(&repo, &branch, &identity, &new_mod)?;
            let new_commit = NewCommitBody {
                author: user.name.to_owned(),
                email: user.email,
                message: "Appending tabular data".to_string(),
            };

            let commit =
                remote_dir_stager::commit(&repo, &branch_repo, &branch, &new_commit, &identity)?;

            // Make sure version file is updated
            let entry = api::local::entries::get_commit_entry(&repo, &commit, &path)?.unwrap();
            let version_file = util::fs::version_path(&repo, &entry);

            let data_frame = df::tabular::read_df(version_file, DFOpts::empty())?;
            println!("{data_frame}");
            assert_eq!(
                format!("{data_frame}"),
                r"shape: (7, 6)
┌─────────────────┬───────┬───────┬───────┬───────┬────────┐
│ file            ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
│ ---             ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
│ str             ┆ str   ┆ f64   ┆ f64   ┆ i64   ┆ i64    │
╞═════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
│ train/dog_1.jpg ┆ dog   ┆ 101.5 ┆ 32.0  ┆ 385   ┆ 330    │
│ train/dog_1.jpg ┆ dog   ┆ 102.5 ┆ 31.0  ┆ 386   ┆ 330    │
│ train/dog_2.jpg ┆ dog   ┆ 7.0   ┆ 29.5  ┆ 246   ┆ 247    │
│ train/dog_3.jpg ┆ dog   ┆ 19.0  ┆ 63.5  ┆ 376   ┆ 421    │
│ train/cat_1.jpg ┆ cat   ┆ 57.0  ┆ 35.5  ┆ 304   ┆ 427    │
│ train/cat_2.jpg ┆ cat   ┆ 30.5  ┆ 44.0  ┆ 333   ┆ 396    │
│ images/test.jpg ┆ dog   ┆ 2.0   ┆ 3.0   ┆ 100   ┆ 120    │
└─────────────────┴───────┴───────┴───────┴───────┴────────┘"
            );
            Ok(())
        })
    }
}
