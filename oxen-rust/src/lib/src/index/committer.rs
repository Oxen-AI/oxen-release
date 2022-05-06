use crate::config::AuthConfig;
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::error::OxenError;
use crate::index::Referencer;
use crate::model::{Commit, CommitEntry, StagedData, StagedEntry, StagedEntryStatus};
use crate::util;

use chrono::Utc;
use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, IteratorMode, LogLevel, MultiThreaded, Options, DB};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;

use crate::model::LocalRepository;

/// history/ dir is a list of directories named after commit ids
pub const HISTORY_DIR: &str = "history";
/// commits/ is a key-value database of commit ids to commit objects
pub const COMMITS_DB: &str = "commits";
/// versions/ is where all the versions are stored so that we can use to quickly swap between versions of the file
pub const VERSIONS_DIR: &str = "versions";

pub struct Committer {
    commits_db: DB,
    // TODO: have map of ref names to dbs so that we can have different ones open besides HEAD
    pub head_commit_db: Option<DBWithThreadMode<MultiThreaded>>,
    pub referencer: Referencer,
    history_dir: PathBuf,
    versions_dir: PathBuf,
    auth_cfg: AuthConfig,
    repository: LocalRepository,
}

impl Committer {
    fn db_opts() -> Options {
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Error);
        opts.create_if_missing(true);
        opts
    }

    pub fn history_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(HISTORY_DIR))
    }

    pub fn commits_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(COMMITS_DB))
    }

    pub fn versions_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(VERSIONS_DIR))
    }

    pub fn new(repository: &LocalRepository) -> Result<Committer, OxenError> {
        let history_path = Committer::history_dir(&repository.path);
        let commits_path = Committer::commits_dir(&repository.path);
        let versions_path = Committer::versions_dir(&repository.path);

        if !history_path.exists() {
            std::fs::create_dir_all(&history_path)?;
        }

        // If there is no head commit, we cannot open the commit db
        let opts = Committer::db_opts();
        let referencer = Referencer::new(repository)?;
        let head_commit_db = Committer::head_commit_db(&repository.path, &referencer);

        Ok(Committer {
            commits_db: DB::open(&opts, &commits_path)?,
            head_commit_db,
            referencer,
            history_dir: history_path,
            versions_dir: versions_path,
            auth_cfg: AuthConfig::default().unwrap(),
            repository: repository.clone(),
        })
    }

    fn head_commit_db(
        repo_dir: &Path,
        referencer: &Referencer,
    ) -> Option<DBWithThreadMode<MultiThreaded>> {
        let history_path = Committer::history_dir(repo_dir);
        let opts = Committer::db_opts();
        match referencer.get_head_commit_id() {
            Ok(commit_id) => {
                let commit_db_path = history_path.join(Path::new(&commit_id));
                Some(DBWithThreadMode::open(&opts, &commit_db_path).unwrap())
            }
            Err(_) => None,
        }
    }

    fn commit_db_path(&self, id: &str) -> PathBuf {
        self.history_dir.join(Path::new(&id))
    }

    fn create_db_dir_for_commit_id(&self, id: &str) -> Result<PathBuf, OxenError> {
        match self.referencer.head_commit_id() {
            Ok(parent_id) => {
                // We have a parent, we have to copy over last db, and continue
                let parent_commit_db_path = self.history_dir.join(Path::new(&parent_id));
                let current_commit_db_path = self.history_dir.join(Path::new(&id));
                util::fs::copy_dir_all(&parent_commit_db_path, &current_commit_db_path)?;
                // return current commit path, so we can add to it
                Ok(current_commit_db_path)
            }
            Err(_) => {
                // We are creating initial commit, no parent
                let commit_db_path = self.history_dir.join(Path::new(&id));
                if !commit_db_path.exists() {
                    std::fs::create_dir_all(&commit_db_path)?;
                }

                // Set head to default name -> first commit
                self.referencer.create_branch(DEFAULT_BRANCH_NAME, id)?;
                // Make sure head is pointing to that branch
                self.referencer.set_head(DEFAULT_BRANCH_NAME);

                // return current commit path, so we can insert into it
                Ok(commit_db_path)
            }
        }
    }

    fn add_path_to_commit_db(
        &self,
        new_commit: &Commit,
        path: &Path,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        let path_str = path.to_str().unwrap();
        let key = path_str.as_bytes();

        log::debug!("Commit [{}] commit file {:?}", new_commit.id, path);
        // if we can't get the extension...not a file we want to index anyways
        if let Some(ext) = path.extension() {
            let full_path = self.repository.path.join(path);
            let filename_str = full_path.to_str().unwrap();
            let id = util::hasher::hash_buffer(filename_str.as_bytes());

            let hash = util::hasher::hash_file_contents(&full_path)?;
            let ext = String::from(ext.to_str().unwrap_or(""));

            // Create entry object to as json
            let entry = CommitEntry {
                id: id.clone(),
                path: path.to_path_buf(),
                hash,
                is_synced: false, // so we know to sync
                commit_id: new_commit.id.clone(),
                extension: ext.clone(),
            };

            // create a copy to our versions directory
            // .oxen/versions/ENTRY_ID/COMMIT_ID.ext
            let name = format!("{}.{}", new_commit.id, ext);
            let versions_entry_dir = self.versions_dir.join(id);
            let versions_path = versions_entry_dir.join(name);

            if !versions_entry_dir.exists() {
                std::fs::create_dir_all(versions_entry_dir)?;
            }
            // println!(
            //     "Commit [{}] copied file {:?} to {:?}",
            //     new_commit.id, path, versions_path
            // );
            std::fs::copy(full_path, versions_path)?;

            // Write to db
            let entry_json = serde_json::to_string(&entry)?;
            db.put(&key, entry_json.as_bytes())?;
        }
        Ok(())
    }

    fn remove_path_from_commit_db(
        &self,
        path: &Path,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        let path_str = path.to_str().unwrap();
        let key = path_str.as_bytes();
        db.delete(key)?;
        Ok(())
    }

    fn add_staged_entries_to_db(
        &self,
        commit: &Commit,
        added_files: &[(PathBuf, StagedEntry)],
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        // len kind of arbitrary right now...just nice to see progress on big sets of files
        if added_files.len() > 10000 {
            self.add_staged_entries_to_db_with_prog(commit, added_files, db)
        } else {
            self.add_staged_entries_to_db_without_prog(commit, added_files, db)
        }
    }

    fn add_staged_files_to_db(
        &self,
        commit: &Commit,
        added_files: &[PathBuf],
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        // len kind of arbitrary right now...just nice to see progress on big sets of files
        if added_files.len() > 10000 {
            self.add_staged_files_to_db_with_prog(commit, added_files, db)
        } else {
            self.add_staged_files_to_db_without_prog(commit, added_files, db)
        }
    }

    fn commit_staged_entry_to_db(
        &self,
        commit: &Commit,
        path: &Path,
        entry: &StagedEntry,
        db: &DBWithThreadMode<MultiThreaded>,
    ) {
        if entry.status == StagedEntryStatus::Removed {
            match self.remove_path_from_commit_db(path, db) {
                Ok(_) => {}
                Err(err) => {
                    let err = format!(
                        "Committer.commit_staged_entry_to_db failed to remove file: {}",
                        err
                    );
                    eprintln!("{}", err)
                }
            }
        } else {
            // TODO: have different path for modified
            match self.add_path_to_commit_db(commit, path, db) {
                Ok(_) => {}
                Err(err) => {
                    let err = format!(
                        "Committer.commit_staged_entry_to_db failed to add file: {}",
                        err
                    );
                    eprintln!("{}", err)
                }
            }
        }
    }

    fn add_staged_files_to_db_without_prog(
        &self,
        commit: &Commit,
        added_files: &[PathBuf],
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        added_files.par_iter().for_each(|path| {
            if self.add_path_to_commit_db(commit, path, db).is_err() {
                eprintln!("Error staging file... {:?}", path);
            }
        });
        Ok(())
    }

    fn add_staged_files_to_db_with_prog(
        &self,
        commit: &Commit,
        added_files: &[PathBuf],
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        let size: u64 = unsafe { std::mem::transmute(added_files.len()) };
        let bar = ProgressBar::new(size);
        added_files.par_iter().for_each(|path| {
            if self.add_path_to_commit_db(commit, path, db).is_err() {
                eprintln!("Error staging file... {:?}", path);
            }
            bar.inc(1);
        });
        bar.finish();
        Ok(())
    }

    fn add_staged_entries_to_db_without_prog(
        &self,
        commit: &Commit,
        added_files: &[(PathBuf, StagedEntry)],
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        added_files
            .par_iter()
            .for_each(|(path, entry)| self.commit_staged_entry_to_db(commit, path, entry, db));
        Ok(())
    }

    fn add_staged_entries_to_db_with_prog(
        &self,
        commit: &Commit,
        added_files: &[(PathBuf, StagedEntry)],
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        let size: u64 = unsafe { std::mem::transmute(added_files.len()) };
        let bar = ProgressBar::new(size);
        added_files.par_iter().for_each(|(path, entry)| {
            self.commit_staged_entry_to_db(commit, path, entry, db);
            bar.inc(1);
        });
        bar.finish();
        Ok(())
    }

    fn add_staged_dirs_to_db(
        &self,
        commit: &Commit,
        added_dirs: &[(PathBuf, usize)],
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        for (dir, _) in added_dirs.iter() {
            // println!("Commit [{}] files in dir: {:?}", commit.id, dir);
            let full_path = self.repository.path.join(dir);
            let files: Vec<PathBuf> = util::fs::rlist_files_in_dir(&full_path)
                .into_iter()
                .map(|path| util::fs::path_relative_to_dir(&path, &self.repository.path).unwrap())
                .collect();
            self.add_staged_files_to_db(commit, &files, db)?;
        }
        Ok(())
    }

    fn create_commit(&self, id_str: &str, message: &str) -> Result<Commit, OxenError> {
        // Commit
        //  - parent_commit_id (can be empty if root)
        //  - message
        //  - date
        //  - author
        match self.referencer.get_head_commit_id() {
            Ok(parent_id) => {
                // We have a parent
                Ok(Commit {
                    id: String::from(id_str),
                    parent_id: Some(parent_id),
                    message: String::from(message),
                    author: self.auth_cfg.user.name.clone(),
                    date: Utc::now(),
                })
            }
            Err(_) => {
                // We are creating initial commit, no parent
                Ok(Commit {
                    id: String::from(id_str),
                    parent_id: None,
                    message: String::from(message),
                    author: self.auth_cfg.user.name.clone(),
                    date: Utc::now(),
                })
            }
        }
    }

    pub fn has_commits(&self) -> bool {
        self.referencer.head_commit_id().is_ok()
    }

    // Create a db in the history/ dir under the id
    // We will have something like:
    // history/
    //   3a54208a-f792-45c1-8505-e325aa4ce5b3/
    //     annotations.txt -> b"{entry_json}"
    //     train/image_1.png -> b"{entry_json}"
    //     train/image_2.png -> b"{entry_json}"
    //     test/image_2.png -> b"{entry_json}"
    pub fn commit(&mut self, status: &StagedData, message: &str) -> Result<Commit, OxenError> {
        // Generate uniq id for this commit
        let commit_id = format!("{}", uuid::Uuid::new_v4());

        // Create a commit object, that either points to parent or not
        // must create this before anything else so that we know if it has parent or not.
        let commit = self.create_commit(&commit_id, message)?;
        log::debug!(
            "COMMIT_START Repo {:?} commit {} message [{}]",
            self.repository.path,
            commit.id,
            commit.message
        );

        // Get last commit_id from the referencer
        // either copy over parent db as a starting point, or start new
        let commit_db_path = self.create_db_dir_for_commit_id(&commit_id)?;

        // Open db
        let opts = Committer::db_opts();
        let commit_db = DBWithThreadMode::open(&opts, &commit_db_path)?;

        // Commit all staged files from db
        self.add_staged_entries_to_db(&commit, &status.added_files, &commit_db)?;

        // Commit all staged dirs from db, and recursively add all the files
        self.add_staged_dirs_to_db(&commit, &status.added_dirs, &commit_db)?;

        // Add to commits db id -> commit_json
        self.add_commit(&commit)?;

        // Move head to commit id
        self.referencer.set_head_commit_id(&commit.id)?;
        // Update our current head db to be this commit db so we can quickly find files
        self.head_commit_db = Some(commit_db);

        // println!("COMMIT_COMPLETE {} -> {}", commit.id, commit.message);

        Ok(commit)
    }

    pub fn num_entries_in_head(&self) -> Result<usize, OxenError> {
        if let Some(db) = &self.head_commit_db {
            Ok(db.iterator(IteratorMode::Start).count())
        } else {
            Ok(0)
        }
    }

    pub fn num_entries_in_commit(&self, commit_id: &str) -> Result<usize, OxenError> {
        let db = self.get_commit_db_read_only(commit_id)?;
        Ok(db.iterator(IteratorMode::Start).count())
    }

    pub fn get_commit_db(
        &self,
        commit_id: &str,
    ) -> Result<DBWithThreadMode<MultiThreaded>, OxenError> {
        let commit_db_path = self.history_dir.join(Path::new(&commit_id));
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Error);
        let db = DBWithThreadMode::open(&opts, &commit_db_path)?;
        Ok(db)
    }

    pub fn get_commit_db_read_only(
        &self,
        commit_id: &str,
    ) -> Result<DBWithThreadMode<MultiThreaded>, OxenError> {
        let commit_db_path = self.history_dir.join(Path::new(&commit_id));
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Error);
        let db = DBWithThreadMode::open_for_read_only(&opts, &commit_db_path, false)?;
        Ok(db)
    }

    pub fn get_path_hash(
        &self,
        db: &Option<DBWithThreadMode<MultiThreaded>>,
        path: &Path,
    ) -> Result<String, OxenError> {
        if let Some(db) = db {
            let key = path.to_str().unwrap();
            let bytes = key.as_bytes();
            match db.get(bytes) {
                Ok(Some(value)) => {
                    let value = str::from_utf8(&*value)?;
                    let entry: CommitEntry = serde_json::from_str(value)?;
                    Ok(entry.hash)
                }
                Ok(None) => Ok(String::from("")), // no hash, empty string
                Err(err) => {
                    let err = format!("get_path_hash() Err: {}", err);
                    Err(OxenError::basic_str(&err))
                }
            }
        } else {
            Err(OxenError::basic_str(
                "Committer.get_path_hash() no commit db.",
            ))
        }
    }

    pub fn set_is_synced(
        &self,
        db: &Option<DBWithThreadMode<MultiThreaded>>,
        entry: &CommitEntry,
    ) -> Result<(), OxenError> {
        if let Some(db) = db {
            let key = entry.path.to_str().unwrap();
            let bytes = key.as_bytes();
            let entry = entry.to_synced();
            let json_str = serde_json::to_string(&entry)?;
            let data = json_str.as_bytes();
            match db.put(bytes, data) {
                Ok(_) => Ok(()),
                Err(err) => {
                    let err = format!("set_is_synced() Err: {}", err);
                    Err(OxenError::basic_str(&err))
                }
            }
        } else {
            Err(OxenError::basic_str(
                "Committer.set_is_synced() no commit db.",
            ))
        }
    }

    pub fn add_commit(&mut self, commit: &Commit) -> Result<(), OxenError> {
        // Write commit json to db
        let commit_json = serde_json::to_string(&commit)?;
        self.commits_db.put(&commit.id, commit_json.as_bytes())?;
        Ok(())
    }

    pub fn list_commits(&self) -> Result<Vec<Commit>, OxenError> {
        let mut commit_msgs: Vec<Commit> = vec![];
        // Start with head, and the get parents until there are no parents
        match self.referencer.head_commit_id() {
            Ok(commit_id) => {
                self.p_list_commits(&commit_id, &mut commit_msgs)?;
                Ok(commit_msgs)
            }
            Err(_) => Ok(commit_msgs),
        }
    }

    pub fn list_files_in_head_commit_db(&self) -> Result<Vec<PathBuf>, OxenError> {
        if let Some(db) = &self.head_commit_db {
            return self.list_files_in_commit_db(db);
        }
        Ok(vec![])
    }

    pub fn list_entries_in_head_commit_db(&self) -> Result<Vec<CommitEntry>, OxenError> {
        if let Some(db) = &self.head_commit_db {
            return self.list_entries_in_commit_db(db);
        }
        Ok(vec![])
    }

    pub fn list_entries_for_commit(&self, commit: &Commit) -> Result<Vec<CommitEntry>, OxenError> {
        match self.get_commit_db_read_only(&commit.id) {
            Ok(db) => {
                log::debug!("Found db for commit_id: {}", commit.id);
                self.list_entries_in_commit_db(&db)
            }
            Err(err) => {
                log::error!("Could not find db for commit_id: {}", commit.id);
                Err(err)
            }
        }
    }

    pub fn list_entry_page_for_commit(&self, commit: &Commit, page_num: usize, page_size: usize) -> Result<Vec<CommitEntry>, OxenError> {
        match self.get_commit_db_read_only(&commit.id) {
            Ok(db) => {
                log::debug!("Found db for commit_id: {}", commit.id);
                self.list_entry_page_in_commit_db(&db, page_num, page_size)
            }
            Err(err) => {
                log::error!("Could not find db for commit_id: {}", commit.id);
                Err(err)
            }
        }
    }

    fn list_files_in_commit_db(
        &self,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<Vec<PathBuf>, OxenError> {
        let mut paths: Vec<PathBuf> = vec![];
        let iter = db.iterator(IteratorMode::Start);
        for (key, _value) in iter {
            paths.push(PathBuf::from(str::from_utf8(&*key)?));
        }
        Ok(paths)
    }

    fn list_entries_in_commit_db(
        &self,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        let mut paths: Vec<CommitEntry> = vec![];
        let iter = db.iterator(IteratorMode::Start);
        for (_key, value) in iter {
            let entry: CommitEntry = serde_json::from_str(str::from_utf8(&*value)?)?;
            paths.push(entry);
        }
        Ok(paths)
    }

    fn list_entry_page_in_commit_db(
        &self,
        db: &DBWithThreadMode<MultiThreaded>,
        page_num: usize,
        page_size: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        // The iterator doesn't technically have a skip method as far as I can tell
        // so we are just going to manually do it
        let mut paths: Vec<CommitEntry> = vec![];
        let iter = db.iterator(IteratorMode::Start);
        // Do not go negative, and start from 0
        let start_page = if page_num == 0 { 0 } else { page_num - 1 };
        let start_idx = start_page * page_size;
        let mut entry_i = 0;
        for (_key, value) in iter {
            // limit to page_size
            if paths.len() >= page_size {
                break;
            }

            // only grab values after start_idx based on page_num and page_size
            if entry_i >= start_idx {
                let entry: CommitEntry = serde_json::from_str(str::from_utf8(&*value)?)?;
                paths.push(entry);
            }

            entry_i += 1;
        }
        Ok(paths)
    }

    pub fn set_working_repo_to_commit_id(&self, commit_id: &str) -> Result<(), OxenError> {
        if !self.commit_id_exists(commit_id) {
            let err = format!("Ref not exist: {}", commit_id);
            return Err(OxenError::basic_str(&err));
        }

        let commit_db_path = self.commit_db_path(commit_id);

        if let Some(head_commit) = self.get_head_commit()? {
            if head_commit.id == commit_id {
                // Don't do anything if we tried to switch to same commit
                return Ok(());
            }
        }

        // Open db
        let opts = Committer::db_opts();
        let commit_db = DBWithThreadMode::open(&opts, &commit_db_path)?;

        // Keep track of directories, since we do not explicitly store which ones are tracked...
        // we will remove them later if no files exist in them.
        let mut candidate_dirs_to_rm: HashSet<PathBuf> = HashSet::new();

        // Iterate over files in that are in *current head* and make sure they should all be there
        // if they aren't in commit db we are switching to, remove them
        let current_entries =
            self.list_files_in_commit_db(self.head_commit_db.as_ref().unwrap())?;
        for path in current_entries.iter() {
            let repo_path = self.repository.path.join(path);
            // println!(
            //     "set_working_repo_to_commit_id current_entries[{:?}]",
            //     repo_path
            // );
            if repo_path.is_file() {
                // println!(
                //     "set_working_repo_to_commit_id[{}] commit_id {} path {:?}",
                //     name, commit_id, path
                // );

                // Keep track of parents to see if we clear them
                if let Some(parent) = path.parent() {
                    if parent.parent().is_some() {
                        // only add one directory below top level
                        // println!("set_working_repo_to_commit_id candidate dir {:?}", parent);
                        candidate_dirs_to_rm.insert(parent.to_path_buf());
                    }
                }

                let bytes = path.to_str().unwrap().as_bytes();
                match commit_db.get(bytes) {
                    Ok(Some(_value)) => {
                        // We already have file ✅
                        // println!(
                        //     "set_working_repo_to_commit_id we already have file ✅ {:?}",
                        //     repo_path
                        // );
                    }
                    _ => {
                        // sorry, we don't know you, bye
                        // println!("set_working_repo_to_commit_id see ya 💀 {:?}", repo_path);
                        std::fs::remove_file(repo_path)?;
                    }
                }
            }
        }

        // Iterate over files in current commit db, and make sure the hashes match,
        // if different, copy the correct version over
        let commit_entries = self.list_entries_in_commit_db(&commit_db)?;
        println!("Setting working directory to {}", commit_id);
        let size: u64 = unsafe { std::mem::transmute(commit_entries.len()) };
        let bar = ProgressBar::new(size);
        for entry in commit_entries.iter() {
            bar.inc(1);
            let path = &entry.path;
            // println!("Committed entry: {:?}", path);
            if let Some(parent) = path.parent() {
                // Check if parent directory exists, if it does, we no longer have
                // it as a candidate to remove
                // println!("CHECKING {:?}", parent);
                if candidate_dirs_to_rm.contains(parent) {
                    candidate_dirs_to_rm.remove(&parent.to_path_buf());
                }
            }

            let dst_path = self.repository.path.join(path);

            // Check the versioned file hash
            let version_filename = entry.filename();
            let version_path = self.versions_dir.join(&entry.id).join(version_filename);

            // If we do not have the file, restore it from our versioned history
            if !dst_path.exists() {
                // println!(
                //     "set_working_repo_to_commit_id restore file, she new 🙏 {:?} -> {:?}",
                //     version_path, dst_path
                // );

                // mkdir if not exists for the parent
                if let Some(parent) = dst_path.parent() {
                    if !parent.exists() {
                        std::fs::create_dir_all(parent)?;
                    }
                }

                std::fs::copy(version_path, dst_path)?;
            } else {
                // we do have it, check if we need to update it
                let dst_hash = util::hasher::hash_file_contents(&dst_path)?;

                // If the hash of the file from the commit is different than the one on disk, update it
                if entry.hash != dst_hash {
                    // we need to update working dir
                    // println!(
                    //     "set_working_repo_to_commit_id restore file diff hash 🙏 {:?} -> {:?}",
                    //     version_path, dst_path
                    // );
                    std::fs::copy(version_path, dst_path)?;
                }
            }
        }

        bar.finish();

        if !candidate_dirs_to_rm.is_empty() {
            println!("Cleaning up...");
        }

        // Remove un-tracked directories
        for dir in candidate_dirs_to_rm.iter() {
            let full_dir = self.repository.path.join(dir);
            // println!("set_working_repo_to_commit_id remove dis dir {:?}", full_dir);
            std::fs::remove_dir_all(full_dir)?;
        }

        Ok(())
    }

    pub fn set_working_repo_to_branch(&self, name: &str) -> Result<(), OxenError> {
        if let Some(commit_id) = self.referencer.get_commit_id_for_branch(name)? {
            self.set_working_repo_to_commit_id(&commit_id)
        } else {
            let err = format!("Could not get commit_id for branch: {}", name);
            Err(OxenError::basic_str(&err))
        }
    }

    fn p_list_commits(&self, commit_id: &str, messages: &mut Vec<Commit>) -> Result<(), OxenError> {
        // println!("p_list_commits commit_id {}", commit_id);

        if let Some(commit) = self.get_commit_by_id(commit_id)? {
            // println!("p_list_commits got commit {}", commit.message);
            messages.push(commit.clone());
            if let Some(parent_id) = &commit.parent_id {
                // println!("p_list_commits got parent {}", parent_id);
                self.p_list_commits(parent_id, messages)?;
            }
        } else {
            // println!("p_list_commits could not get commit id... {}", commit_id);
        }
        Ok(())
    }

    pub fn list_unsynced_entries_for_commit(
        &self,
        commit: &Commit,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        let mut entries: Vec<CommitEntry> = vec![];

        match self.get_head_commit() {
            Ok(Some(head_commit)) => {
                if head_commit.id == commit.id {
                    if let Some(db) = &self.head_commit_db {
                        self.p_add_untracked_files_from_commit(&mut entries, db)?
                    } else {
                        eprintln!(
                            "list_unsynced_entries_for_commit Err: Could not get head commit db"
                        );
                    }
                } else {
                    let db = self.get_commit_db(&commit.id)?;
                    self.p_add_untracked_files_from_commit(&mut entries, &db)?;
                }
            }
            _ => {
                let db = self.get_commit_db(&commit.id)?;
                self.p_add_untracked_files_from_commit(&mut entries, &db)?;
            }
        };

        Ok(entries)
    }

    fn p_add_untracked_files_from_commit(
        &self,
        entries: &mut Vec<CommitEntry>,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        let iter = db.iterator(IteratorMode::Start);
        for (_key, value) in iter {
            match str::from_utf8(&*value) {
                Ok(value_str) => {
                    let entry: CommitEntry = serde_json::from_str(value_str)?;
                    if !entry.is_synced {
                        entries.push(entry);
                    }
                }
                _ => {
                    eprintln!("Could not read utf8 val...")
                }
            }
        }
        Ok(())
    }

    pub fn get_head_commit(&self) -> Result<Option<Commit>, OxenError> {
        match self.referencer.head_commit_id() {
            Ok(commit_id) => Ok(self.get_commit_by_id(&commit_id)?),
            Err(_) => Ok(None),
        }
    }

    pub fn commit_id_exists(&self, commit_id: &str) -> bool {
        // Check if the id is in the DB
        let key = commit_id.as_bytes();
        match self.commits_db.get(key) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(_) => false,
        }
    }

    pub fn get_commit_by_id(&self, commit_id: &str) -> Result<Option<Commit>, OxenError> {
        // Check if the id is in the DB
        let key = commit_id.as_bytes();
        match self.commits_db.get(key) {
            Ok(Some(value)) => {
                let commit: Commit = serde_json::from_str(str::from_utf8(&*value)?)?;
                Ok(Some(commit))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!(
                    "Error commits_db to find commit_id {:?}\nErr: {}",
                    commit_id, err
                );
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn head_has_files_in_dir(&self, dir: &Path) -> bool {
        match self.list_entries_in_head_commit_db() {
            Ok(entries) => entries.into_iter().any(|entry| entry.path.starts_with(dir)),
            _ => false,
        }
    }

    pub fn list_head_files_from_dir(&self, dir: &Path) -> Vec<CommitEntry> {
        match self.list_entries_in_head_commit_db() {
            Ok(entries) => entries
                .into_iter()
                .filter(|entry| entry.path.starts_with(dir))
                .collect(),
            _ => {
                vec![]
            }
        }
    }

    pub fn get_entry(&self, path: &Path) -> Result<Option<CommitEntry>, OxenError> {
        if let Some(db) = self.head_commit_db.as_ref() {
            let key = path.to_str().unwrap();
            let bytes = key.as_bytes();
            match db.get(bytes) {
                Ok(Some(value)) => match str::from_utf8(&*value) {
                    Ok(value) => {
                        let entry: CommitEntry = serde_json::from_str(value)?;
                        Ok(Some(entry))
                    }
                    Err(_) => Err(OxenError::basic_str(
                        "get_local_entry_from_commit invalid entry",
                    )),
                },
                Ok(None) => Ok(None),
                Err(err) => {
                    let err = format!("get_local_entry_from_commit Error reading db\nErr: {}", err);
                    Err(OxenError::basic_str(&err))
                }
            }
        } else {
            Err(OxenError::basic_str(
                "get_local_entry_from_commit no head db",
            ))
        }
    }

    pub fn file_is_committed(&self, path: &Path) -> bool {
        match self.head_contains_file(path) {
            Ok(val) => val,
            Err(_err) => false,
        }
    }

    pub fn head_contains_file(&self, path: &Path) -> Result<bool, OxenError> {
        if let Some(db) = self.head_commit_db.as_ref() {
            // Check if path is in this commit
            let key = path.to_str().unwrap();
            let bytes = key.as_bytes();
            match db.get(bytes) {
                Ok(Some(_value)) => Ok(true),
                Ok(None) => Ok(false),
                Err(err) => {
                    let err = format!("head_contains_file Error reading db\nErr: {}", err);
                    Err(OxenError::basic_str(&err))
                }
            }
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::index::Committer;
    use crate::test;
    use crate::util;

    #[test]
    fn test_commit_staged() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let repo_path = &stager.repository.path;
            let mut committer = Committer::new(&stager.repository)?;

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
            stager.add_file(&annotation_file, &committer)?;
            stager.add_dir(&train_dir, &committer)?;

            let message = "Adding training data to 🐂";
            let status = stager.status(&committer)?;
            let commit = committer.commit(&status, message)?;
            stager.unstage()?;
            let commit_history = committer.list_commits()?;

            let head = committer.get_head_commit()?;
            assert!(head.is_some());

            // always start with an initial commit
            assert_eq!(commit_history.len(), 2);
            assert_eq!(commit_history[0].id, commit.id);
            assert_eq!(commit_history[0].message, message);

            // Check that the files are no longer staged
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 0);
            let dirs = stager.list_added_directories()?;
            assert_eq!(dirs.len(), 0);

            // List files in commit to be pushed
            let files = committer.list_unsynced_entries_for_commit(&commit)?;
            for file in files.iter() {
                log::debug!("unsynced: {:?}", file);
            }
            // three files in training_data and one annotation file at base level
            assert_eq!(files.len(), 4);

            // Verify that the current commit contains the hello file
            let relative_annotation_path =
                util::fs::path_relative_to_dir(&annotation_file, repo_path)?;
            assert!(committer.head_contains_file(&relative_annotation_path)?);

            // Add more files and commit again, make sure the commit copied over the last one
            stager.add_dir(&test_dir, &committer)?;
            let message_2 = "Adding test data to 🐂";
            let status = stager.status(&committer)?;
            let commit = committer.commit(&status, message_2)?;

            // Remove from staged
            stager.unstage()?;

            // Check commit history
            let commit_history = committer.list_commits()?;
            // The messages come out LIFO
            assert_eq!(commit_history.len(), 3);
            assert_eq!(commit_history[0].id, commit.id);
            assert_eq!(commit_history[0].message, message_2);
            assert!(committer.head_contains_file(&relative_annotation_path)?);

            Ok(())
        })
    }

    #[test]
    fn test_commit_modified() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let repo_path = &stager.repository.path;
            let mut committer = Committer::new(&stager.repository)?;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello")?;

            // add & commit the file
            stager.add_file(&hello_file, &committer)?;
            let status = stager.status(&committer)?;
            committer.commit(&status, "added hello")?;
            stager.unstage()?; // make sure to unstage

            // modify the file
            let hello_file = test::modify_txt_file(hello_file, "Hello World")?;
            let status = stager.status(&committer)?;
            assert_eq!(status.modified_files.len(), 1);
            // Add the modified file
            stager.add_file(&hello_file, &committer)?;
            // commit the mods
            let status = stager.status(&committer)?;
            let _commit = committer.commit(&status, "modified hello to be world")?;

            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;
            let entry = committer.get_entry(&relative_path)?.unwrap();
            let entry_dir = Committer::versions_dir(repo_path).join(&entry.id);
            assert!(entry_dir.exists());

            let entry_file = entry_dir.join(entry.filename());
            assert!(entry_file.exists());

            Ok(())
        })
    }
}
