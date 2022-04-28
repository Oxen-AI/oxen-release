use crate::config::AuthConfig;
use crate::error::OxenError;
use crate::index::referencer::DEFAULT_BRANCH;
use crate::index::Referencer;
use crate::model::{Commit, LocalEntry, StagedData};
use crate::util;

use chrono::Utc;
use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, IteratorMode, LogLevel, MultiThreaded, Options, DB};
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

    fn history_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(HISTORY_DIR))
    }

    fn commits_dir(path: &Path) -> PathBuf {
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
        match referencer.read_head_ref() {
            Ok(ref_name) => match referencer.get_commit_id(&ref_name) {
                Ok(commit_id) => {
                    let commit_db_path = history_path.join(Path::new(&commit_id));
                    Some(DBWithThreadMode::open(&opts, &commit_db_path).unwrap())
                }
                Err(_) => None,
            },
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
                self.referencer.create_branch(DEFAULT_BRANCH, id)?;
                // Make sure head is pointing to that branch
                self.referencer.set_head(DEFAULT_BRANCH)?;

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

        println!("Commit[{}] {:?}", new_commit.id, path);

        // if we can't get the extension...not a file we want to index anyways
        if let Some(ext) = path.extension() {
            let file_path = self.repository.path.join(path);
            let filename_str = file_path.to_str().unwrap();
            let id = util::hasher::hash_buffer(filename_str.as_bytes());

            let hash = util::hasher::hash_file_contents(&file_path)?;
            let ext = String::from(ext.to_str().unwrap_or(""));

            // Create entry object to as json
            let entry = LocalEntry {
                id: id.clone(),
                hash: hash.clone(),
                is_synced: false, // so we know to sync
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
            std::fs::copy(file_path, versions_path)?;

            // Write to db
            let entry_json = serde_json::to_string(&entry)?;
            db.put(&key, entry_json.as_bytes())?;
        }
        Ok(())
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

    fn add_staged_files_to_db_without_prog(
        &self,
        commit: &Commit,
        added_files: &[PathBuf],
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        added_files
            .par_iter()
            .for_each(|path| match self.add_path_to_commit_db(commit, path, db) {
                Ok(_) => {}
                Err(err) => {
                    let err = format!("Committer failed to commit file: {}", err);
                    eprintln!("{}", err)
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
            match self.add_path_to_commit_db(commit, path, db) {
                Ok(_) => {}
                Err(err) => {
                    let err = format!("Committer failed to commit file: {}", err);
                    eprintln!("{}", err)
                }
            }
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
            println!("Committing files in dir: {:?}", dir);
            let full_path = self.repository.path.join(dir);
            let files: Vec<PathBuf> = util::fs::list_files_in_dir(&full_path)
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
        let ref_name = self.referencer.read_head_ref()?;
        match self.referencer.get_commit_id(&ref_name) {
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
    pub fn commit(
        &mut self,
        status: &StagedData,
        message: &str,
    ) -> Result<Commit, OxenError> {
        // Generate uniq id for this commit
        let commit_id = format!("{}", uuid::Uuid::new_v4());

        // Create a commit object, that either points to parent or not
        // must create this before anything else so that we know if it has parent or not.
        let commit = self.create_commit(&commit_id, message)?;

        // Get last commit_id from the referencer
        // either copy over parent db as a starting point, or start new
        let commit_db_path = self.create_db_dir_for_commit_id(&commit_id)?;

        // Open db
        let opts = Committer::db_opts();
        let commit_db = DBWithThreadMode::open(&opts, &commit_db_path)?;

        // Commit all staged files from db
        self.add_staged_files_to_db(&commit, &status.added_files, &commit_db)?;

        // Commit all staged dirs from db, and recursively add all the files
        self.add_staged_dirs_to_db(&commit, &status.added_dirs, &commit_db)?;

        // Add to commits db id -> commit_json
        self.add_commit(&commit)?;

        // Move head to commit id
        self.referencer.set_head_commit_id(&commit.id)?;
        // Update our current head db to be this commit db so we can quickly find files
        self.head_commit_db = Some(commit_db);

        Ok(commit)
    }

    pub fn get_num_entries_in_head(&self) -> Result<usize, OxenError> {
        if let Some(db) = &self.head_commit_db {
            Ok(db.iterator(IteratorMode::Start).count())
        } else {
            Ok(0)
        }
    }

    pub fn get_num_entries_in_commit(&self, commit_id: &str) -> Result<usize, OxenError> {
        let db = self.get_commit_db(commit_id)?;
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
                    let entry: LocalEntry = serde_json::from_str(value)?;
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

    pub fn update_path_hash(
        &self,
        db: &Option<DBWithThreadMode<MultiThreaded>>,
        path: &Path,
        hash: &str,
    ) -> Result<(), OxenError> {
        if let Some(db) = db {
            let key = path.to_str().unwrap();
            let bytes = key.as_bytes();
            match db.put(bytes, hash) {
                Ok(_) => Ok(()),
                Err(err) => {
                    let err = format!("get_path_hash() Err: {}", err);
                    Err(OxenError::basic_str(&err))
                }
            }
        } else {
            Err(OxenError::basic_str(
                "Committer.update_path_hash() no commit db.",
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

    pub fn set_working_repo_to_branch(&self, name: &str) -> Result<(), OxenError> {
        let commit_id = self.referencer.get_commit_id(name)?;
        let commit_db_path = self.commit_db_path(&commit_id);

        // Open db
        let opts = Committer::db_opts();
        let db = DB::open(&opts, &commit_db_path)?;

        // Iterate over files in current dir, and make sure they should all be there
        // if they aren't in this commit db, remove them
        let dir_entries = std::fs::read_dir(&self.repository.path)?;
        for entry in dir_entries {
            let local_path = entry?.path();
            if local_path.is_file() {
                let relative_path =
                    util::fs::path_relative_to_dir(&local_path, &self.repository.path)?;
                println!("set_working_repo_to_branch[{}] commit_id {} relative_path {:?}", name, commit_id, relative_path);
                let bytes = relative_path.to_str().unwrap().as_bytes();
                match db.get_pinned(bytes) {
                    Ok(_) => {
                        println!("WE HAVE FILE {:?} WE GOOD HOMIE", relative_path);
                    },
                    _ => {
                        // sorry, we don't know you, bye
                        println!("you dead. {:?}", local_path);
                        std::fs::remove_file(local_path)?;
                    }
                }
            }
        }

        // Iterate over files in db, and make sure the hashes match,
        // if different, copy the correct version over
        let iter = db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            let path = Path::new(str::from_utf8(&*key)?);
            let entry: LocalEntry = serde_json::from_str(str::from_utf8(&*value)?)?;

            let dst_path = self.repository.path.join(path);
            let dst_hash = util::hasher::hash_file_contents(&dst_path)?;

            if entry.hash != dst_hash {
                // we need to update working dir
                let entry_filename = entry.file_from_commit_id(&commit_id);
                let version_file = self.versions_dir.join(&entry.id).join(entry_filename);
                std::fs::copy(version_file, dst_path)?;
            }
        }

        Ok(())
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

    pub fn list_unsynced_files_for_commit(
        &self,
        commit: &Commit,
    ) -> Result<Vec<PathBuf>, OxenError> {
        let mut paths: Vec<PathBuf> = vec![];

        match self.get_head_commit() {
            Ok(Some(head_commit)) => {
                if head_commit.id == commit.id {
                    if let Some(db) = &self.head_commit_db {
                        self.p_add_untracked_files_from_commit(&mut paths, db)?
                    } else {
                        eprintln!(
                            "list_unsynced_files_for_commit Err: Could not get head commit db"
                        );
                    }
                } else {
                    let db = self.get_commit_db(&commit.id)?;
                    self.p_add_untracked_files_from_commit(&mut paths, &db)?;
                }
            }
            _ => {
                let db = self.get_commit_db(&commit.id)?;
                self.p_add_untracked_files_from_commit(&mut paths, &db)?;
            }
        };

        Ok(paths)
    }

    fn p_add_untracked_files_from_commit(
        &self,
        paths: &mut Vec<PathBuf>,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        let iter = db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            match (str::from_utf8(&*key), str::from_utf8(&*value)) {
                (Ok(key_str), Ok(value_str)) => {
                    let filepath = PathBuf::from(String::from(key_str));
                    let entry: LocalEntry = serde_json::from_str(value_str)?;
                    if !entry.is_synced {
                        paths.push(filepath);
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

    pub fn get_entry(&self, path: &Path) -> Result<Option<LocalEntry>, OxenError> {
        if let Some(db) = self.head_commit_db.as_ref() {
            let key = path.to_str().unwrap();
            let bytes = key.as_bytes();
            match db.get(bytes) {
                Ok(Some(value)) => {
                    match str::from_utf8(&*value) {
                        Ok(value) => {
                            let entry: LocalEntry = serde_json::from_str(value)?;
                            Ok(Some(entry))
                        },
                        Err(_) => {
                            Err(OxenError::basic_str("get_local_entry_from_commit invalid entry"))
                        }
                    }
                    
                },
                Ok(None) => Ok(None),
                Err(err) => {
                    let err = format!("get_local_entry_from_commit Error reading db\nErr: {}", err);
                    Err(OxenError::basic_str(&err))
                }
            }
        } else {
            Err(OxenError::basic_str("get_local_entry_from_commit no head db"))
        }
    }

    pub fn file_is_committed(&self, path: &Path) -> bool {
        match self.head_contains_file(path) {
            Ok(val) => val,
            Err(_err) => false,
        }
    }

    fn head_contains_file(&self, path: &Path) -> Result<bool, OxenError> {
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

            assert_eq!(commit_history.len(), 1);
            assert_eq!(commit_history[0].id, commit.id);
            assert_eq!(commit_history[0].message, message);

            // Check that the files are no longer staged
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 0);
            let dirs = stager.list_added_directories()?;
            assert_eq!(dirs.len(), 0);

            // List files in commit to be pushed
            let files = committer.list_unsynced_files_for_commit(&commit)?;
            // Two files in training_data and one at base level
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
            assert_eq!(commit_history.len(), 2);
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
            let hello_file = test::add_txt_file_to_dir(&repo_path, "Hello")?;

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
            let commit = committer.commit(&status, "modified hello to be world")?;

            let relative_path = util::fs::path_relative_to_dir(&hello_file, &repo_path)?;
            let entry = committer.get_entry(&relative_path)?.unwrap();
            let entry_dir = Committer::versions_dir(&repo_path).join(&entry.id);
            assert!(entry_dir.exists());

            let entry_file = entry_dir.join(entry.file_from_commit(&commit));
            assert!(entry_file.exists());

            Ok(())
        })
    }
}
