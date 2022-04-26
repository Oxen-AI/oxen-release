use crate::config::AuthConfig;
use crate::error::OxenError;
use crate::index::referencer::DEFAULT_BRANCH;
use crate::index::Referencer;
use crate::model::Commit;
use crate::util;

use chrono::Utc;
use rocksdb::{DBWithThreadMode, IteratorMode, LogLevel, MultiThreaded, Options, DB};
use std::path::{Path, PathBuf};
use std::str;

use crate::model::LocalRepository;

pub const HISTORY_DIR: &str = "history";
pub const COMMITS_DB: &str = "commits";

pub struct Committer {
    commits_db: DB,
    // TODO: have map of ref names to dbs so that we can have different ones open besides HEAD
    pub head_commit_db: Option<DBWithThreadMode<MultiThreaded>>,
    pub referencer: Referencer,
    history_dir: PathBuf,
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

    pub fn new(repository: &LocalRepository) -> Result<Committer, OxenError> {
        let history_path = Committer::history_dir(&repository.path);
        let commits_path = Committer::commits_dir(&repository.path);

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

    fn add_staged_files_to_db(
        &self,
        added_files: &[PathBuf],
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        for path in added_files.iter() {
            let path_str = path.to_str().unwrap();
            let key = path_str.as_bytes();

            // Value is initially empty, meaning we still have to hash, but just keeping track of what is staged
            // Then when we push, we hash the file contents and save it back in here to keep track of progress
            db.put(&key, b"")?;
        }
        Ok(())
    }

    fn add_staged_dirs_to_db(
        &self,
        added_dirs: &[(PathBuf, usize)],
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        for (dir, _) in added_dirs.iter() {
            let full_path = self.repository.path.join(dir);
            let files = util::fs::list_files_in_dir(&full_path);
            self.add_staged_files_to_db(&files, db)?;
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
    //     annotations.txt -> b""
    //     train/image_1.png -> b""
    //     train/image_2.png -> b""
    //     test/image_2.png -> b""
    pub fn commit(
        &mut self,
        added_files: &[PathBuf],
        added_dirs: &[(PathBuf, usize)],
        message: &str,
    ) -> Result<String, OxenError> {
        // Generate uniq id for this commit
        let commit_id = format!("{}", uuid::Uuid::new_v4());

        // Create a commit object, that either points to parent or not
        // must create this before anything else so that we know if it has parent or not.
        let commit = self.create_commit(&commit_id, message)?;
        println!("CREATE COMMIT: {:?}", commit);

        // Get last commit_id from the referencer
        // either copy over parent db as a starting point, or start new
        let commit_db_path = self.create_db_dir_for_commit_id(&commit_id)?;

        // Open db
        let opts = Committer::db_opts();
        let commit_db = DBWithThreadMode::open(&opts, &commit_db_path)?;

        // Commit all staged files from db
        self.add_staged_files_to_db(added_files, &commit_db)?;

        // Commit all staged dirs from db, and recursively add all the files
        self.add_staged_dirs_to_db(added_dirs, &commit_db)?;

        // Add to commits db id -> commit_json
        self.add_commit(&commit)?;

        // Move head to commit id
        self.referencer.set_head_commit_id(&commit.id)?;
        // Update our current head db to be this commit db so we can quickly find files
        self.head_commit_db = Some(commit_db);

        Ok(commit_id)
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
                Ok(Some(value)) => Ok(String::from(str::from_utf8(&*value)?)),
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
        commit_id: &str,
    ) -> Result<Vec<PathBuf>, OxenError> {
        let mut paths: Vec<PathBuf> = vec![];

        match self.get_head_commit() {
            Ok(Some(head_commit)) => {
                if head_commit.id == commit_id {
                    if let Some(db) = &self.head_commit_db {
                        self.p_add_untracked_files_from_commit(&mut paths, db)
                    } else {
                        eprintln!(
                            "list_unsynced_files_for_commit Err: Could not get head commit db"
                        );
                    }
                } else {
                    let db = self.get_commit_db(commit_id)?;
                    self.p_add_untracked_files_from_commit(&mut paths, &db);
                }
            }
            _ => {
                let db = self.get_commit_db(commit_id)?;
                self.p_add_untracked_files_from_commit(&mut paths, &db);
            }
        };

        Ok(paths)
    }

    fn p_add_untracked_files_from_commit(
        &self,
        paths: &mut Vec<PathBuf>,
        db: &DBWithThreadMode<MultiThreaded>,
    ) {
        let iter = db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            match str::from_utf8(&*key) {
                Ok(key_str) => {
                    let filepath = PathBuf::from(String::from(key_str));
                    // let value_str = str::from_utf8(&*value)?;
                    // println!("list_unsynced_files_for_commit ({},{})", key_str, value_str);
                    // If we don't have a hash for the file as the value, it means we haven't pushed it.
                    if value.is_empty() {
                        paths.push(filepath);
                    }
                }
                Err(_) => {
                    eprintln!("Could not read utf8 val...")
                }
            }
        }
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
            let added_files = stager.list_added_files()?;
            let added_dirs = stager.list_added_directories()?;
            let commit_id = committer.commit(&added_files, &added_dirs, message)?;
            stager.unstage()?;
            let commit_history = committer.list_commits()?;

            let head = committer.get_head_commit()?;
            assert!(head.is_some());

            assert_eq!(commit_history.len(), 1);
            assert_eq!(commit_history[0].id, commit_id);
            assert_eq!(commit_history[0].message, message);

            // Check that the files are no longer staged
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 0);
            let dirs = stager.list_added_directories()?;
            assert_eq!(dirs.len(), 0);

            // List files in commit to be pushed
            let files = committer.list_unsynced_files_for_commit(&commit_id)?;
            // Two files in training_data and one at base level
            assert_eq!(files.len(), 4);

            // Verify that the current commit contains the hello file
            let relative_annotation_path =
                util::fs::path_relative_to_dir(&annotation_file, repo_path)?;
            assert!(committer.head_contains_file(&relative_annotation_path)?);

            // Add more files and commit again, make sure the commit copied over the last one
            stager.add_dir(&test_dir, &committer)?;
            let message_2 = "Adding test data to 🐂";
            let added_files = stager.list_added_files()?;
            let added_dirs = stager.list_added_directories()?;
            let commit_id = committer.commit(&added_files, &added_dirs, message_2)?;
            stager.unstage()?;
            let commit_history = committer.list_commits()?;

            for commit in commit_history.iter() {
                println!("COMMIT HISTORY {:?}", commit);
            }

            // The messages come out LIFO
            assert_eq!(commit_history.len(), 2);
            assert_eq!(commit_history[0].id, commit_id);
            assert_eq!(commit_history[0].message, message_2);
            assert!(committer.head_contains_file(&relative_annotation_path)?);

            // Push some of them

            // List remaining

            // Push rest

            // Confirm none left to be pushed

            // List all files in commit

            // List pushed files in commit

            Ok(())
        })
    }
}
