use crate::cli::indexer::OXEN_HIDDEN_DIR;

use crate::cli::{Referencer};
use crate::config::AuthConfig;
use crate::error::OxenError;
use crate::model::CommitMsg;
use crate::util::FileUtil;

use chrono::Utc;
use rocksdb::{IteratorMode, DB, DBWithThreadMode, MultiThreaded, Options, LogLevel};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;

pub const HISTORY_DIR: &str = "history";
pub const COMMITS_DB: &str = "commits";

pub struct Committer {
    commits_db: DB,
    // TODO: have map of ref names to dbs so that we can have different ones open besides HEAD
    pub head_commit_db: Option<DBWithThreadMode<MultiThreaded>>,
    pub referencer: Referencer,
    history_dir: PathBuf,
    auth_cfg: AuthConfig,
    pub repo_dir: PathBuf,
}

impl Committer {
    pub fn db_opts() -> Options {
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Warn);
        opts.create_if_missing(true);
        opts
    }

    pub fn new(repo_dir: &Path) -> Result<Committer, OxenError> {
        let history_path = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(HISTORY_DIR)));
        let commits_path = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(COMMITS_DB)));

        if !history_path.exists() {
            std::fs::create_dir_all(&history_path)?;
        }

        // If there is no head commit, we cannot open the commit db
        let opts = Committer::db_opts();
        let referencer = Referencer::new(repo_dir)?;
        let head_commit_db = Committer::head_commit_db(&repo_dir, &referencer);

        Ok(Committer {
            commits_db: DB::open(&opts, &commits_path)?,
            head_commit_db: head_commit_db,
            referencer: referencer,
            history_dir: history_path,
            auth_cfg: AuthConfig::default().unwrap(),
            repo_dir: repo_dir.to_path_buf(),
        })
    }

    pub fn count_files_from_dir(&self, dir: &Path) -> usize {
        // TODO: Make more common util to collect files with extensions
        let exts: HashSet<String> = vec!["jpg", "jpeg", "png", "txt"]
            .into_iter()
            .map(String::from)
            .collect();
        FileUtil::rcount_files_with_extension(&dir, &exts)
    }

    fn head_commit_db(repo_dir: &Path, referencer: &Referencer) -> Option<DBWithThreadMode<MultiThreaded>>  {
        let history_path = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(HISTORY_DIR)));
        let opts = Committer::db_opts();
        match referencer.read_head() {
            Ok(ref_name) => {
                match referencer.get_commit_id(&ref_name) {
                    Ok(commit_id) => {
                        let commit_db_path = history_path.join(Path::new(&commit_id));
                        Some(DBWithThreadMode::open(&opts, &commit_db_path).unwrap())
                    },
                    Err(_) => None
                }
            }
            Err(_) => None,
        }
    }

    fn list_image_files_from_dir(&self, dir: &Path) -> Vec<PathBuf> {
        let img_ext: HashSet<String> = vec!["jpg", "jpeg", "png"]
            .into_iter()
            .map(String::from)
            .collect();
        FileUtil::recursive_files_with_extensions(dir, &img_ext)
            .into_iter()
            .collect()
    }

    fn list_text_files_from_dir(&self, dir: &Path) -> Vec<PathBuf> {
        let img_ext: HashSet<String> = vec!["txt"].into_iter().map(String::from).collect();
        FileUtil::recursive_files_with_extensions(dir, &img_ext)
            .into_iter()
            .collect()
    }

    fn list_files_in_dir(&self, path: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = vec![];
        let mut img_paths = self.list_image_files_from_dir(path);
        let mut txt_paths = self.list_text_files_from_dir(path);

        // println!("Found {} images", img_paths.len());
        // println!("Found {} text files", txt_paths.len());

        paths.append(&mut img_paths);
        paths.append(&mut txt_paths);
        paths
    }

    // Create a db in the history/ dir under the id
    // We will have something like:
    // history/
    //   3a54208a-f792-45c1-8505-e325aa4ce5b3/
    //     annotations.txt -> b""
    //     train/image_1.png -> b""
    //     train/image_2.png -> b""
    //     test/image_2.png -> b""
    pub fn commit(&mut self, added_files: &Vec<PathBuf>, added_dirs: &Vec<(PathBuf, usize)>, message: &str) -> Result<String, OxenError> {
        // Generate uniq id for this commit
        let commit_id = uuid::Uuid::new_v4();
        let id_str = format!("{}", commit_id);
        println!("Commit id {}", id_str);

        // Get last commit_id from the referencer
        // either copy over parent db as a starting point, or start new
        let commit_db_path = match self.referencer.head_commit_id() {
            Ok(parent_id) => {
                // We have a parent, we have to copy over last db, and continue
                let parent_commit_db_path = self.history_dir.join(Path::new(&parent_id));
                let current_commit_db_path = self.history_dir.join(Path::new(&id_str));
                FileUtil::copy_dir_all(&parent_commit_db_path, &current_commit_db_path)?;
                // return current commit path, so we can add to it
                current_commit_db_path
            }
            Err(_) => {
                // We are creating initial commit, no parent
                let commit_db_path = self.history_dir.join(Path::new(&id_str));
                if !commit_db_path.exists() {
                    std::fs::create_dir_all(&commit_db_path)?;
                }
                // return current commit path, so we can insert into it
                commit_db_path
            }
        };

        // println!("Saving commit in {:?}", commit_db_path);
        let opts = Committer::db_opts();
        let commit_db = DBWithThreadMode::open(&opts, &commit_db_path)?; //DB::open(&opts, &commit_db_path)?;

        // Commit all staged files from db
        // println!("Stager found {} files", added_files.len());
        for path in added_files.iter() {
            let path_str = path.to_str().unwrap();
            let key = path_str.as_bytes();

            // println!("Committing key {}", path_str);
            // Value is initially empty, meaning we still have to hash, but just keeping track of what is staged
            // Then when we push, we hash the file contents and save it back in here to keep track of progress
            commit_db.put(&key, b"")?;
        }

        // Commit all staged dirs from db, and recursively add all the files
        // println!("Stager found {} dirs", added_dirs.len());
        for (dir, _) in added_dirs.iter() {
            let full_path = self.repo_dir.join(dir);
            // println!(
            //     "Committer.commit({:?}) list_files_in_dir for dir {:?}",
            //     dir, full_path
            // );

            for path in self.list_files_in_dir(&full_path) {
                let relative_path = FileUtil::path_relative_to_dir(&path, &self.repo_dir)?;
                let key = relative_path.to_str().unwrap().as_bytes();

                // println!("Adding key {}", path.to_str().unwrap());
                // Value is initially empty, meaning we still have to hash, but just keeping track of what is staged
                // Then when we push, we hash the file contents and save it back in here to keep track of progress
                commit_db.put(&key, b"")?;
            }
        }

        // Create an entry in the commits_db that is the id -> CommitMsg
        //  - parent_commit_id (can be empty if root)
        //  - message
        //  - date
        //  - author
        let ref_name = self.referencer.read_head()?;
        let commit = match self.referencer.get_commit_id(&ref_name) {
            Ok(parent_id) => {
                // We have a parent
                CommitMsg {
                    id: id_str.clone(),
                    parent_id: Some(parent_id),
                    message: String::from(message),
                    author: self.auth_cfg.user.name.clone(),
                    date: Utc::now(),
                }
            }
            Err(_) => {
                // We are creating initial commit, no parent
                CommitMsg {
                    id: id_str.clone(),
                    parent_id: None,
                    message: String::from(message),
                    author: self.auth_cfg.user.name.clone(),
                    date: Utc::now(),
                }
            }
        };

        // Add to commits db
        self.add_commit_to_db(&commit)?;
        self.head_commit_db = Some(commit_db);

        Ok(id_str)
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

    pub fn get_commit_db(&self, commit_id: &str) -> Result<DBWithThreadMode<MultiThreaded>, OxenError> {
        let commit_db_path = self.history_dir.join(Path::new(&commit_id));
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Warn);
        let db = DBWithThreadMode::open(&opts, &commit_db_path)?;
        Ok(db)
    }

    pub fn get_path_hash(&self, db: &Option<DBWithThreadMode<MultiThreaded>>, path: &Path) -> Result<String, OxenError> {
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
            Err(OxenError::basic_str("Committer.get_path_hash() no commit db."))
        }
    }

    pub fn update_path_hash(&self, db: &Option<DBWithThreadMode<MultiThreaded>>, path: &Path, hash: &str) -> Result<(), OxenError> {
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
            Err(OxenError::basic_str("Committer.update_path_hash() no commit db."))
        }
    }

    pub fn add_commit_to_db(&mut self, commit: &CommitMsg) -> Result<(), OxenError> {
        // Set head db
        let ref_name = self.referencer.read_head()?;
        self.referencer.set_head(&ref_name, &commit.id)?;

        // Write commit json to db
        let commit_json = serde_json::to_string(&commit)?;
        self.commits_db.put(&commit.id, commit_json.as_bytes())?;
        Ok(())
    }

    pub fn list_commits(&self) -> Result<Vec<CommitMsg>, OxenError> {
        let mut commit_msgs: Vec<CommitMsg> = vec![];
        // Start with head, and the get parents until there are no parents
        match self.referencer.head_commit_id() {
            Ok(commit_id) => {
                self.p_list_commits(&commit_id, &mut commit_msgs)?;
                Ok(commit_msgs)
            }
            Err(_) => Err(OxenError::basic_str("No commits found.")),
        }
    }

    fn p_list_commits(
        &self,
        commit_id: &str,
        messages: &mut Vec<CommitMsg>,
    ) -> Result<(), OxenError> {
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
                        eprintln!("list_unsynced_files_for_commit Err: Could not get head commit db");
                    }
                } else {
                    let db = self.get_commit_db(&commit_id)?;
                    self.p_add_untracked_files_from_commit(&mut paths, &db);
                }
            },
            _ => {
                let db = self.get_commit_db(&commit_id)?;
                self.p_add_untracked_files_from_commit(&mut paths, &db);
            }
        };


        Ok(paths)
    }

    fn p_add_untracked_files_from_commit(&self, paths: &mut Vec<PathBuf>, db: &DBWithThreadMode<MultiThreaded>) {
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
                },
                Err(_) => {
                    eprintln!("Could not read utf8 val...")
                }
            }
        }
    }

    pub fn get_head_commit(&self) -> Result<Option<CommitMsg>, OxenError> {
        match self.referencer.head_commit_id() {
            Ok(commit_id) => Ok(self.get_commit_by_id(&commit_id)?),
            Err(_) => Ok(None),
        }
    }

    pub fn get_commit_by_id(&self, commit_id: &str) -> Result<Option<CommitMsg>, OxenError> {
        // Check if the id is in the DB
        let key = commit_id.as_bytes();
        match self.commits_db.get(key) {
            Ok(Some(value)) => {
                let commit: CommitMsg = serde_json::from_str(str::from_utf8(&*value)?)?;
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
    use crate::cli::Committer;
    use crate::error::OxenError;
    use crate::test;
    use crate::util::FileUtil;

    const BASE_DIR: &str = "data/test/runs";

    #[test]
    fn test_commit_staged() -> Result<(), OxenError> {
        let (stager, repo_path) = test::create_stager(BASE_DIR)?;
        let mut committer = Committer::new(&repo_path)?;

        let train_dir = repo_path.join("training_data");
        std::fs::create_dir_all(&train_dir)?;
        let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 1")?;
        let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 2")?;
        let _ = test::add_txt_file_to_dir(&train_dir, "Train Ex 3")?;
        let annotation_file = test::add_txt_file_to_dir(&repo_path, "some annotations...")?;

        let test_dir = repo_path.join("test_data");
        std::fs::create_dir_all(&test_dir)?;
        let _ = test::add_txt_file_to_dir(&test_dir, "Test Ex 1")?;
        let _ = test::add_txt_file_to_dir(&test_dir, "Test Ex 2")?;

        // Add a file and a directory
        stager.add_file(&annotation_file)?;
        stager.add_dir(&train_dir)?;

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
            FileUtil::path_relative_to_dir(&annotation_file, &repo_path)?;
        assert!(committer.head_contains_file(&relative_annotation_path)?);

        // Add more files and commit again, make sure the commit copied over the last one
        stager.add_dir(&test_dir)?;
        let message_2 = "Adding test data to 🐂";
        let added_files = stager.list_added_files()?;
        let added_dirs = stager.list_added_directories()?;
        let commit_id = committer.commit(&added_files, &added_dirs, message_2)?;
        stager.unstage()?;
        let commit_history = committer.list_commits()?;

        for commit in commit_history.iter() {
            println!("{:?}", commit);
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

        // cleanup
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }
}
