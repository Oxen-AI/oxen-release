use crate::cli::indexer::OXEN_HIDDEN_DIR;

use crate::cli::{Stager, Referencer};
use crate::error::OxenError;
use crate::model::CommitMsg;
use crate::util::FileUtil;

use rocksdb::{IteratorMode, DB};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;

pub const HISTORY_DIR: &str = "history";
pub const COMMITS_DB: &str = "commits";

pub struct Committer {
    commits_db: DB,
    referencer: Referencer,
    history_dir: PathBuf,
    pub repo_dir: PathBuf,
}

impl Committer {
    pub fn new(repo_dir: &Path) -> Result<Committer, OxenError> {
        let history_path = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(HISTORY_DIR)));
        let commits_path = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(COMMITS_DB)));

        if !history_path.exists() {
            std::fs::create_dir_all(&history_path)?;
        }

        Ok(Committer {
            commits_db: DB::open_default(&commits_path)?,
            referencer: Referencer::new(&repo_dir)?,
            history_dir: history_path,
            repo_dir: repo_dir.to_path_buf(),
        })
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
    pub fn commit(&self, stager: &Stager, message: &str) -> Result<String, OxenError> {
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
            },
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
        let commit_db = DB::open_default(&commit_db_path)?;

        // List all files that are staged, and write them to this db
        let added_files = stager.list_added_files()?;
        // println!("Stager found {} files", added_files.len());
        for path in added_files.iter() {
            let path_str = path.to_str().unwrap();
            let key = path_str.as_bytes();

            // println!("Committing key {}", path_str);
            // Value is initially empty, meaning we still have to hash, but just keeping track of what is staged
            // Then when we push, we hash the file contents and save it back in here to keep track of progress
            commit_db.put(&key, b"")?;
        }

        // List all the dirs that are staged, and recursively add all the files
        let added_dirs = stager.list_added_directories()?;
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
                }
            },
            Err(_) => {
                // We are creating initial commit, no parent
                CommitMsg {
                    id: id_str.clone(),
                    parent_id: None,
                    message: String::from(message),
                }
            }
        };
        
        // Update head
        self.referencer.set_head(&ref_name, &commit.id)?;

        // Write commit json to db
        let commit_json = serde_json::to_string(&commit)?;
        self.commits_db.put(&id_str, commit_json.as_bytes())?;

        // Unstage all the files at the end
        stager.unstage()?;

        Ok(id_str)
    }

    pub fn list_commits(&self) -> Result<Vec<CommitMsg>, OxenError> {
        let mut commit_msgs: Vec<CommitMsg> = vec![];
        // Start with head, and the get parents until there are no parents
        let commit_id = self.referencer.head_commit_id()?;
        // println!("list_commits start {}", commit_id);
        self.p_list_commits(&commit_id, &mut commit_msgs)?;
        Ok(commit_msgs)
    }

    fn p_list_commits(&self, commit_id: &str, messages: &mut Vec<CommitMsg>) -> Result<(), OxenError> {
        // println!("p_list_commits commit_id {}", commit_id);
        
        if let Some(commit) = self.get_commit_by_id(&commit_id)? {
            // println!("p_list_commits got commit {}", commit.message);
            messages.push(commit.clone());
            if let Some(parent_id) = &commit.parent_id {
                // println!("p_list_commits got parent {}", parent_id);
                self.p_list_commits(&parent_id, messages)?;
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
        let commit_db_path = self.history_dir.join(Path::new(&commit_id));
        let commit_db = DB::open_default(&commit_db_path)?;
        let iter = commit_db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            let key_str = str::from_utf8(&*key)?;
            let value_str = str::from_utf8(&*value)?;
            let filepath = PathBuf::from(String::from(key_str));
            println!("list_unsynced_files_for_commit ({},{})", key_str, value_str);
            // If we don't have a hash for the file as the value, it means we haven't pushed it.
            if value.is_empty() {
                paths.push(filepath);
            }
        }

        Ok(paths)
    }

    pub fn get_commit_by_id(&self, commit_id: &str) -> Result<Option<CommitMsg>, OxenError> {
        // Check if the id is in the DB
        let key = commit_id.as_bytes();
        match self.commits_db.get(key) {
            Ok(Some(value)) => {
                let commit: CommitMsg = serde_json::from_str(str::from_utf8(&*value)?)?;
                Ok(Some(commit))
            }
            Ok(None) => {
                Ok(None)
            }
            Err(err) => {
                let err = format!("Error commits_db to find commit_id {:?}\nErr: {}", commit_id, err);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn head_contains_file(&self, path: &Path) -> Result<bool, OxenError> {
         // Grab current head from referencer
         let ref_name = self.referencer.read_head()?;

         // Get commit db that contains all files
         let commit_id = self.referencer.get_commit_id(&ref_name)?;
         let commit_db_path = self.history_dir.join(Path::new(&commit_id));
         let commit_db = DB::open_default(&commit_db_path)?;
         // println!("head_contains_file path: {:?}\ndb: {:?}", path, commit_db_path);
 
         // Check if path is in this commit
         let key = path.to_str().unwrap();
         let bytes = key.as_bytes();
         match commit_db.get(bytes) {
             Ok(Some(_value)) => {
                 Ok(true)
             }
             Ok(None) => {
                 Ok(false)
             }
             Err(err) => {
                 let err = format!("Error reading db {:?}\nErr: {}", commit_db_path, err);
                 Err(OxenError::basic_str(&err))
             }
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
        let committer = Committer::new(&repo_path)?;

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
        let commit_id = committer.commit(&stager, message)?;
        let commit_history = committer.list_commits()?;

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
        let relative_annotation_path = FileUtil::path_relative_to_dir(&annotation_file, &repo_path)?;
        assert_eq!(committer.head_contains_file(&relative_annotation_path)?, true);

        // Add more files and commit again, make sure the commit copied over the last one
        stager.add_dir(&test_dir)?;
        let message_2 = "Adding test data to 🐂";
        let commit_id = committer.commit(&stager, message_2)?;
        let commit_history = committer.list_commits()?;

        assert_eq!(commit_history.len(), 2);
        assert_eq!(commit_history[0].id, commit_id);
        assert_eq!(commit_history[0].message, message_2);
        assert_eq!(committer.head_contains_file(&relative_annotation_path)?, true);

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
