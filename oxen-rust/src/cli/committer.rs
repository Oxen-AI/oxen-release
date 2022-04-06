use crate::cli::indexer::OXEN_HIDDEN_DIR;

use crate::error::OxenError;
use crate::cli::Stager;
use crate::util::FileUtil;
use crate::model::CommitMsg;

use rocksdb::{IteratorMode, DB};
use std::path::{Path, PathBuf};
use std::str;
use std::collections::HashSet;

pub const HISTORY_DIR: &str = "history";
pub const COMMITS_DB: &str = "commits";

pub struct Committer {
    commits_db: DB,
    history_dir: PathBuf,
    repo_dir: PathBuf,
}

impl Committer {
    pub fn new(repo_dir: &Path) -> Result<Committer, OxenError> {
        let history_dir = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(HISTORY_DIR)));
        let commits_dir = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(COMMITS_DB)));

        if !history_dir.exists() {
            std::fs::create_dir_all(&history_dir)?;
        }

        Ok(Committer {
            commits_db: DB::open_default(&commits_dir)?,
            history_dir: history_dir,
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

    pub fn commit(&self, stager: &Stager, message: &str) -> Result<String, OxenError> {
        // Generate uniq id for this commit
        let commit_id = uuid::Uuid::new_v4();
        let id_str = format!("{}", commit_id);
        println!("Commit id {}", id_str);

        // Create a db in the history/ dir under the id
        // We will have something like:
        // history/
        //   3a54208a-f792-45c1-8505-e325aa4ce5b3/
        //     annotations.txt -> b""
        //     train/image_1.png -> b""
        //     train/image_2.png -> b""
        //     test/image_2.png -> b""
        let commit_db_path = self.history_dir.join(Path::new(&id_str));
        if !commit_db_path.exists() {
            std::fs::create_dir_all(&commit_db_path)?;
        }

        println!("Saving commit in {:?}", commit_db_path);
        let commit_db = DB::open_default(&commit_db_path)?;

        // List all files that are staged, and write them to this db
        let added_files = stager.list_added_files()?;
        println!("Stager found {} files", added_files.len());
        for path in added_files.iter() {
            let path_str = path.to_str().unwrap();
            let key = path_str.as_bytes();

            println!("Committing key {}", path_str);
            // Value is initially empty, meaning we still have to hash, but just keeping track of what is staged
            // Then when we push, we hash the file contents and save it back in here to keep track of progress
            commit_db.put(&key, b"")?;
        }

        // List all the dirs that are staged, and recursively add all the files
        let added_dirs = stager.list_added_directories()?;
        println!("Stager found {} dirs", added_dirs.len());
        for (dir, _) in added_dirs.iter() {
            let full_path = self.repo_dir.join(dir);
            println!("Committer.commit({:?}) list_files_in_dir for dir {:?}", dir, full_path);
            
            for path in self.list_files_in_dir(&full_path) {
                let relative_path = FileUtil::path_relative_to_dir(&path, &self.repo_dir)?;
                let key = relative_path.to_str().unwrap().as_bytes();

                // println!("Adding key {}", path.to_str().unwrap());
                // Value is initially empty, meaning we still have to hash, but just keeping track of what is staged
                // Then when we push, we hash the file contents and save it back in here to keep track of progress
                commit_db.put(&key, b"")?;
            }
        }

        // Create an entry in the commits_db that is the id -> message
        // TODO: Make a json struct with more info
        //  - Message
        //  - UTC date
        //  - Author
        self.commits_db.put(&id_str, message.as_bytes())?;

        // Unstage all the files at the end
        stager.unstage()?;

        Ok(id_str)
    }

    pub fn list_commits(&self) -> Result<Vec<CommitMsg>, OxenError> {
        let mut commit_msgs: Vec<CommitMsg> = vec![];
        let iter = self.commits_db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            let commit_id = String::from(str::from_utf8(&*key)?);
            let commit_message = String::from(str::from_utf8(&*value)?);
            commit_msgs.push(CommitMsg { 
                id: commit_id,
                message: commit_message,
            });
        }

        Ok(commit_msgs)
    }

    pub fn list_unsynced_files_for_commit(&self, commit_id: &str) -> Result<Vec<PathBuf>, OxenError> {
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
}

#[cfg(test)]
mod tests {
    use crate::cli::Committer;
    use crate::error::OxenError;
    use crate::test;

    const BASE_DIR: &str = "data/test/runs";

    #[test]
    fn test_commit_staged() -> Result<(), OxenError> {
        let (stager, repo_path) = test::create_stager(BASE_DIR)?;
        let committer = Committer::new(&repo_path)?;

        let sub_dir = repo_path.join("training_data");
        std::fs::create_dir_all(&sub_dir)?;
        let _ = test::add_txt_file_to_dir(&sub_dir, "Train Ex 1")?;
        let _ = test::add_txt_file_to_dir(&sub_dir, "Train Ex 2")?;
        let hello_file = test::add_txt_file_to_dir(&repo_path, "Hello World")?;
    
        // Add a file and a directory
        stager.add_file(&hello_file)?;
        stager.add_dir(&sub_dir)?;

        let message = "Plowing through all the data 🐂";
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
        assert_eq!(files.len(), 3);

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
