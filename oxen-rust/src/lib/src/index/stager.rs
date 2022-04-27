use crate::constants;
use crate::error::OxenError;
use crate::index::Committer;
use crate::model::{LocalRepository, LocalEntry};
use crate::util;

use rocksdb::{IteratorMode, LogLevel, Options, DB};
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::str;

pub const STAGED_DIR: &str = "staged";

pub struct Stager {
    db: DB,
    pub repository: LocalRepository,
}

impl Stager {
    pub fn staging_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(STAGED_DIR))
    }

    pub fn new(repository: &LocalRepository) -> Result<Stager, OxenError> {
        let dbpath = Stager::staging_dir(&repository.path);
        std::fs::create_dir_all(&dbpath)?;
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Error);
        opts.create_if_missing(true);
        Ok(Stager {
            db: DB::open(&opts, &dbpath)?,
            repository: repository.clone(),
        })
    }

    pub fn add(&self, path: &Path, committer: &Committer) -> Result<(), OxenError> {
        if path.is_dir() {
            match self.add_dir(path, committer) {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            match self.add_file(path, committer) {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            }
        }
    }

    fn list_untracked_files_in_dir(&self, dir: &Path, committer: &Committer) -> Vec<PathBuf> {
        util::fs::recursive_eligible_files(dir)
            .into_iter()
            .map(|file| util::fs::path_relative_to_dir(&file, &self.repository.path).unwrap())
            .filter(|file| !self.file_is_in_index(file, committer))
            .collect()
    }

    fn count_untracked_files_in_dir(&self, dir: &Path, committer: &Committer) -> usize {
        let files = self.list_untracked_files_in_dir(dir, committer);
        files.len()
    }

    pub fn add_dir(&self, path: &Path, committer: &Committer) -> Result<usize, OxenError> {
        if !path.exists() {
            let err = format!("Stager.add_dir({:?}) cannot stage non-existant dir", path);
            return Err(OxenError::basic_str(&err));
        }

        let relative_path = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        let key = relative_path.to_str().unwrap().as_bytes();

        // Add all files, and get a count
        let paths: Vec<PathBuf> = self.list_untracked_files_in_dir(path, committer);

        // TODO: Find dirs and recursively add
        let count: usize = paths.len();
        self.add_dir_count(key, count)
    }

    fn add_dir_count(&self, key: &[u8], count: usize) -> Result<usize, OxenError> {
        // store count in little endian
        match self.db.put(key, count.to_le_bytes()) {
            Ok(_) => Ok(count),
            Err(err) => {
                let err = format!("Error adding key {}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn has_entry(&self, path: &Path) -> bool {
        if let Some(path_str) = path.to_str() {
            let bytes = path_str.as_bytes();
            match self.db.get_pinned(bytes) {
                Ok(Some(_value)) => {
                    true
                },
                Ok(None) => {
                    false
                }
                Err(err) => {
                    eprintln!("Stager::get_entry err: {}", err);
                    false
                }
            }
        } else {
            false
        }
    }

    pub fn get_entry(&self, path: &Path) -> Option<LocalEntry> {
        // I know this is ugly as shit.. long day, and to be honest all these matches should work so it's just.. i dont want to return the error. w/e fix later
        if let Some(path_str) = path.to_str() {
            let bytes = path_str.as_bytes();
            match self.db.get(bytes) {
                Ok(Some(value)) => {
                    // found it
                    match str::from_utf8(&*value) {
                        Ok(value) => {
                            match serde_json::from_str(value) {
                                Ok(entry) => {
                                    Some(entry)
                                },
                                Err(err) => {
                                    // could not serialize json
                                    eprintln!("get_entry could not serialize json {}", err);
                                    None
                                }
                            }
                        },
                        Err(err) => {
                            // could not convert to utf8
                            eprintln!("get_entry could not convert from utf8: {}", err);
                            None
                        }
                    }
                }
                Ok(None) => {
                    // did not get val
                    // eprintln!("get_entry did not get value");
                    None
                }
                Err(err) => {
                    eprintln!("could not fetch value from db: {}", err);
                    None
                }
            }
        } else {
            eprintln!("could not convert path to str: {:?}", path);
            None
        }
    }

    pub fn add_file(&self, path: &Path, committer: &Committer) -> Result<PathBuf, OxenError> {
        // We should have normalized to path past repo at this point
        // println!("Add file: {:?} to {:?}", path, self.repository.path);
        if !path.exists() {
            let err = format!("Stage.add_file({:?}) cannot stage non-existant file", path);
            return Err(OxenError::basic_str(&err));
        }

        // create a little meta data object to attach to file path
        let entry = LocalEntry {
            id: format!("{}", uuid::Uuid::new_v4()),
            hash: util::hasher::hash_file_contents(path)?,
            is_synced: false, // so we know to sync
            extension: String::from(path.extension().unwrap().to_str().unwrap()),
        };

        // Key is the filename relative to the repository
        // if repository: /Users/username/Datasets/MyRepo
        //   /Users/username/Datasets/MyRepo/train -> train
        //   /Users/username/Datasets/MyRepo/annotations/train.txt -> annotations/train.txt
        let path = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        let key = path.to_str().unwrap().as_bytes();

        let entry_json = serde_json::to_string(&entry)?;
        self.db.put(&key, entry_json.as_bytes())?;

        // Check if we have added the full directory,
        // if we have, remove all the individual keys
        // and add the full directory
        // println!("Checking parent of file: {:?}", path);
        if let Some(parent) = path.parent() {
            // println!("Parent {:?} is_dir {}", parent, parent.is_dir());
            if parent != Path::new("") {
                let full_path = self.repository.path.join(parent);
                // println!("Getting count for parent {:?} full path: {:?}", parent, full_path);
                let untracked_files = self.list_untracked_files_in_dir(&full_path, committer);
                // println!("Got {} untracked files", untracked_files.len());
                if untracked_files.is_empty() {
                    let to_remove = self.list_keys_with_prefix(parent.to_str().unwrap())?;
                    let count = to_remove.len();
                    // println!("Remove {} keys", to_remove.len());
                    for key in to_remove.iter() {
                        match self.db.delete(key) {
                            Ok(_) => {
                                // println!("Deleted key: {}", key);
                            }
                            Err(err) => {
                                eprintln!("Unable to delete key [{}] err: {}", key, err);
                            }
                        }
                    }

                    let key = parent.to_str().unwrap().as_bytes();
                    self.add_dir_count(key, count)?;
                }
            }
        }

        Ok(path)
    }

    fn list_keys_with_prefix(&self, path: &str) -> Result<Vec<String>, OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut keys: Vec<String> = vec![];
        for (key, _) in iter {
            let key = String::from(str::from_utf8(&*key)?);
            if key.starts_with(path) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    pub fn list_added_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut paths: Vec<PathBuf> = vec![];
        for (key, _) in iter {
            let local_path = PathBuf::from(String::from(str::from_utf8(&*key)?));
            let full_path = self.repository.path.join(&local_path);
            if full_path.is_file() {
                paths.push(local_path);
            }
        }
        Ok(paths)
    }

    pub fn list_added_directories(&self) -> Result<Vec<(PathBuf, usize)>, OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut paths: Vec<(PathBuf, usize)> = vec![];
        for (key, value) in iter {
            let local_path = PathBuf::from(String::from(str::from_utf8(&*key)?));
            let full_path = self.repository.path.join(&local_path);
            if full_path.is_dir() {
                match self.convert_usize_slice(&*value) {
                    Ok(size) => {
                        paths.push((local_path, size));
                    }
                    Err(err) => {
                        eprintln!(
                            "Could not convert data attached to: {:?}\nErr:{}",
                            full_path, err
                        )
                    }
                }
            }
        }
        Ok(paths)
    }

    pub fn list_untracked_files(&self, committer: &Committer) -> Result<Vec<PathBuf>, OxenError> {
        // We just look at the top level here for summary..not recursively right now

        let dir_entries = std::fs::read_dir(&self.repository.path)?;
        // println!("Listing untracked files from {:?}", dir_entries);

        let mut paths: Vec<PathBuf> = vec![];
        for entry in dir_entries {
            let local_path = entry?.path();
            if local_path.is_file() {
                // Return relative path with respect to the repo
                let relative_path =
                    util::fs::path_relative_to_dir(&local_path, &self.repository.path)?;
                if committer.file_is_committed(&relative_path) {
                    continue;
                }

                // println!("Checking if we have the key? {:?}", relative_path);
                if let Some(path_str) = relative_path.to_str() {
                    let bytes = path_str.as_bytes();
                    match self.db.get(bytes) {
                        Ok(Some(_value)) => {
                            // already added
                            // println!("got value: {:?}", value);
                        }
                        Ok(None) => {
                            // did not get val
                            // println!("untracked! {:?}", relative_path);
                            paths.push(relative_path);
                        }
                        Err(err) => {
                            eprintln!("{}", err);
                        }
                    }
                }
            }
        }

        Ok(paths)
    }

    fn file_is_in_index(&self, path: &Path, committer: &Committer) -> bool {
        if self.has_entry(path) {
            // we have it in our staged db
            true
        } else {
            // it is committed
            committer.file_is_committed(path)
        }
    }

    pub fn list_untracked_directories(
        &self,
        committer: &Committer,
    ) -> Result<Vec<(PathBuf, usize)>, OxenError> {
        // println!("list_untracked_directories {:?}", self.repository.path);
        let dir_entries = std::fs::read_dir(&self.repository.path)?;

        let mut paths: Vec<(PathBuf, usize)> = vec![];
        for entry in dir_entries {
            let path = entry?.path();
            // println!("list_untracked_directories considering {:?}", path);
            if path.is_dir() {
                let relative_path = util::fs::path_relative_to_dir(&path, &self.repository.path)?;
                // println!("list_untracked_directories relative {:?}", relative_path);

                if committer.file_is_committed(&relative_path) {
                    continue;
                }

                if let Some(path_str) = relative_path.to_str() {
                    if path_str.contains(constants::OXEN_HIDDEN_DIR) {
                        continue;
                    }

                    let bytes = path_str.as_bytes();
                    match self.db.get(bytes) {
                        Ok(Some(_value)) => {
                            // already added
                            // println!("got value: {:?}", value);
                        }
                        Ok(None) => {
                            // did not get val
                            // println!("list_untracked_directories get file count in: {:?}", path);

                            // TODO: Speed this up, maybe we are opening and closing the db too many times
                            // example: adding and committing a 12500 files, then checking status
                            let count = self.count_untracked_files_in_dir(&path, committer);
                            paths.push((relative_path, count));
                        }
                        Err(err) => {
                            eprintln!("{}", err);
                        }
                    }
                }
            }
        }

        Ok(paths)
    }

    pub fn unstage(&self) -> Result<(), OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        for (key, _) in iter {
            self.db.delete(key)?;
        }
        Ok(())
    }

    fn convert_usize_slice(&self, slice: &[u8]) -> Result<usize, OxenError> {
        match <[u8; 8]>::try_from(slice) {
            Ok(data) => {
                let size: usize = usize::from_le_bytes(data);
                Ok(size)
            }
            Err(err) => {
                let err = format!("Unable to convert data to usize: {:?}\nErr: {}", slice, err);
                Err(OxenError::basic_str(&err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::index::Committer;
    use crate::test;
    use crate::util;

    use std::path::{Path, PathBuf};

    #[test]
    fn test_stager_add_file() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            // Write a file to disk
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            // Add the file
            let path = stager.add_file(&hello_file, &committer)?;

            // Make sure we saved the relative path
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;
            assert_eq!(path, relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_unstage() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            // Add a file and a directory
            stager.add_file(&hello_file, &committer)?;
            stager.add_dir(&sub_dir, &committer)?;

            // Make sure the counts start as 1
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 1);
            let dirs = stager.list_added_directories()?;
            assert_eq!(dirs.len(), 1);

            // Unstage
            stager.unstage()?;

            // There should no longer be any added files
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 0);
            let dirs = stager.list_added_directories()?;
            assert_eq!(dirs.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_add_twice_only_adds_once() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            // Make sure we have a valid file
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            // Add it twice
            stager.add_file(&hello_file, &committer)?;
            stager.add_file(&hello_file, &committer)?;

            // Make sure we still only have it once
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_add_non_existant_file() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            let hello_file = PathBuf::from("non-existant.txt");
            if stager.add_file(&hello_file, &committer).is_ok() {
                // we don't want to be able to add this file
                panic!("test_add_non_existant_file() Cannot stage non-existant file")
            }

            Ok(())
        })
    }

    #[test]
    fn test_add_directory() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            // Write two files to directories
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            match stager.add_dir(&sub_dir, &committer) {
                Ok(num_files) => {
                    assert_eq!(2, num_files);
                }
                Err(err) => {
                    panic!("test_add_directory() Should have returned path... {}", err)
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_stager_get_entry() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;

            // Stage file
            stager.add_file(&hello_file, &committer)?;

            // we should be able to fetch this entry json
            let entry = stager.get_entry(&relative_path).unwrap();
            assert!(!entry.id.is_empty());
            assert!(!entry.hash.is_empty());
            assert!(!entry.is_synced);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;

            // Stage file
            stager.add_file(&hello_file, &committer)?;

            // List files
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 1);
            
            assert_eq!(files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_file_in_sub_dir() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let sub_file = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            stager.add_file(&sub_file, &committer)?;

            // List files
            let files = stager.list_added_files()?;

            // There is one file
            assert_eq!(files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(&sub_file, repo_path)?;
            assert_eq!(files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_file_in_sub_dir_updates_untracked_count() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;
            let sub_file = test::add_txt_file_to_dir(&sub_dir, "Hello 3")?;

            let dirs = stager.list_untracked_directories(&committer)?;
            // There is one directory
            assert_eq!(dirs.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(&sub_dir, repo_path)?;
            assert_eq!(dirs[0].0, relative_path);

            // With three untracked files
            assert_eq!(dirs[0].1, 3);

            // Then we add one file
            stager.add_file(&sub_file, &committer)?;

            // There are still two untracked files in the dir
            let dirs = stager.list_untracked_directories(&committer)?;
            assert_eq!(dirs.len(), 1);
            assert_eq!(dirs[0].0, relative_path);

            // With two files
            assert_eq!(dirs[0].1, 2);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_all_files_in_sub_dir() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;
            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let sub_file_1 = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let sub_file_2 = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;
            let sub_file_3 = test::add_txt_file_to_dir(&sub_dir, "Hello 3")?;

            let dirs = stager.list_untracked_directories(&committer)?;

            // There is one directory
            assert_eq!(dirs.len(), 1);
            // With three untracked files
            assert_eq!(dirs[0].1, 3);

            // Then we add all three
            stager.add_file(&sub_file_1, &committer)?;
            stager.add_file(&sub_file_2, &committer)?;
            stager.add_file(&sub_file_3, &committer)?;

            // There now there are no untracked directories
            let untracked_dirs = stager.list_untracked_directories(&committer)?;
            assert_eq!(untracked_dirs.len(), 0);

            // And there is one tracked directory
            let added_dirs = stager.list_added_directories()?;
            assert_eq!(added_dirs.len(), 1);
            assert_eq!(added_dirs[0].1, 3);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_directories() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            stager.add_dir(&sub_dir, &committer)?;

            // List files
            let dirs = stager.list_added_directories()?;

            // There is one directory
            assert_eq!(dirs.len(), 1);
            assert_eq!(dirs[0].0, Path::new("training_data"));

            // With two files
            assert_eq!(dirs[0].1, 2);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello 1")?;

            // Do not add...

            // List files
            let files = stager.list_untracked_files(&committer)?;
            assert_eq!(files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;
            assert_eq!(files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_dirs() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            // Do not add...

            // List files
            let files = stager.list_untracked_directories(&committer)?;
            assert_eq!(files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_one_untracked_directory() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            // Do not add...

            // List files
            let files = stager.list_untracked_directories(&committer)?;

            // There is one directory
            assert_eq!(files.len(), 1);

            // With two files
            assert_eq!(files[0].1, 2);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_directories_after_add() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let committer = Committer::new(&stager.repository)?;

            // Create 2 sub directories, one with  Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let train_dir = repo_path.join("train");
            std::fs::create_dir_all(&train_dir)?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/cat_1.jpg"))?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/dog_1.jpg"))?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/cat_2.jpg"))?;
            let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/dog_2.jpg"))?;

            let test_dir = repo_path.join("test");
            std::fs::create_dir_all(&test_dir)?;
            let _ = test::add_img_file_to_dir(&test_dir, Path::new("data/test/images/cat_3.jpg"))?;
            let _ = test::add_img_file_to_dir(&test_dir, Path::new("data/test/images/dog_3.jpg"))?;

            let valid_dir = repo_path.join("valid");
            std::fs::create_dir_all(&valid_dir)?;
            let _ = test::add_img_file_to_dir(&valid_dir, Path::new("data/test/images/dog_4.jpg"))?;

            let base_file_1 = test::add_txt_file_to_dir(repo_path, "Hello 1")?;
            let _base_file_2 = test::add_txt_file_to_dir(repo_path, "Hello 2")?;
            let _base_file_3 = test::add_txt_file_to_dir(repo_path, "Hello 3")?;

            // At first there should be 3 untracked
            let untracked_dirs = stager.list_untracked_directories(&committer)?;
            assert_eq!(untracked_dirs.len(), 3);

            // Add the directory
            let _ = stager.add_dir(&train_dir, &committer)?;
            // Add one file
            let _ = stager.add_file(&base_file_1, &committer)?;

            // List the files
            let added_files = stager.list_added_files()?;
            let added_dirs = stager.list_added_directories()?;
            let untracked_files = stager.list_untracked_files(&committer)?;
            let untracked_dirs = stager.list_untracked_directories(&committer)?;

            // There is 1 added file and 1 added dir
            assert_eq!(added_files.len(), 1);
            assert_eq!(added_dirs.len(), 1);

            // There are 2 untracked files at the top level
            assert_eq!(untracked_files.len(), 2);
            // There are 2 untracked dirs at the top level
            assert_eq!(untracked_dirs.len(), 2);

            Ok(())
        })
    }
}
