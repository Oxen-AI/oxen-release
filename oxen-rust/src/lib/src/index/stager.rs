use crate::constants;
use crate::error::OxenError;
use crate::index::committer::VERSIONS_DIR;
use crate::index::Committer;
use crate::model::{CommitEntry, LocalRepository, StagedData, StagedEntry, StagedEntryStatus};
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
        if path.to_str().unwrap().to_string().contains(".oxen") {
            return Ok(());
        }

        // println!("stager.add({:?})", path);

        if path == Path::new(".") {
            for entry in (std::fs::read_dir(path)?).flatten() {
                let path = entry.path();
                let entry_path = self.repository.path.join(&path);
                self.add(&entry_path, committer)?;
            }
            // println!("ADD CURRENT DIR: {:?}", path);
            return Ok(());
        }

        // If it doesn't exist on disk, we can't tell if it is a file or dir
        // so we have to check if it is committed, and what the backup version is
        if !path.exists() {
            let relative_path = util::fs::path_relative_to_dir(path, &self.repository.path)?;
            // println!("Stager.add() checking relative path: {:?}", relative_path);
            // Since entries that are committed are only files.. we will have to have different logic for dirs
            if let Ok(Some(value)) = committer.get_entry(&relative_path) {
                self.add_removed_file(&relative_path, &value)?;
                return Ok(());
            }

            let files_in_dir = committer.list_head_files_from_dir(&relative_path);
            if !files_in_dir.is_empty() {
                for entry in files_in_dir.iter() {
                    self.add_removed_file(&entry.path, entry)?;
                }

                return Ok(());
            }
        }

        // println!("Stager.add() is_dir? {} path: {:?}", path.is_dir(), path);
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

    pub fn status(&self, committer: &Committer) -> Result<StagedData, OxenError> {
        // TODO: let's do this in a single loop and filter model
        // println!("status: before list_added_directories");
        let added_dirs = self.list_added_directories()?;
        // println!("status: list_added_files");
        let added_files = self.list_added_files()?;
        // println!("status: list_untracked_directories");
        let untracked_dirs = self.list_untracked_directories(committer)?;
        // println!("status: list_untracked_files");
        let untracked_files = self.list_untracked_files(committer)?;
        // println!("status: list_modified_files");
        let modified_files = self.list_modified_files(committer)?;
        // println!("status: list_removed_files");
        let removed_files = self.list_removed_files(committer)?;
        // println!("status: ok");
        let status = StagedData {
            added_dirs,
            added_files,
            untracked_dirs,
            untracked_files,
            modified_files,
            removed_files,
        };
        Ok(status)
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

    fn add_removed_file(
        &self,
        repo_path: &Path,
        entry: &CommitEntry,
    ) -> Result<StagedEntry, OxenError> {
        let version_dir =
            util::fs::oxen_hidden_dir(&self.repository.path).join(Path::new(VERSIONS_DIR));
        let version_path = version_dir.join(&entry.id).join(entry.filename());
        if !version_path.exists() {
            eprintln!("Version file not found: {:?}", version_path);
            let err = format!("Stager.add_removed_file({:?}) cannot stage non-existant file\npath: {:?}\nentry: {:?}", version_path, repo_path, entry);
            return Err(OxenError::basic_str(&err));
        }

        let entry = StagedEntry {
            id: entry.id.clone(),
            hash: entry.hash.clone(),
            status: StagedEntryStatus::Removed,
        };

        let key = repo_path.to_str().unwrap();
        let entry_json = serde_json::to_string(&entry)?;

        // println!("Stager.add_removed_file {} -> {}", key, entry_json);
        self.db.put(&key, entry_json.as_bytes())?;

        Ok(entry)
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
            // TODO: is there a faster way to check a key? "key_may_exist" sounds sketchy what does may mean?
            match self.db.get_pinned(bytes) {
                Ok(Some(_value)) => true,
                Ok(None) => false,
                Err(err) => {
                    eprintln!("Stager::get_entry err: {}", err);
                    false
                }
            }
        } else {
            false
        }
    }

    pub fn get_entry(&self, path: &Path) -> Option<StagedEntry> {
        if let Some(path_str) = path.to_str() {
            let bytes = path_str.as_bytes();
            match self.db.get(bytes) {
                Ok(Some(value)) => {
                    // found it
                    match str::from_utf8(&*value) {
                        Ok(value) => {
                            match serde_json::from_str(value) {
                                Ok(entry) => Some(entry),
                                Err(err) => {
                                    // could not serialize json
                                    eprintln!("get_entry could not serialize json {}", err);
                                    None
                                }
                            }
                        }
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
                    eprintln!("Err could not fetch value from db: {}", err);
                    None
                }
            }
        } else {
            None
        }
    }

    pub fn add_file(&self, path: &Path, committer: &Committer) -> Result<PathBuf, OxenError> {
        // We should have normalized to path past repo at this point
        // println!("Add file: {:?} to {:?}", path, self.repository.path);
        if !path.exists() {
            let err = format!("Stager.add_file({:?}) cannot stage non-existant file", path);
            return Err(OxenError::basic_str(&err));
        }

        // create a little meta data object to attach to file path
        let filename_str = path.to_str().unwrap();
        let id = util::hasher::hash_buffer(filename_str.as_bytes());
        let hash = util::hasher::hash_file_contents(path)?;

        // Key is the filename relative to the repository
        // if repository: /Users/username/Datasets/MyRepo
        //   /Users/username/Datasets/MyRepo/train -> train
        //   /Users/username/Datasets/MyRepo/annotations/train.txt -> annotations/train.txt
        let path = util::fs::path_relative_to_dir(path, &self.repository.path)?;

        if let Ok(Some(entry)) = committer.get_entry(&path) {
            if entry.hash == hash {
                // file has not changed, don't add it
                return Ok(path);
            }
        }

        let entry = StagedEntry {
            id,
            hash,
            status: StagedEntryStatus::Added,
        };

        let key = path.to_str().unwrap();
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

    pub fn list_added_files(&self) -> Result<Vec<(PathBuf, StagedEntry)>, OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut paths: Vec<(PathBuf, StagedEntry)> = vec![];
        for (key, value) in iter {
            match (str::from_utf8(&*key), str::from_utf8(&*value)) {
                (Ok(key), Ok(value)) => {
                    // println!("list_added_files reading key [{}] value [{}]", key, value);
                    let local_path = PathBuf::from(String::from(key));
                    let entry: Result<StagedEntry, serde_json::error::Error> =
                        serde_json::from_str(value);
                    if let Ok(entry) = entry {
                        paths.push((local_path, entry));
                    }
                }
                (Ok(_key), _) => {
                    // This is fine because it's a directory with a count at the end
                    // eprintln!("list_added_files() Could not values for key {}.", key)
                }
                (_, Ok(val)) => {
                    // This shouldn't happen
                    eprintln!("list_added_files() Could not key for value {}.", val)
                }
                _ => {
                    // This shouldn't happen
                    eprintln!("list_added_files() Could not decoded keys and values.")
                }
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

    pub fn list_removed_files(&self, committer: &Committer) -> Result<Vec<PathBuf>, OxenError> {
        // TODO: We are looping multiple times to check whether file is added,modified,or removed, etc
        //       We should do this loop once, and check each thing
        let mut paths: Vec<PathBuf> = vec![];
        for short_path in committer.list_files_in_head_commit_db()? {
            let path = self.repository.path.join(&short_path);
            if !path.exists() && !self.has_entry(&short_path) {
                paths.push(short_path);
            }
        }
        Ok(paths)
    }

    pub fn list_modified_files(&self, committer: &Committer) -> Result<Vec<PathBuf>, OxenError> {
        // TODO: We are looping multiple times to check whether file is added,modified,or removed, etc
        //       We should do this loop once, and check each thing
        let dir_entries = std::fs::read_dir(&self.repository.path)?;

        let mut paths: Vec<PathBuf> = vec![];
        for entry in dir_entries {
            let local_path = entry?.path();
            if local_path.is_file() {
                // Return relative path with respect to the repo
                let relative_path =
                    util::fs::path_relative_to_dir(&local_path, &self.repository.path)?;

                if self.has_entry(&relative_path) {
                    continue;
                }

                // Check if we have the entry in the head commit
                if let Ok(Some(old_entry)) = committer.get_entry(&relative_path) {
                    // Check if the old_entry has changed
                    let current_hash = util::hasher::hash_file_contents(&local_path)?;
                    if current_hash != old_entry.hash {
                        paths.push(relative_path);
                    }
                }
            }
        }

        Ok(paths)
    }

    pub fn list_untracked_files(&self, committer: &Committer) -> Result<Vec<PathBuf>, OxenError> {
        // TODO: We just look at the top level here for summary..not recursively right now
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
                if !self.has_entry(&relative_path) {
                    paths.push(relative_path);
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
                            if count > 0 {
                                paths.push((relative_path, count));
                            }
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
    use crate::index::{Committer, Stager};
    use crate::model::StagedEntryStatus;
    use crate::test;
    use crate::util;

    use std::path::{Path, PathBuf};

    #[test]
    fn test_1_stager_add_file() -> Result<(), OxenError> {
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
    fn test_cannot_add_if_no_difference_than_commit() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let mut committer = Committer::new(&stager.repository)?;

            // Make sure we have a valid file
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            // Add it
            stager.add_file(&hello_file, &committer)?;

            // Commit it
            let status = stager.status(&committer)?;
            committer.commit(&status, "Add Hello World")?;
            stager.unstage()?;

            // try to add it again
            stager.add_file(&hello_file, &committer)?;

            // make sure we don't have it added again, because the hash hadn't changed since last commit
            let status = stager.status(&committer)?;
            assert_eq!(status.added_files.len(), 0);

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
            assert_eq!(entry.status, StagedEntryStatus::Added);

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

            assert_eq!(files[0].0, relative_path);

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
            assert_eq!(files[0].0, relative_path);

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
    fn test_stager_list_modified_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager| {
            // Create committer with no commits
            let mut committer = Committer::new(&stager.repository)?;
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello 1")?;

            // add the file
            stager.add_file(&hello_file, &committer)?;

            // commit the file
            let status = stager.status(&committer)?;
            committer.commit(&status, "added hello 1")?;
            stager.unstage()?;

            let mod_files = stager.list_modified_files(&committer)?;
            assert_eq!(mod_files.len(), 0);

            // modify the file
            let hello_file = test::modify_txt_file(hello_file, "Hello 2")?;

            // List files
            let mod_files = stager.list_modified_files(&committer)?;
            assert_eq!(mod_files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;
            assert_eq!(mod_files[0], relative_path);

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

            // Must have some sort of file in the dir to add it.
            test::write_txt_file_to_path(sub_dir.join("hi.txt"), "Hi")?;

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
    fn test_stager_add_dir_recursive() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let stager = Stager::new(&repo)?;
            let committer = Committer::new(&repo)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let annotations_dir = repo_path.join("annotations");

            // Add the directory which has the structure
            // annotations/
            //   train/
            //     annotations.txt
            //     one_shot.txt
            //   test/
            //     annotations.txt
            stager.add(&annotations_dir, &committer)?;

            // List dirs
            let dirs = stager.list_added_directories()?;

            // There is one directory
            assert_eq!(dirs.len(), 1);

            // With 3 recursive files
            assert_eq!(dirs[0].1, 3);

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
