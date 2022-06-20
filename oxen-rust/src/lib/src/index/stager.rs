use crate::constants;
use crate::db;
use crate::error::OxenError;
use crate::index::CommitEntryReader;
use crate::model::{
    CommitEntry,
    LocalRepository,
    StagedData,
    StagedDirStats,
    StagedEntry,
    StagedEntryStatus
};
use crate::util;

use rocksdb::{IteratorMode, DB};
use std::collections::HashSet;
// use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::str;
use std::fs;
use filetime::FileTime;

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
        log::debug!("Stager db_path {:?}", dbpath);
        if !dbpath.exists() {
            std::fs::create_dir_all(&dbpath)?;
        }
        let opts = db::opts::default();
        Ok(Stager {
            db: DB::open(&opts, &dbpath)?,
            repository: repository.clone(),
        })
    }

    pub fn add(&self, path: &Path, commit_reader: &CommitEntryReader) -> Result<(), OxenError> {
        if path.to_str().unwrap().to_string().contains(constants::OXEN_HIDDEN_DIR) {
            return Ok(());
        }

        log::debug!("stager.add({:?})", path);

        if path == Path::new(".") {
            for entry in (std::fs::read_dir(path)?).flatten() {
                let path = entry.path();
                let entry_path = self.repository.path.join(&path);
                self.add(&entry_path, commit_reader)?;
            }
            log::debug!("ADD CURRENT DIR: {:?}", path);
            return Ok(());
        }

        // If it doesn't exist on disk, we can't tell if it is a file or dir
        // so we have to check if it is committed, and what the backup version is
        if !path.exists() {
            let relative_path = util::fs::path_relative_to_dir(path, &self.repository.path)?;
            // println!("Stager.add() checking relative path: {:?}", relative_path);
            // Since entries that are committed are only files.. we will have to have different logic for dirs
            if let Ok(Some(value)) = commit_reader.get_entry(&relative_path) {
                self.add_removed_file(&relative_path, &value)?;
                return Ok(());
            }

            let files_in_dir = commit_reader.list_files_from_dir(&relative_path);
            if !files_in_dir.is_empty() {
                for entry in files_in_dir.iter() {
                    self.add_removed_file(&entry.path, entry)?;
                }

                return Ok(());
            }
        }

        log::debug!("Stager.add() is_dir? {} path: {:?}", path.is_dir(), path);
        if path.is_dir() {
            match self.add_dir(path, commit_reader) {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            match self.add_file(path, commit_reader) {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            }
        }
    }

    pub fn status(&self, entry_reader: &CommitEntryReader) -> Result<StagedData, OxenError> {
        // TODO: let's do this in a single loop and filter model
        log::debug!("STATUS: before list_added_directories");
        let added_dirs = self.list_added_directories()?;
        log::debug!("STATUS: list_added_files");
        let added_files = self.list_added_files()?;
        log::debug!("STATUS: list_untracked_directories");
        let untracked_dirs = self.list_untracked_directories(entry_reader)?;
        log::debug!("STATUS: list_untracked_files");
        let untracked_files = self.list_untracked_files(entry_reader)?;
        log::debug!("STATUS: list_modified_files");
        let modified_files = self.list_modified_files(entry_reader)?;
        log::debug!("STATUS: list_removed_files");
        let removed_files = self.list_removed_files(entry_reader)?;
        log::debug!("STATUS: ok");
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

    fn list_untracked_files_in_dir(&self, dir: &Path, entry_reader: &CommitEntryReader) -> Vec<PathBuf> {
        util::fs::recursive_eligible_files(dir)
            .into_iter()
            .map(|file| util::fs::path_relative_to_dir(&file, &self.repository.path).unwrap())
            .filter(|file| {
                log::debug!("list_untracked_files_in_dir {:?}", file);
                !self.file_is_in_index(file, entry_reader)
            })
            .collect()
    }

    fn count_untracked_files_in_dir(&self, dir: &Path, entry_reader: &CommitEntryReader) -> usize {
        let files = self.list_untracked_files_in_dir(dir, entry_reader);
        files.len()
    }

    fn add_removed_file(
        &self,
        repo_path: &Path,
        entry: &CommitEntry,
    ) -> Result<StagedEntry, OxenError> {
        let entry = StagedEntry {
            id: entry.id.clone(),
            hash: entry.hash.clone(),
            status: StagedEntryStatus::Removed,
        };

        let key = repo_path.to_str().unwrap();
        let entry_json = serde_json::to_string(&entry)?;
        self.db.put(&key, entry_json.as_bytes())?;

        Ok(entry)
    }

    pub fn add_dir(&self, path: &Path, entry_reader: &CommitEntryReader) -> Result<(), OxenError> {
        if !path.exists() {
            let err = format!("Cannot stage non-existant dir: {:?}", path);
            return Err(OxenError::basic_str(&err));
        }

        // Add all untracked files and modified files
        let mut paths = self.list_untracked_files_in_dir(path, entry_reader);
        let mut modified_paths = self.list_modified_files(entry_reader)?;
        paths.append(&mut modified_paths);
        
        log::debug!("Stager.add_dir {:?} -> {}", path, paths.len());
        
        for path in paths.iter() {
            let full_path = self.repository.path.join(path);
            self.add_file(&full_path, entry_reader)?;
        }

        Ok(())
    }

    pub fn has_entry(&self, path: &Path) -> bool {
        if let Some(path_str) = path.to_str() {
            let bytes = path_str.as_bytes();
            match self.db.get(bytes) {
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

    pub fn add_file(&self, path: &Path, entry_reader: &CommitEntryReader) -> Result<PathBuf, OxenError> {
        // We should have normalized to path past repo at this point
        // println!("Add file: {:?} to {:?}", path, self.repository.path);
        if !path.exists() {
            let err = format!("Err cannot stage non-existant file: {:?}", path);
            return Err(OxenError::basic_str(&err));
        }

        // compute the hash to know if it has changed
        let hash = util::hasher::hash_file_contents(&path)?;

        // Key is the filename relative to the repository
        // if repository: /Users/username/Datasets/MyRepo
        //   /Users/username/Datasets/MyRepo/train -> train
        //   /Users/username/Datasets/MyRepo/annotations/train.txt -> annotations/train.txt
        let path = util::fs::path_relative_to_dir(path, &self.repository.path)?;

        log::debug!("add_file hash_filename: {:?}", path);
        let id = util::hasher::hash_filename(&path);
        let mut staged_entry = StagedEntry {
            id,
            hash: hash.to_owned(),
            status: StagedEntryStatus::Added,
        };

        if let Ok(Some(entry)) = entry_reader.get_entry(&path) {
            if entry.hash == hash {
                // file has not changed, don't add it
                log::debug!("add_file do not add file, it hasn't changed: {:?}", path);
                return Ok(path);
            } else {
                // Hash doesn't match, mark it as modified
                staged_entry.status = StagedEntryStatus::Modified;
            }
        }

        let key = path.to_str().unwrap();
        let entry_json = serde_json::to_string(&staged_entry)?;
        self.db.put(&key, entry_json.as_bytes())?;

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

    pub fn list_added_directories(&self) -> Result<HashSet<StagedDirStats>, OxenError> {
        let mut paths: HashSet<StagedDirStats> = HashSet::new();
        for entry in fs::read_dir(&self.repository.path)? {
            let entry = entry?;
            let path = entry.path();

            // Only consider directories
            if !path.is_dir() {
                continue;
            }

            // Relative path will be stored
            let relative_path = util::fs::path_relative_to_dir(&path, &self.repository.path)?;
            let path_str = relative_path.to_str().unwrap();

            // Don't add .oxen dir
            if path_str.contains(constants::OXEN_HIDDEN_DIR) {
                continue;
            }

            // Count in db from relative path
            let num_added = self.list_keys_with_prefix(&path_str)?.len();

            // Make sure we have some files added
            if num_added == 0 {
                continue;
            }

            // Count in fs from full path
            let total = util::fs::rcount_files_in_dir(&path);
            
            let dir = StagedDirStats {
                path: relative_path,
                num_files_staged: num_added,
                total_files: total,
            };
            
            log::debug!("list_added_directories: {:?}", dir);
            paths.insert(dir);
        }
        Ok(paths)
    }

    pub fn list_removed_files(&self, entry_reader: &CommitEntryReader) -> Result<Vec<PathBuf>, OxenError> {
        // TODO: We are looping multiple times to check whether file is added,modified,or removed, etc
        //       We should do this loop once, and check each thing
        let mut paths: Vec<PathBuf> = vec![];
        for short_path in entry_reader.list_files()? {
            let path = self.repository.path.join(&short_path);
            if !path.exists() && !self.has_entry(&short_path) {
                paths.push(short_path);
            }
        }
        Ok(paths)
    }

    pub fn list_modified_files(&self, entry_reader: &CommitEntryReader) -> Result<Vec<PathBuf>, OxenError> {
        // TODO: We are looping multiple times to check whether file is added,modified,or removed, etc
        //       We should do this loop once, and check each thing
        let dir_entries = util::fs::rlist_files_in_dir(&self.repository.path);

        let mut paths: Vec<PathBuf> = vec![];
        for local_path in dir_entries.iter() {
            if local_path.is_file() {
                // Return relative path with respect to the repo
                let relative_path =
                    util::fs::path_relative_to_dir(&local_path, &self.repository.path)?;

                // log::debug!("stager::list_modified_files considering path {:?}", relative_path);
                
                if self.has_entry(&relative_path) {
                    // log::debug!("stager::list_modified_files already added path {:?}", relative_path);
                    continue;
                }

                // Check if we have the entry in the head commit
                if let Ok(Some(old_entry)) = entry_reader.get_entry(&relative_path) {
                    // Get last modified time
                    let metadata = fs::metadata(local_path).unwrap();
                    let mtime = FileTime::from_last_modification_time(&metadata);

                    // log::debug!("comparing timestamps: {} to {}", old_entry.last_modified_nanoseconds, mtime.nanoseconds());

                    if old_entry.has_different_modification_time(&mtime) {
                        // log::debug!("stager::list_modified_files modification times are different! {:?}", relative_path);
                        paths.push(relative_path);
                    }
                } else {
                    // log::debug!("stager::list_modified_files we don't have file in head commit {:?}", relative_path);
                }
            }
        }

        Ok(paths)
    }

    pub fn list_untracked_files(&self, entry_reader: &CommitEntryReader) -> Result<Vec<PathBuf>, OxenError> {
        let dir_entries = std::fs::read_dir(&self.repository.path)?;
        // println!("Listing untracked files from {:?}", dir_entries);
        let num_in_head = entry_reader.num_entries()?;
        log::debug!(
            "stager::list_untracked_files head has {} files",
            num_in_head
        );

        let mut paths: Vec<PathBuf> = vec![];
        for entry in dir_entries {
            let local_path = entry?.path();
            if local_path.is_file() {
                // Return relative path with respect to the repo
                let relative_path =
                    util::fs::path_relative_to_dir(&local_path, &self.repository.path)?;
                log::debug!(
                    "stager::list_untracked_files considering path {:?}",
                    relative_path
                );

                // File is committed in HEAD
                if entry_reader.has_file(&relative_path) {
                    continue;
                }

                // File is staged
                if !self.has_entry(&relative_path) {
                    paths.push(relative_path);
                }
            }
        }

        Ok(paths)
    }

    fn file_is_in_index(&self, path: &Path, entry_reader: &CommitEntryReader) -> bool {
        if self.has_entry(path) {
            // we have it in our staged db
            true
        } else {
            // it is committed
            entry_reader.has_file(path)
        }
    }

    pub fn list_untracked_directories(
        &self,
        entry_writer: &CommitEntryReader,
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

                if entry_writer.has_file(&relative_path) {
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
                            let count = self.count_untracked_files_in_dir(&path, entry_writer);
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
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::index::{CommitReader, CommitWriter, CommitEntryReader, Stager};
    use crate::model::{StagedEntryStatus, StagedDirStats};
    use crate::test;
    use crate::util;

    use std::path::{Path, PathBuf};

    #[test]
    fn test_1_stager_add_file() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits

            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;

            // Write a file to disk
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            // Add the file
            let path = stager.add_file(&hello_file, &entry_reader)?;

            // Make sure we saved the relative path
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;
            assert_eq!(path, relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_unstage() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            // Add a file and a directory
            stager.add_file(&hello_file, &entry_reader)?;
            stager.add_dir(&sub_dir, &entry_reader)?;

            // Make sure the counts start properly
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 3);
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
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Make sure we have a valid file
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            // Add it twice
            stager.add_file(&hello_file, &entry_reader)?;
            stager.add_file(&hello_file, &entry_reader)?;

            // Make sure we still only have it once
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_cannot_add_if_not_different_from_commit() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Make sure we have a valid file
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            // Add it
            stager.add_file(&hello_file, &entry_reader)?;

            // Commit it
            let commit_writer = CommitWriter::new(&repo)?;
            let status = stager.status(&entry_reader)?;
            let commit = commit_writer.commit(&status, "Add Hello World")?;
            stager.unstage()?;

            // try to add it again
            let entry_reader = CommitEntryReader::new(&repo, &commit)?;
            stager.add_file(&hello_file, &entry_reader)?;

            // make sure we don't have it added again, because the hash hadn't changed since last commit
            let status = stager.status(&entry_reader)?;
            assert_eq!(status.added_files.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_add_non_existant_file() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let hello_file = PathBuf::from("non-existant.txt");
            if stager.add_file(&hello_file, &entry_reader).is_ok() {
                // we don't want to be able to add this file
                panic!("test_add_non_existant_file() Cannot stage non-existant file")
            }

            Ok(())
        })
    }

    #[test]
    fn test_add_directory() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to directories
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            stager.add_dir(&sub_dir, &entry_reader)?;

            let num_files = stager.list_added_files()?.len();
            assert_eq!(2, num_files);

            Ok(())
        })
    }

    #[test]
    fn test_stager_get_entry() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;

            // Stage file
            stager.add_file(&hello_file, &entry_reader)?;

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
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;

            // Stage file
            stager.add_file(&hello_file, &entry_reader)?;

            // List files
            let files = stager.list_added_files()?;
            assert_eq!(files.len(), 1);

            assert_eq!(files[0].0, relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_file_in_sub_dir() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let sub_file = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            stager.add_file(&sub_file, &entry_reader)?;

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
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;
            let sub_file = test::add_txt_file_to_dir(&sub_dir, "Hello 3")?;

            let dirs = stager.list_untracked_directories(&entry_reader)?;
            // There is one directory
            assert_eq!(dirs.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(&sub_dir, repo_path)?;
            assert_eq!(dirs[0].0, relative_path);

            // With three untracked files
            assert_eq!(dirs[0].1, 3);

            // Then we add one file
            stager.add_file(&sub_file, &entry_reader)?;

            // There are still two untracked files in the dir
            let dirs = stager.list_untracked_directories(&entry_reader)?;
            assert_eq!(dirs.len(), 1);
            assert_eq!(dirs[0].0, relative_path);

            // With two files
            assert_eq!(dirs[0].1, 2);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_all_files_in_sub_dir() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let training_data_dir = PathBuf::from("training_data");
            let sub_dir = repo_path.join(&training_data_dir);
            std::fs::create_dir_all(&sub_dir)?;

            let sub_file_1 = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let sub_file_2 = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;
            let sub_file_3 = test::add_txt_file_to_dir(&sub_dir, "Hello 3")?;

            let dirs = stager.list_untracked_directories(&entry_reader)?;

            // There is one directory
            assert_eq!(dirs.len(), 1);
            // With three untracked files
            assert_eq!(dirs[0].1, 3);

            // Then we add all three
            stager.add_file(&sub_file_1, &entry_reader)?;
            stager.add_file(&sub_file_2, &entry_reader)?;
            stager.add_file(&sub_file_3, &entry_reader)?;

            // There now there are no untracked directories
            let untracked_dirs = stager.list_untracked_directories(&entry_reader)?;
            assert_eq!(untracked_dirs.len(), 0);

            // And there is one tracked directory
            let added_dirs = stager.list_added_directories()?;
            assert_eq!(added_dirs.len(), 1);
            let added_dir = added_dirs.get(&StagedDirStats::from_path(training_data_dir)).unwrap();
            assert_eq!(added_dir.num_files_staged, 3);
            assert_eq!(added_dir.total_files, 3);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_directories() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let training_data_dir = PathBuf::from("training_data");
            let sub_dir = repo_path.join(&training_data_dir);
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            stager.add_dir(&sub_dir, &entry_reader)?;

            // List files
            let dirs = stager.list_added_directories()?;

            // There is one directory
            assert_eq!(dirs.len(), 1);
            let added_dir = dirs.get(&StagedDirStats::from_path(&training_data_dir)).unwrap();
            assert_eq!(added_dir.path, training_data_dir);

            // With two files
            assert_eq!(added_dir.num_files_staged, 2);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello 1")?;

            // Do not add...

            // List files
            let files = stager.list_untracked_files(&entry_reader)?;
            assert_eq!(files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;
            assert_eq!(files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_modified_files() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello 1")?;

            // add the file
            stager.add_file(&hello_file, &entry_reader)?;

            // commit the file
            let status = stager.status(&entry_reader)?;
            let commit_writer = CommitWriter::new(&repo)?;
            let commit = commit_writer.commit(&status, "added hello 1")?;
            stager.unstage()?;

            let mod_files = stager.list_modified_files(&entry_reader)?;
            assert_eq!(mod_files.len(), 0);

            // modify the file
            let hello_file = test::modify_txt_file(hello_file, "Hello 2")?;

            // List files
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;
            let mod_files = stager.list_modified_files(&entry_reader)?;
            assert_eq!(mod_files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;
            assert_eq!(mod_files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_dirs() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            // Must have some sort of file in the dir to add it.
            test::write_txt_file_to_path(sub_dir.join("hi.txt"), "Hi")?;

            // Do not add...

            // List files
            let files = stager.list_untracked_directories(&entry_reader)?;
            assert_eq!(files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_one_untracked_directory() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            // Do not add...

            // List files
            let files = stager.list_untracked_directories(&entry_reader)?;

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
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&repo, &commit)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let annotations_dir = PathBuf::from("annotations");
            let full_annotations_dir = repo_path.join(&annotations_dir);

            // Add the directory which has the structure
            // annotations/
            //   train/
            //     annotations.txt
            //     one_shot.txt
            //   test/
            //     annotations.txt
            stager.add(&full_annotations_dir, &entry_reader)?;

            // List dirs
            let dirs = stager.list_added_directories()?;

            // There is one directory
            assert_eq!(dirs.len(), 1);

            // With 3 recursive files
            let added_dir = dirs.get(&StagedDirStats::from_path(annotations_dir)).unwrap();
            assert_eq!(added_dir.num_files_staged, 3);

            Ok(())
        })
    }

    #[test]
    fn test_stager_modify_file_recursive() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let stager = Stager::new(&repo)?;
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&repo, &commit)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let one_shot_file = repo_path.join("annotations").join("train").join("one_shot.txt");

            // Add the directory which has the structure
            // annotations/
            //   train/
            //     one_shot.txt

            // Modify the committed file
            let one_shot_file = test::modify_txt_file(one_shot_file, "new content coming in hot")?;

            // List dirs
            let files = stager.list_modified_files(&entry_reader)?;

            // There is one modified file
            assert_eq!(files.len(), 1);

            // And it is
            let relative_path = util::fs::path_relative_to_dir(&one_shot_file, repo_path)?;
            assert_eq!(files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_directories_after_add() -> Result<(), OxenError> {
        test::run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;

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
            let untracked_dirs = stager.list_untracked_directories(&entry_reader)?;
            assert_eq!(untracked_dirs.len(), 3);

            // Add the directory
            let _ = stager.add_dir(&train_dir, &entry_reader)?;
            // Add one file
            let _ = stager.add_file(&base_file_1, &entry_reader)?;

            // List the files
            let added_files = stager.list_added_files()?;
            let added_dirs = stager.list_added_directories()?;
            let untracked_files = stager.list_untracked_files(&entry_reader)?;
            let untracked_dirs = stager.list_untracked_directories(&entry_reader)?;

            // There is 5 added file and 1 added dir
            assert_eq!(added_files.len(), 5);
            assert_eq!(added_dirs.len(), 1);

            // There are 2 untracked files at the top level
            assert_eq!(untracked_files.len(), 2);
            // There are 2 untracked dirs at the top level
            assert_eq!(untracked_dirs.len(), 2);

            Ok(())
        })
    }
}
