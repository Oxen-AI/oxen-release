use crate::constants::HISTORY_DIR;
use crate::db;
use crate::error::OxenError;
use crate::index::{CommitEntryDBReader, CommitReader};
use crate::model::{Commit, CommitEntry, DirEntry};
use crate::util;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::str;

use crate::model::LocalRepository;

pub struct CommitEntryReader {
    db: DBWithThreadMode<MultiThreaded>,
}

impl CommitEntryReader {
    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitEntryReader, OxenError> {
        log::debug!("CommitEntryReader::new() commit_id: {}", commit.id);
        let db_path = util::fs::oxen_hidden_dir(&repository.path)
            .join(HISTORY_DIR)
            .join(&commit.id);
        let opts = db::opts::default();
        Ok(CommitEntryReader {
            db: DBWithThreadMode::open_for_read_only(&opts, &db_path, true)?,
        })
    }

    /// For opening the entry reader from head, so that it opens and closes the commit db within the constructor
    pub fn new_from_head(repository: &LocalRepository) -> Result<CommitEntryReader, OxenError> {
        let commit_reader = CommitReader::new(repository)?;
        let commit = commit_reader.head_commit()?;
        log::debug!(
            "CommitEntryReader::new_from_head() commit_id: {}",
            commit.id
        );
        CommitEntryReader::new(repository, &commit)
    }

    pub fn num_entries(&self) -> Result<usize, OxenError> {
        Ok(self.db.iterator(IteratorMode::Start).count())
    }

    pub fn get_path_hash(&self, path: &Path) -> Result<String, OxenError> {
        let key = path.to_str().unwrap();
        let bytes = key.as_bytes();
        match self.db.get(bytes) {
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
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut paths: Vec<PathBuf> = vec![];
        let iter = self.db.iterator(IteratorMode::Start);
        for (key, _value) in iter {
            paths.push(PathBuf::from(str::from_utf8(&*key)?));
        }
        Ok(paths)
    }

    /// List entries in a vector when we need ordering
    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        let mut paths: Vec<CommitEntry> = vec![];
        let iter = self.db.iterator(IteratorMode::Start);
        for (_key, value) in iter {
            let entry: CommitEntry = serde_json::from_str(str::from_utf8(&*value)?)?;
            paths.push(entry);
        }
        Ok(paths)
    }

    /// List entries in a set for quick lookup
    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        let mut paths: HashSet<CommitEntry> = HashSet::new();
        let iter = self.db.iterator(IteratorMode::Start);
        for (_key, value) in iter {
            let entry: CommitEntry = serde_json::from_str(str::from_utf8(&*value)?)?;
            paths.insert(entry);
        }
        Ok(paths)
    }

    pub fn list_entry_page(
        &self,
        page_num: usize,
        page_size: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        // The iterator doesn't technically have a skip method as far as I can tell
        // so we are just going to manually do it
        let mut paths: Vec<CommitEntry> = vec![];
        let iter = self.db.iterator(IteratorMode::Start);
        // Do not go negative, and start from 0
        let start_page = if page_num == 0 { 0 } else { page_num - 1 };
        let start_idx = start_page * page_size;
        for (entry_i, (_key, value)) in iter.enumerate() {
            // limit to page_size
            if paths.len() >= page_size {
                break;
            }

            // only grab values after start_idx based on page_num and page_size
            if entry_i >= start_idx {
                let entry: CommitEntry = serde_json::from_str(str::from_utf8(&*value)?)?;
                paths.push(entry);
            }
        }
        Ok(paths)
    }

    pub fn list_directory(
        &self,
        search_dir: &Path,
        page_num: usize,
        page_size: usize,
    ) -> Result<(Vec<DirEntry>, usize), OxenError> {
        let root_dir = Path::new("./");
        let dir_components_count = search_dir.components().count();

        let mut base_dirs: HashSet<PathBuf> = HashSet::new();

        let mut dir_paths: Vec<DirEntry> = vec![];
        let mut file_paths: Vec<DirEntry> = vec![];
        let iter = self.db.iterator(IteratorMode::Start);
        // Do not go negative, and start from 0
        let start_page = if page_num == 0 { 0 } else { page_num - 1 };
        let start_idx = start_page * page_size;
        for (key, _value) in iter {
            let path_str = str::from_utf8(&*key)?;
            let path = Path::new(&path_str);
            // Find all the base dirs within this directory
            if path.starts_with(search_dir) {
                let subpath = util::fs::path_relative_to_dir(path, search_dir)?;
                let mut components = subpath.components().collect::<VecDeque<_>>();

                // Get uniq top level dirs
                if let Some(base_dir) = components.pop_front() {
                    let base_path: &Path = base_dir.as_ref();
                    if base_dirs.insert(base_path.to_path_buf()) {
                        dir_paths.push(DirEntry {
                            filename: String::from(base_path.to_str().unwrap()),
                            is_dir: true,
                        })
                    }
                }

                // Get all files that are in this dir level
                if !components.is_empty() && (components.len() - 1) == dir_components_count {
                    file_paths.push(DirEntry {
                        filename: String::from(subpath.to_str().unwrap()),
                        is_dir: false,
                    })
                }
            }

            // If searching for root
            if search_dir == root_dir {
                let mut components = path.components().collect::<VecDeque<_>>();
                if let Some(base_dir) = components.pop_front() {
                    let base_path: &Path = base_dir.as_ref();
                    if base_path.extension().is_none() && base_dirs.insert(base_path.to_path_buf())
                    {
                        dir_paths.push(DirEntry {
                            filename: String::from(base_path.to_str().unwrap()),
                            is_dir: true,
                        })
                    }
                }

                // zero since we popped
                if components.is_empty() {
                    file_paths.push(DirEntry {
                        filename: String::from(path.to_str().unwrap()),
                        is_dir: false,
                    })
                }
            }
        }

        // Combine all paths, starting with dirs
        dir_paths.append(&mut file_paths);

        let count = dir_paths.len();
        log::debug!(
            "list_directory page_num {} page_size {} start_index {} total {}",
            page_num,
            page_size,
            start_idx,
            count
        );
        if (start_idx + page_size) < dir_paths.len() {
            let subset: Vec<DirEntry> = dir_paths[start_idx..(start_idx + page_size)].to_vec();
            Ok((subset, count))
        } else if (start_idx < dir_paths.len()) && (start_idx + page_size) >= dir_paths.len() {
            let subset: Vec<DirEntry> = dir_paths[start_idx..dir_paths.len()].to_vec();
            Ok((subset, count))
        } else {
            Ok((vec![], count))
        }
    }

    pub fn has_prefix_in_dir(&self, prefix: &Path) -> bool {
        match self.list_entries() {
            Ok(entries) => entries
                .into_iter()
                .any(|entry| entry.path.starts_with(prefix)),
            _ => false,
        }
    }

    pub fn list_files_from_dir(&self, dir: &Path) -> Vec<CommitEntry> {
        match self.list_entries() {
            Ok(entries) => entries
                .into_iter()
                .filter(|entry| entry.path.starts_with(dir))
                .collect(),
            _ => {
                vec![]
            }
        }
    }

    pub fn has_file(&self, path: &Path) -> bool {
        CommitEntryDBReader::has_file(&self.db, path)
    }

    pub fn get_entry(&self, path: &Path) -> Result<Option<CommitEntry>, OxenError> {
        CommitEntryDBReader::get_entry(&self.db, path)
    }

    pub fn contains_path(&self, path: &Path) -> Result<bool, OxenError> {
        // Check if path is in this commit
        let key = path.to_str().unwrap();
        let bytes = key.as_bytes();
        match self.db.get(bytes) {
            Ok(Some(_value)) => Ok(true),
            Ok(None) => Ok(false),
            Err(err) => {
                let err = format!("contains_path Error reading db\nErr: {}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::CommitEntryReader;

    use crate::test;

    use std::path::Path;

    #[test]
    fn test_check_if_file_exists() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            command::add(&repo, &filepath)?;
            let commit = command::commit(&repo, "Adding labels file")?.unwrap();

            let reader = CommitEntryReader::new(&repo, &commit)?;
            let path = Path::new(filename);
            assert!(reader.contains_path(path)?);

            Ok(())
        })
    }

    #[test]
    fn test_commit_entry_reader_list_top_level_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();

            let reader = CommitEntryReader::new(&repo, commit)?;
            let (dir_entries, size) = reader.list_directory(Path::new("./"), 1, 10)?;
            for entry in dir_entries.iter() {
                println!("{:?}", entry);
            }

            assert_eq!(size, 5);
            assert_eq!(dir_entries.len(), 5);
            assert_eq!(
                dir_entries
                    .clone()
                    .into_iter()
                    .filter(|e| !e.is_dir)
                    .count(),
                2
            );
            assert_eq!(dir_entries.into_iter().filter(|e| e.is_dir).count(), 3);

            Ok(())
        })
    }

    #[test]
    fn test_commit_entry_reader_list_train_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();

            let reader = CommitEntryReader::new(&repo, commit)?;
            let (dir_entries, size) = reader.list_directory(Path::new("train"), 1, 10)?;

            assert_eq!(size, 5);
            assert_eq!(dir_entries.len(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_commit_entry_reader_list_train_directory_subset() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();

            let reader = CommitEntryReader::new(&repo, commit)?;
            let (dir_entries, size) = reader.list_directory(Path::new("train"), 2, 3)?;

            assert_eq!(size, 5);
            assert_eq!(dir_entries.len(), 2);

            Ok(())
        })
    }
}
