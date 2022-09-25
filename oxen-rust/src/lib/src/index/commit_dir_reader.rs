use crate::constants::HISTORY_DIR;
use crate::db;
use crate::error::OxenError;
use crate::index::{CommitDirEntryReader, CommitReader};
use crate::model::{Commit, CommitEntry, DirEntry};
use crate::util;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;

use crate::model::LocalRepository;

pub struct CommitDirReader {
    dir_db: DBWithThreadMode<MultiThreaded>,
    repository: LocalRepository,
    pub commit_id: String,
}

impl CommitDirReader {
    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitDirReader, OxenError> {
        log::debug!("CommitDirReader::new() commit_id: {}", commit.id);
        let db_path = util::fs::oxen_hidden_dir(&repository.path)
            .join(HISTORY_DIR)
            .join(&commit.id);
        let opts = db::opts::default();
        Ok(CommitDirReader {
            dir_db: DBWithThreadMode::open_for_read_only(&opts, &db_path, true)?,
            repository: repository.to_owned(),
            commit_id: commit.id.to_owned(),
        })
    }

    /// For opening the entry reader from head, so that it opens and closes the commit db within the constructor
    pub fn new_from_head(repository: &LocalRepository) -> Result<CommitDirReader, OxenError> {
        let commit_reader = CommitReader::new(repository)?;
        let commit = commit_reader.head_commit()?;
        log::debug!(
            "CommitDirReader::new_from_head() commit_id: {}",
            commit.id
        );
        CommitDirReader::new(repository, &commit)
    }

    pub fn list_committed_dirs(&self) -> Result<Vec<PathBuf>, OxenError> {
        let iter = self.dir_db.iterator(IteratorMode::Start);
        let mut paths: Vec<PathBuf> = vec![];
        for (key, _value) in iter {
            match str::from_utf8(&*key) {
                Ok(key) => {
                    paths.push(PathBuf::from(String::from(key)));
                }
                _ => {
                    log::error!("list_committed_dirs() Could not decode key {:?}", key)
                }
            }
        }
        Ok(paths)
    }

    pub fn num_entries(&self) -> Result<usize, OxenError> {
        let mut count = 0;
        for dir in self.list_committed_dirs()? {
            let commit_entry_dir = CommitDirEntryReader::new(&self.repository, &dir)?;
            count += commit_entry_dir.num_entries();
        }
        Ok(count)
    }

    pub fn get_path_hash<T: AsRef<Path>>(&self, path: T) -> Result<String, OxenError> {
        let path = path.as_ref();
        if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
            let dir = CommitDirEntryReader::new(&self.repository, &parent)?;
            dir.get_path_hash(file_name)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut paths: Vec<PathBuf> = vec![];
        for dir in self.list_committed_dirs()? {
            let commit_dir = CommitDirEntryReader::new(&self.repository, &dir)?;
            let mut files = commit_dir.list_files()?;
            paths.append(&mut files);
        }
        Ok(paths)
    }

    /// List entries in a vector when we need ordering
    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        let mut paths: Vec<CommitEntry> = vec![];
        for dir in self.list_committed_dirs()? {
            let commit_dir = CommitDirEntryReader::new(&self.repository, &dir)?;
            let mut files = commit_dir.list_entries()?;
            paths.append(&mut files);
        }
        Ok(paths)
    }

    /// List entries in a set for quick lookup
    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        let mut paths: HashSet<CommitEntry> = HashSet::new();
        for dir in self.list_committed_dirs()? {
            let commit_dir = CommitDirEntryReader::new(&self.repository, &dir)?;
            let files = commit_dir.list_entries_set()?;
            paths.extend(files);
        }
        Ok(paths)
    }

    // pub fn list_entry_page(
    //     &self,
    //     page_num: usize,
    //     page_size: usize,
    // ) -> Result<Vec<CommitEntry>, OxenError> {
    //     // The iterator doesn't technically have a skip method as far as I can tell
    //     // so we are just going to manually do it
    //     let mut paths: Vec<CommitEntry> = vec![];
    //     let iter = self.db.iterator(IteratorMode::Start);
    //     // Do not go negative, and start from 0
    //     let start_page = if page_num == 0 { 0 } else { page_num - 1 };
    //     let start_idx = start_page * page_size;
    //     for (entry_i, (_key, value)) in iter.enumerate() {
    //         // limit to page_size
    //         if paths.len() >= page_size {
    //             break;
    //         }

    //         // only grab values after start_idx based on page_num and page_size
    //         if entry_i >= start_idx {
    //             let entry: CommitEntry = serde_json::from_str(str::from_utf8(&*value)?)?;
    //             paths.push(entry);
    //         }
    //     }
    //     Ok(paths)
    // }

    pub fn list_directory(
        &self,
        search_dir: &Path,
        page_num: usize,
        page_size: usize,
    ) -> Result<(Vec<DirEntry>, usize), OxenError> {
        let root_dir = Path::new("./");
        let mut search_dir = search_dir.to_path_buf();
        if !search_dir.starts_with(&root_dir) {
            search_dir = root_dir.join(&search_dir);
        }
        let search_components_count = search_dir.components().count();

        let mut base_dirs: HashSet<PathBuf> = HashSet::new();

        let mut dir_paths: Vec<DirEntry> = vec![];

        let mut file_paths: Vec<DirEntry> = vec![];
        // Do not go negative, and start from 0
        let start_page = if page_num == 0 { 0 } else { page_num - 1 };
        let start_idx = start_page * page_size;

        panic!("TODO implement");
        // let iter = self.db.iterator(IteratorMode::Start);
        // for (key, _value) in iter {
        //     let path_str = format!("{}{}", root_dir.to_str().unwrap(), str::from_utf8(&*key)?);
        //     let path = Path::new(&path_str);
        //     // log::debug!("Considering {:?} starts with {:?}", path, search_dir);
        //     // Find all the base dirs within this directory
        //     if path.starts_with(&search_dir) {
        //         let path_components_count = path.components().count();
        //         let subpath = util::fs::path_relative_to_dir(path, &search_dir)?;
        //         let mut components = subpath.components().collect::<VecDeque<_>>();

        //         // Get uniq top level dirs
        //         if let Some(base_dir) = components.pop_front() {
        //             let base_path: &Path = base_dir.as_ref();
        //             if base_path.extension().is_none() && base_dirs.insert(base_path.to_path_buf())
        //             {
        //                 dir_paths.push(DirEntry {
        //                     filename: String::from(base_path.to_str().unwrap()),
        //                     is_dir: true,
        //                 })
        //             }
        //         }

        //         // Get all files that are in this dir level
        //         if (path_components_count - 1) == search_components_count {
        //             // TODO: add in author and last modified given the CommitEntry commit_id
        //             file_paths.push(DirEntry {
        //                 filename: String::from(subpath.to_str().unwrap()),
        //                 is_dir: false,
        //             })
        //         }
        //     }
        // }

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
        if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
            if let Ok(dir) = CommitDirEntryReader::new(&self.repository, &parent) {
                return dir.has_file(file_name);
            }
        }
        false
    }

    pub fn get_entry(&self, path: &Path) -> Result<Option<CommitEntry>, OxenError> {
        if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
            let dir = CommitDirEntryReader::new(&self.repository, &parent)?;
            dir.get_entry(file_name)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    pub fn contains_path(&self, path: &Path) -> Result<bool, OxenError> {
        if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
            let dir = CommitDirEntryReader::new(&self.repository, &parent)?;
            dir.contains_path(file_name)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::CommitDirReader;

    use crate::test;

    use std::path::Path;

    #[test]
    fn test_check_if_file_exists() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            command::add(&repo, &filepath)?;
            let commit = command::commit(&repo, "Adding labels file")?.unwrap();

            let reader = CommitDirReader::new(&repo, &commit)?;
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

            let reader = CommitDirReader::new(&repo, commit)?;
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
    fn test_commit_entry_reader_list_train_directory_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();

            let reader = CommitDirReader::new(&repo, commit)?;
            let (dir_entries, size) = reader.list_directory(Path::new("train"), 1, 10)?;

            assert_eq!(size, 5);
            assert_eq!(dir_entries.len(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_commit_entry_reader_list_train_sub_directory_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();

            let reader = CommitDirReader::new(&repo, commit)?;
            let (dir_entries, size) =
                reader.list_directory(Path::new("annotations/train"), 1, 10)?;

            assert_eq!(size, 2);
            assert_eq!(dir_entries.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_commit_entry_reader_list_train_directory_subset() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();

            let reader = CommitDirReader::new(&repo, commit)?;
            let (dir_entries, size) = reader.list_directory(Path::new("train"), 2, 3)?;

            assert_eq!(size, 5);
            assert_eq!(dir_entries.len(), 2);

            Ok(())
        })
    }
}
