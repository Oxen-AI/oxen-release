use crate::constants::{DIRS_DIR, HISTORY_DIR};
use crate::db;
use crate::error::OxenError;
use crate::index::{CommitDirEntryReader, CommitReader};
use crate::model::{Commit, CommitEntry, DirEntry};
use crate::util;
use crate::view::entry::ResourceVersion;
use crate::view::PaginatedDirEntries;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::db::path_db;
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
        CommitDirReader::new_from_commit_id(repository, &commit.id)
    }

    pub fn new_from_commit_id(
        repository: &LocalRepository,
        commit_id: &str,
    ) -> Result<CommitDirReader, OxenError> {
        log::debug!(
            "CommitDirReader::new_from_commit_id() commit_id: {}",
            commit_id
        );
        let db_path = util::fs::oxen_hidden_dir(&repository.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(DIRS_DIR);
        let opts = db::opts::default();

        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
        }

        Ok(CommitDirReader {
            dir_db: DBWithThreadMode::open_for_read_only(&opts, &db_path, true)?,
            repository: repository.to_owned(),
            commit_id: commit_id.to_owned(),
        })
    }

    /// For opening the entry reader from head, so that it opens and closes the commit db within the constructor
    pub fn new_from_head(repository: &LocalRepository) -> Result<CommitDirReader, OxenError> {
        let commit_reader = CommitReader::new(repository)?;
        let commit = commit_reader.head_commit()?;
        log::debug!("CommitDirReader::new_from_head() commit_id: {}", commit.id);
        CommitDirReader::new(repository, &commit)
    }

    pub fn list_committed_dirs(&self) -> Result<Vec<PathBuf>, OxenError> {
        path_db::list_paths(&self.dir_db, Path::new(""))
    }

    pub fn has_dir<P: AsRef<Path>>(&self, path: P) -> bool {
        path_db::has_entry(&self.dir_db, path)
    }

    pub fn num_entries(&self) -> Result<usize, OxenError> {
        let mut count = 0;
        for dir in self.list_committed_dirs()? {
            let commit_entry_dir =
                CommitDirEntryReader::new(&self.repository, &self.commit_id, &dir)?;
            count += commit_entry_dir.num_entries();
        }
        Ok(count)
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut paths: Vec<PathBuf> = vec![];
        for dir in self.list_committed_dirs()? {
            let commit_dir = CommitDirEntryReader::new(&self.repository, &self.commit_id, &dir)?;
            let mut files = commit_dir.list_files()?;
            paths.append(&mut files);
        }
        Ok(paths)
    }

    /// List entries in a vector when we need ordering
    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        let mut paths: Vec<CommitEntry> = vec![];
        for dir in self.list_committed_dirs()? {
            let commit_dir = CommitDirEntryReader::new(&self.repository, &self.commit_id, &dir)?;
            let mut files = commit_dir.list_entries()?;
            paths.append(&mut files);
        }
        Ok(paths)
    }

    /// List entries in a set for quick lookup
    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        let mut paths: HashSet<CommitEntry> = HashSet::new();
        for dir in self.list_committed_dirs()? {
            let commit_dir = CommitDirEntryReader::new(&self.repository, &self.commit_id, &dir)?;
            let files = commit_dir.list_entries_set()?;
            paths.extend(files);
        }
        Ok(paths)
    }

    pub fn list_entry_page(
        &self,
        page: usize,
        page_size: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        let entries = self.list_entries()?;

        let start_page = if page == 0 { 0 } else { page - 1 };
        let start_idx = start_page * page_size;

        if (start_idx + page_size) < entries.len() {
            let subset: Vec<CommitEntry> = entries[start_idx..(start_idx + page_size)].to_vec();
            Ok(subset)
        } else if (start_idx < entries.len()) && (start_idx + page_size) >= entries.len() {
            let subset: Vec<CommitEntry> = entries[start_idx..entries.len()].to_vec();
            Ok(subset)
        } else {
            Ok(vec![])
        }
    }

    pub fn list_directory(
        &self,
        directory: &Path,
        branch_or_commit_id: &str,
        page: usize,
        page_size: usize,
    ) -> Result<PaginatedDirEntries, OxenError> {
        let commit_reader = CommitReader::new(&self.repository)?;

        let mut dir_paths: Vec<DirEntry> = vec![];
        for dir in self.list_committed_dirs()? {
            // log::debug!("LIST DIRECTORY considering committed dir: {:?} for search {:?}", dir, search_dir);
            if let Some(parent) = dir.parent() {
                if parent == directory || (parent == Path::new("") && directory == Path::new("./"))
                {
                    dir_paths.push(self.dir_entry_from_dir(
                        &dir,
                        &commit_reader,
                        branch_or_commit_id,
                    )?);
                }
            }
        }
        log::debug!("list_directory got dir_paths {}", dir_paths.len());

        let mut file_paths: Vec<DirEntry> = vec![];
        let commit_dir_reader =
            CommitDirEntryReader::new(&self.repository, &self.commit_id, directory)?;
        let total = commit_dir_reader.num_entries() + dir_paths.len();
        for file in commit_dir_reader.list_entries()? {
            file_paths.push(self.dir_entry_from_commit_entry(
                &file,
                &commit_reader,
                branch_or_commit_id,
            )?)
        }
        log::debug!("list_directory got file_paths {}", dir_paths.len());

        // Combine all paths, starting with dirs
        dir_paths.append(&mut file_paths);

        log::debug!(
            "list_directory {:?} page {} page_size {} total {}",
            directory,
            page,
            page_size,
            total,
        );

        let resource = Some(ResourceVersion {
            path: directory.to_str().unwrap().to_string(),
            version: branch_or_commit_id.to_string(),
        });
        Ok(PaginatedDirEntries::from_entries(
            dir_paths, resource, page, page_size, total,
        ))
    }

    fn dir_entry_from_dir(
        &self,
        path: &Path,
        commit_reader: &CommitReader,
        branch_or_commit_id: &str,
    ) -> Result<DirEntry, OxenError> {
        let commit = commit_reader.get_commit_by_id(&self.commit_id)?.unwrap();
        let commit_dir_reader = CommitDirReader::new(&self.repository, &commit)?;

        // Find latest commit within dir and compute recursive size
        let commits: HashMap<String, Commit> = HashMap::new();
        let mut latest_commit = Some(commit);
        let mut total_size: u64 = 0;
        // This lists all the committed dirs
        let dirs = commit_dir_reader.list_committed_dirs()?;
        for dir in dirs {
            // Have to make sure we are in a subset of the dir (not really a tree structure)
            if dir.starts_with(path) {
                let commit_dir_reader =
                    CommitDirEntryReader::new(&self.repository, &self.commit_id, &dir)?;
                for entry in commit_dir_reader.list_entries()? {
                    total_size += entry.num_bytes;

                    let commit = if commits.contains_key(&entry.commit_id) {
                        Some(commits[&entry.commit_id].clone())
                    } else {
                        commit_reader.get_commit_by_id(&entry.commit_id)?
                    };

                    if latest_commit.is_none() {
                        latest_commit = commit.clone();
                    }

                    if latest_commit.as_ref().unwrap().timestamp
                        > commit.as_ref().unwrap().timestamp
                    {
                        latest_commit = commit.clone();
                    }
                }
            }
        }

        return Ok(DirEntry {
            filename: String::from(path.file_name().unwrap().to_str().unwrap()),
            is_dir: true,
            size: total_size,
            latest_commit,
            datatype: String::from("dir"),
            resource: Some(ResourceVersion {
                version: branch_or_commit_id.to_string(),
                path: path.to_str().unwrap().to_string(),
            }),
        });
    }

    fn dir_entry_from_commit_entry(
        &self,
        entry: &CommitEntry,
        commit_reader: &CommitReader,
        branch_or_commit_id: &str,
    ) -> Result<DirEntry, OxenError> {
        let size = util::fs::version_file_size(&self.repository, entry)?;
        let latest_commit = commit_reader.get_commit_by_id(&entry.commit_id)?.unwrap();

        let version_path = util::fs::version_path(&self.repository, entry);
        return Ok(DirEntry {
            filename: String::from(entry.path.file_name().unwrap().to_str().unwrap()),
            is_dir: false,
            size,
            latest_commit: Some(latest_commit),
            datatype: util::fs::file_datatype(&version_path),
            resource: Some(ResourceVersion {
                version: branch_or_commit_id.to_string(),
                path: entry.path.to_str().unwrap().to_string(),
            }),
        });
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
            if let Ok(dir) = CommitDirEntryReader::new(&self.repository, &self.commit_id, parent) {
                return dir.has_file(file_name);
            }
        }
        false
    }

    pub fn get_entry(&self, path: &Path) -> Result<Option<CommitEntry>, OxenError> {
        if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
            let dir = CommitDirEntryReader::new(&self.repository, &self.commit_id, parent)?;
            dir.get_entry(file_name)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::index::CommitDirReader;
    use crate::{command, util};

    use crate::test;

    use std::path::Path;

    #[test]
    fn test_commit_dir_reader_check_if_file_exists() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            command::add(&repo, filepath)?;
            let commit = command::commit(&repo, "Adding labels file")?.unwrap();

            let reader = CommitDirReader::new(&repo, &commit)?;
            let path = Path::new(filename);
            assert!(reader.has_file(path));

            Ok(())
        })
    }

    #[test]
    fn test_commit_dir_reader_list_top_level_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();

            let reader = CommitDirReader::new(&repo, commit)?;
            let paginated = reader.list_directory(Path::new("./"), &commit.id, 1, 10)?;
            let dir_entries = paginated.entries;
            let size = paginated.total_entries;
            for entry in dir_entries.iter() {
                println!("{entry:?}");
            }

            assert_eq!(size, 7);
            assert_eq!(dir_entries.len(), 7);
            assert_eq!(
                dir_entries
                    .clone()
                    .into_iter()
                    .filter(|e| !e.is_dir)
                    .count(),
                2
            );
            assert_eq!(dir_entries.into_iter().filter(|e| e.is_dir).count(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_commit_dir_reader_list_train_directory_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();

            let reader = CommitDirReader::new(&repo, commit)?;
            let paginated = reader.list_directory(Path::new("train"), &commit.id, 1, 10)?;
            let dir_entries = paginated.entries;
            let size = paginated.total_entries;

            assert_eq!(size, 5);
            assert_eq!(dir_entries.len(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_commit_dir_reader_list_train_sub_directory_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();

            let reader = CommitDirReader::new(&repo, commit)?;
            let paginated =
                reader.list_directory(Path::new("annotations/train"), &commit.id, 1, 10)?;
            let dir_entries = paginated.entries;
            let size = paginated.total_entries;

            assert_eq!(size, 4);
            assert_eq!(dir_entries.len(), 4);

            Ok(())
        })
    }

    #[test]
    fn test_commit_dir_reader_list_train_directory_subset() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let commit = commits.first().unwrap();

            let reader = CommitDirReader::new(&repo, commit)?;
            let paginated = reader.list_directory(Path::new("train"), &commit.id, 2, 3)?;
            let dir_entries = paginated.entries;
            let total_entries = paginated.total_entries;

            for entry in dir_entries.iter() {
                println!("{entry:?}");
            }

            assert_eq!(total_entries, 5);
            assert_eq!(dir_entries.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_commit_dir_reader_list_train_directory_exactly_ten() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 8 directories
            for n in 0..8 {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?.unwrap();

            let page_number = 1;
            let page_size = 10;

            let reader = CommitDirReader::new(&repo, &commit)?;
            let paginated =
                reader.list_directory(Path::new("."), &commit.id, page_number, page_size)?;
            assert_eq!(paginated.total_entries, 10);
            assert_eq!(paginated.total_pages, 1);
            assert_eq!(paginated.entries.len(), 10);

            Ok(())
        })
    }

    #[test]
    fn test_commit_dir_reader_list_train_directory_exactly_ten_page_two() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 8 directories
            for n in 0..8 {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?.unwrap();

            let page_number = 2;
            let page_size = 10;

            let reader = CommitDirReader::new(&repo, &commit)?;
            let paginated =
                reader.list_directory(Path::new("."), &commit.id, page_number, page_size)?;
            assert_eq!(paginated.total_entries, 10);
            assert_eq!(paginated.total_pages, 1);
            assert_eq!(paginated.entries.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_commit_dir_reader_list_train_directory_nine_entries_page_size_ten(
    ) -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 7 directories
            for n in 0..7 {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?.unwrap();

            let page_number = 1;
            let page_size = 10;

            let reader = CommitDirReader::new(&repo, &commit)?;
            let paginated =
                reader.list_directory(Path::new("."), &commit.id, page_number, page_size)?;
            assert_eq!(paginated.total_entries, 9);
            assert_eq!(paginated.total_pages, 1);

            Ok(())
        })
    }

    #[test]
    fn test_commit_dir_reader_list_train_directory_eleven_entries_page_size_ten(
    ) -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 9 directories
            for n in 0..9 {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?.unwrap();

            let page_number = 1;
            let page_size = 10;

            let reader = CommitDirReader::new(&repo, &commit)?;
            let paginated =
                reader.list_directory(Path::new("."), &commit.id, page_number, page_size)?;
            assert_eq!(paginated.total_entries, 11);
            assert_eq!(paginated.total_pages, 2);

            Ok(())
        })
    }
}
