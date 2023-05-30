//! Entries are the files and directories that are stored in a commit.
//!

use crate::error::OxenError;
use crate::util;
use crate::view::entry::ResourceVersion;
use rayon::prelude::*;

use crate::core::index::{CommitDirEntryReader, CommitEntryReader, CommitReader};
use crate::model::{Commit, CommitEntry, DirEntry, LocalRepository};
use crate::view::PaginatedDirEntries;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Get the directory entry for a given path in a commit.
/// Could be a file or a directory.
pub fn get_dir_entry(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<DirEntry, OxenError> {
    let entry_reader = CommitEntryReader::new(repo, commit)?;
    let commit_reader = CommitReader::new(repo)?;
    // Check if the path is a dir
    if entry_reader.has_dir(path) {
        dir_entry_from_dir(repo, commit, path, &commit_reader, &commit.id)
    } else {
        let parent = path.parent().ok_or(OxenError::file_has_no_parent(path))?;
        let base_name = path.file_name().ok_or(OxenError::file_has_no_name(path))?;
        let dir_entry_reader = CommitDirEntryReader::new(repo, &commit.id, parent)?;
        let entry = dir_entry_reader
            .get_entry(base_name)?
            .ok_or(OxenError::entry_does_not_exist_in_commit(path, &commit.id))?;
        dir_entry_from_commit_entry(repo, &entry, &commit_reader, &commit.id)
    }
}

/// Get a DirEntry summing up the size of all files in a directory
/// and finding the latest commit within the directory
pub fn dir_entry_from_dir(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
    commit_reader: &CommitReader,
    revision: &str,
) -> Result<DirEntry, OxenError> {
    let commit = commit_reader.get_commit_by_id(&commit.id)?.unwrap();
    let entry_reader = CommitEntryReader::new(repo, &commit)?;

    // Find latest commit within dir and compute recursive size
    let commits: HashMap<String, Commit> = HashMap::new();
    let mut latest_commit = Some(commit.to_owned());
    let mut total_size: u64 = 0;
    // This lists all the committed dirs
    let dirs = entry_reader.list_dirs()?;
    for dir in dirs {
        // Have to make sure we are in a subset of the dir (not really a tree structure)
        if dir.starts_with(path) {
            let entry_reader = CommitDirEntryReader::new(repo, &commit.id, &dir)?;
            for entry in entry_reader.list_entries()? {
                total_size += entry.num_bytes;

                let commit = if commits.contains_key(&entry.commit_id) {
                    Some(commits[&entry.commit_id].clone())
                } else {
                    commit_reader.get_commit_by_id(&entry.commit_id)?
                };

                if latest_commit.is_none() {
                    latest_commit = commit.clone();
                }

                if latest_commit.as_ref().unwrap().timestamp > commit.as_ref().unwrap().timestamp {
                    latest_commit = commit.clone();
                }
            }
        }
    }

    let base_name = path.file_name().ok_or(OxenError::file_has_no_name(path))?;
    return Ok(DirEntry {
        filename: String::from(base_name.to_string_lossy()),
        is_dir: true,
        size: total_size,
        latest_commit,
        datatype: String::from("dir"),
        resource: Some(ResourceVersion {
            version: revision.to_string(),
            path: String::from(path.to_string_lossy()),
        }),
    });
}

pub fn dir_entry_from_commit_entry(
    repo: &LocalRepository,
    entry: &CommitEntry,
    commit_reader: &CommitReader,
    revision: &str,
) -> Result<DirEntry, OxenError> {
    let size = util::fs::version_file_size(repo, entry)?;
    let latest_commit = commit_reader.get_commit_by_id(&entry.commit_id)?.unwrap();

    let base_name = entry
        .path
        .file_name()
        .ok_or(OxenError::file_has_no_name(&entry.path))?;
    let version_path = util::fs::version_path(repo, entry);
    return Ok(DirEntry {
        filename: String::from(base_name.to_string_lossy()),
        is_dir: false,
        size,
        latest_commit: Some(latest_commit),
        datatype: util::fs::file_datatype(&version_path),
        resource: Some(ResourceVersion {
            version: revision.to_string(),
            path: String::from(entry.path.to_string_lossy()),
        }),
    });
}

/// Commit entries are always files, not directories. Will return None if the path is a directory.
pub fn get_commit_entry(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<Option<CommitEntry>, OxenError> {
    let reader = CommitEntryReader::new(repo, commit)?;
    reader.get_entry(path)
}

pub fn list_all(repo: &LocalRepository, commit: &Commit) -> Result<Vec<CommitEntry>, OxenError> {
    let reader = CommitEntryReader::new(repo, commit)?;
    reader.list_entries()
}

pub fn count_for_commit(repo: &LocalRepository, commit: &Commit) -> Result<usize, OxenError> {
    let reader = CommitEntryReader::new(repo, commit)?;
    reader.num_entries()
}

pub fn list_page(
    repo: &LocalRepository,
    commit: &Commit,
    page: &usize,
    page_size: &usize,
) -> Result<Vec<CommitEntry>, OxenError> {
    let reader = CommitEntryReader::new(repo, commit)?;
    reader.list_entry_page(*page, *page_size)
}

pub fn list_directory(
    repo: &LocalRepository,
    commit: &Commit,
    directory: &Path,
    revision: &str,
    page: usize,
    page_size: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    let entry_reader = CommitEntryReader::new(repo, commit)?;
    let commit_reader = CommitReader::new(repo)?;

    let mut dir_paths: Vec<DirEntry> = vec![];
    for dir in entry_reader.list_dirs()? {
        // log::debug!("LIST DIRECTORY considering committed dir: {:?} for search {:?}", dir, search_dir);
        if let Some(parent) = dir.parent() {
            if parent == directory || (parent == Path::new("") && directory == Path::new("./")) {
                dir_paths.push(dir_entry_from_dir(
                    repo,
                    commit,
                    &dir,
                    &commit_reader,
                    revision,
                )?);
            }
        }
    }
    log::debug!("list_directory got dir_paths {}", dir_paths.len());

    let mut file_paths: Vec<DirEntry> = vec![];
    let dir_entry_reader = CommitDirEntryReader::new(repo, &commit.id, directory)?;
    let total = dir_entry_reader.num_entries() + dir_paths.len();
    for entry in dir_entry_reader.list_entries()? {
        file_paths.push(dir_entry_from_commit_entry(
            repo,
            &entry,
            &commit_reader,
            revision,
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
        version: revision.to_string(),
    });
    Ok(PaginatedDirEntries::from_entries(
        dir_paths, resource, page, page_size, total,
    ))
}

pub fn compute_entries_size(entries: &[CommitEntry]) -> Result<u64, OxenError> {
    let total_size: u64 = entries.into_par_iter().map(|e| e.num_bytes).sum();
    Ok(total_size)
}

pub fn group_entries_to_parent_dirs(entries: &[CommitEntry]) -> HashMap<PathBuf, Vec<CommitEntry>> {
    let mut results: HashMap<PathBuf, Vec<CommitEntry>> = HashMap::new();

    for entry in entries.iter() {
        if let Some(parent) = entry.path.parent() {
            results
                .entry(parent.to_path_buf())
                .or_default()
                .push(entry.clone());
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::test;
    use crate::util;

    #[test]
    fn test_api_local_entries_list_all() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (file already created in helper)
            let file_to_add = repo.path.join("labels.txt");

            // Commit the file
            command::add(&repo, file_to_add)?;
            let commit = command::commit(&repo, "Adding labels file")?;

            let entries = api::local::entries::list_all(&repo, &commit)?;
            assert_eq!(entries.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_api_local_entries_count_one_for_commit() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (file already created in helper)
            let file_to_add = repo.path.join("labels.txt");

            // Commit the file
            command::add(&repo, file_to_add)?;
            let commit = command::commit(&repo, "Adding labels file")?;

            let count = api::local::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, 1);

            Ok(())
        })
    }

    #[test]
    fn test_api_local_entries_count_many_for_commit() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (files already created in helper)
            let dir_to_add = repo.path.join("train");
            let num_files = util::fs::rcount_files_in_dir(&dir_to_add);

            // Commit the dir
            command::add(&repo, &dir_to_add)?;
            let commit = command::commit(&repo, "Adding training data")?;
            let count = api::local::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, num_files);

            Ok(())
        })
    }

    #[test]
    fn test_api_local_entries_count_many_dirs() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (files already created in helper)
            let num_files = util::fs::rcount_files_in_dir(&repo.path);

            // Commit the dir
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all data")?;

            let count = api::local::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, num_files);

            Ok(())
        })
    }

    #[test]
    fn test_get_dir_entry_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let path = Path::new("annotations").join("train");
            let entry = api::local::entries::get_dir_entry(&repo, commit, &path)?;

            assert!(entry.is_dir);
            assert_eq!(entry.filename, "train");
            assert_eq!(Path::new(&entry.resource.unwrap().path), path);

            Ok(())
        })
    }

    #[test]
    fn test_get_dir_entry_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let path = test::test_nlp_classification_csv();
            let entry = api::local::entries::get_dir_entry(&repo, commit, path)?;

            assert!(!entry.is_dir);
            assert_eq!(entry.filename, "test.tsv");
            assert_eq!(
                Path::new(&entry.resource.unwrap().path),
                test::test_nlp_classification_csv()
            );

            Ok(())
        })
    }

    #[test]
    fn test_list_top_level_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let paginated = api::local::entries::list_directory(
                &repo,
                commit,
                Path::new("./"),
                &commit.id,
                1,
                10,
            )?;
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
    fn test_list_train_directory_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let paginated = api::local::entries::list_directory(
                &repo,
                commit,
                Path::new("train"),
                &commit.id,
                1,
                10,
            )?;
            let dir_entries = paginated.entries;
            let size = paginated.total_entries;

            assert_eq!(size, 5);
            assert_eq!(dir_entries.len(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_list_train_sub_directory_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let paginated = api::local::entries::list_directory(
                &repo,
                commit,
                Path::new("annotations/train"),
                &commit.id,
                1,
                10,
            )?;
            let dir_entries = paginated.entries;
            let size = paginated.total_entries;

            assert_eq!(size, 4);
            assert_eq!(dir_entries.len(), 4);

            Ok(())
        })
    }

    #[test]
    fn test_list_train_directory_subset() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let paginated = api::local::entries::list_directory(
                &repo,
                commit,
                Path::new("train"),
                &commit.id,
                2,
                3,
            )?;
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
    fn test_list_train_directory_exactly_ten() -> Result<(), OxenError> {
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
            let commit = command::commit(&repo, "Adding all the data")?;

            let page_number = 1;
            let page_size = 10;

            let paginated = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new("."),
                &commit.id,
                page_number,
                page_size,
            )?;
            assert_eq!(paginated.total_entries, 10);
            assert_eq!(paginated.total_pages, 1);
            assert_eq!(paginated.entries.len(), 10);

            Ok(())
        })
    }

    #[test]
    fn test_list_train_directory_exactly_ten_page_two() -> Result<(), OxenError> {
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
            let commit = command::commit(&repo, "Adding all the data")?;

            let page_number = 2;
            let page_size = 10;

            let paginated = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new("."),
                &commit.id,
                page_number,
                page_size,
            )?;
            assert_eq!(paginated.total_entries, 10);
            assert_eq!(paginated.total_pages, 1);
            assert_eq!(paginated.entries.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_list_train_directory_nine_entries_page_size_ten() -> Result<(), OxenError> {
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
            let commit = command::commit(&repo, "Adding all the data")?;

            let page_number = 1;
            let page_size = 10;

            let paginated = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new("."),
                &commit.id,
                page_number,
                page_size,
            )?;
            assert_eq!(paginated.total_entries, 9);
            assert_eq!(paginated.total_pages, 1);

            Ok(())
        })
    }

    #[test]
    fn test_list_train_directory_eleven_entries_page_size_ten() -> Result<(), OxenError> {
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
            let commit = command::commit(&repo, "Adding all the data")?;

            let page_number = 1;
            let page_size = 10;

            let paginated = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new("."),
                &commit.id,
                page_number,
                page_size,
            )?;
            assert_eq!(paginated.total_entries, 11);
            assert_eq!(paginated.total_pages, 2);

            Ok(())
        })
    }
}
