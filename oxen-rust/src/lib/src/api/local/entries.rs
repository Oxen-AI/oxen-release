//! Entries are the files and directories that are stored in a commit.
//!

use crate::core::index::object_db_reader::get_object_reader;
use crate::error::OxenError;
use crate::model::entry::commit_entry::{Entry, SchemaEntry};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::opts::DFOpts;
use crate::view::entry::ResourceVersion;
use crate::view::DataTypeCount;
use crate::{api, util};
use os_path::OsPath;
use rayon::prelude::*;

use crate::core::index::{CommitDirEntryReader, CommitEntryReader, CommitReader};
use crate::core::index::{ObjectDBReader, SchemaReader};
use crate::core::{self, index};
use crate::model::{
    Commit, CommitEntry, EntryDataType, LocalRepository, MetadataEntry, ParsedResource,
};
use crate::view::PaginatedDirEntries;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Get the entry for a given path in a commit.
/// Could be a file or a directory.
pub fn get_meta_entry(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<MetadataEntry, OxenError> {
    let object_reader = get_object_reader(repo, &commit.id)?;
    let entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &commit.id, object_reader.clone())?;
    let commit_reader = CommitReader::new(repo)?;
    let mut commits = commit_reader.history_from_commit_id(&commit.id)?;
    commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    // Check if the path is a dir or is the root
    if entry_reader.has_dir(path) || path == Path::new("") {
        log::debug!("get_meta_entry found dir: {:?}", path);
        meta_entry_from_dir(
            repo,
            object_reader,
            commit,
            path,
            &commit_reader,
            &commit.id,
        )
    } else {
        log::debug!("get_meta_entry has file: {:?}", path);

        let parent = path.parent().ok_or(OxenError::file_has_no_parent(path))?;
        let base_name = path.file_name().ok_or(OxenError::file_has_no_name(path))?;
        let dir_entry_reader =
            CommitDirEntryReader::new(repo, &commit.id, parent, object_reader.clone())?;

        // load all commit entry readers once
        let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();

        for c in commits {
            let object_reader = get_object_reader(repo, &c.id)?;
            let reader = CommitDirEntryReader::new(repo, &c.id, parent, object_reader.clone())?;
            commit_entry_readers.push((c.clone(), reader));
        }

        let entry = dir_entry_reader
            .get_entry(base_name)?
            .ok_or(OxenError::entry_does_not_exist_in_commit(path, &commit.id))?;
        meta_entry_from_commit_entry(repo, &entry, &commit_entry_readers, &commit.id)
    }
}

/// Get a DirEntry summing up the size of all files in a directory
/// and finding the latest commit within the directory
pub fn meta_entry_from_dir(
    repo: &LocalRepository,
    object_reader: Arc<ObjectDBReader>,
    commit: &Commit,
    path: &Path,
    commit_reader: &CommitReader,
    revision: &str,
) -> Result<MetadataEntry, OxenError> {
    // We cache the latest commit and size for each file in the directory after commit
    let latest_commit_path =
        core::cache::cachers::repo_size::dir_latest_commit_path(repo, commit, path);
    // log::debug!(
    //     "meta_entry_from_dir {:?} latest_commit_path: {:?}",
    //     path,
    //     latest_commit_path
    // );

    let latest_commit = match util::fs::read_from_path(latest_commit_path) {
        Ok(id) => {
            // log::debug!("meta_entry_from_dir found latest_commit on disk: {:?}", id);
            commit_reader.get_commit_by_id(id)?
        }
        Err(_) => {
            // log::debug!("meta_entry_from_dir computing latest_commit");
            compute_latest_commit_for_dir(repo, object_reader.clone(), commit, path, commit_reader)?
        }
    };

    let total_size_path = core::cache::cachers::repo_size::dir_size_path(repo, commit, path);
    let total_size = match util::fs::read_from_path(total_size_path) {
        Ok(total_size_str) => total_size_str
            .parse::<u64>()
            .map_err(|_| OxenError::basic_str("Could not get cached total size of dir"))?,
        Err(_) => {
            // cache failed, go compute it
            compute_dir_size(repo, object_reader.clone(), commit, path)?
        }
    };

    let dir_metadata = api::local::entries::get_dir_entry_metadata(repo, commit, path)?;

    let base_name = path.file_name().unwrap_or(std::ffi::OsStr::new(""));
    return Ok(MetadataEntry {
        filename: String::from(base_name.to_string_lossy()),
        is_dir: true,
        size: total_size,
        latest_commit,
        data_type: EntryDataType::Dir,
        mime_type: "inode/directory".to_string(),
        extension: util::fs::file_extension(path),
        resource: Some(ParsedResource {
            commit: Some(commit.clone()),
            branch: None,
            version: PathBuf::from(revision),
            path: path.to_path_buf(),
            resource: PathBuf::from(revision).join(path),
        }),
        metadata: Some(GenericMetadata::MetadataDir(dir_metadata)),
        is_queryable: None,
    });
}

fn compute_latest_commit_for_dir(
    repo: &LocalRepository,
    object_reader: Arc<ObjectDBReader>,
    commit: &Commit,
    path: &Path,
    commit_reader: &CommitReader,
) -> Result<Option<Commit>, OxenError> {
    let entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &commit.id, object_reader.clone())?;
    let commits: HashMap<String, Commit> = HashMap::new();
    let mut latest_commit = Some(commit.to_owned());
    // This lists all the committed dirs
    let dirs = entry_reader.list_dirs()?;
    for dir in dirs {
        // Have to make sure we are in a subset of the dir (not really a tree structure)
        if dir.starts_with(path) {
            let entry_reader =
                CommitDirEntryReader::new(repo, &commit.id, &dir, object_reader.clone())?;
            for entry in entry_reader.list_entries()? {
                let commit = if commits.contains_key(&entry.commit_id) {
                    Some(commits[&entry.commit_id].clone())
                } else {
                    commit_reader.get_commit_by_id(&entry.commit_id)?
                };

                if latest_commit.is_none() {
                    latest_commit.clone_from(&commit);
                }

                if latest_commit.as_ref().unwrap().timestamp < commit.as_ref().unwrap().timestamp {
                    latest_commit.clone_from(&commit);
                }
            }
        }
    }
    Ok(latest_commit)
}

fn compute_dir_size(
    repo: &LocalRepository,
    object_reader: Arc<ObjectDBReader>,
    commit: &Commit,
    path: &Path,
) -> Result<u64, OxenError> {
    let entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &commit.id, object_reader.clone())?;
    let mut total_size: u64 = 0;
    // This lists all the committed dirs
    let dirs = entry_reader.list_dirs()?;
    let object_reader = get_object_reader(repo, &commit.id)?;
    for dir in dirs {
        // Have to make sure we are in a subset of the dir (not really a tree structure)
        if dir.starts_with(path) {
            let entry_reader =
                CommitDirEntryReader::new(repo, &commit.id, &dir, object_reader.clone())?;
            for entry in entry_reader.list_entries()? {
                total_size += entry.num_bytes;
            }
        }
    }
    Ok(total_size)
}

pub fn get_commit_history_path(
    commits: &[(Commit, CommitDirEntryReader)],
    path: impl AsRef<Path>,
) -> Result<Vec<Commit>, OxenError> {
    let os_path = OsPath::from(path.as_ref()).to_pathbuf();
    let path = os_path
        .file_name()
        .ok_or(OxenError::file_has_no_name(&path))?;

    // log::debug!("get_commit_history_path: checking path {:?}", path);

    let mut latest_hash: Option<String> = None;
    let mut history: Vec<Commit> = Vec::new();

    for (commit, entry_reader) in commits.iter().rev() {
        if let Some(old_entry) = entry_reader.get_entry(path)? {
            if latest_hash.is_none() {
                // This is the first encountered entry; set it as the baseline for comparison.
                // log::debug!("get_commit_history_path: first entry {:?}", commit);
                latest_hash = Some(old_entry.hash.clone());
                history.push(commit.clone()); // Include the first commit as the starting point of history
            } else if latest_hash.as_ref() != Some(&old_entry.hash) {
                // A change in hash is detected, indicating an edit. Include this commit in history.
                // log::debug!("get_commit_history_path: new entry {:?}", commit);
                latest_hash = Some(old_entry.hash.clone());
                history.push(commit.clone());
            }
        }
    }

    history.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    Ok(history)
}

pub fn get_latest_commit_for_entry(
    commits: &[(Commit, CommitDirEntryReader)],
    entry: &CommitEntry,
) -> Result<Option<Commit>, OxenError> {
    get_latest_commit_for_path(commits, &entry.path)
}

pub fn get_latest_commit_for_path(
    commits: &[(Commit, CommitDirEntryReader)],
    path: &Path,
) -> Result<Option<Commit>, OxenError> {
    let os_path = OsPath::from(path).to_pathbuf();
    let path = os_path
        .file_name()
        .ok_or(OxenError::file_has_no_name(path))?;
    let mut latest_hash: Option<String> = None;
    // Store the commit from the previous iteration. Initialized as None.
    let mut previous_commit: Option<Commit> = None;

    for (commit, entry_reader) in commits.iter().rev() {
        if let Some(old_entry) = entry_reader.get_entry(path)? {
            if latest_hash.is_none() {
                // This is the first encountered entry, setting it as the baseline for comparison.
                latest_hash = Some(old_entry.hash.clone());
            } else if latest_hash.as_ref() != Some(&old_entry.hash) {
                // A change is detected, return the previous commit which introduced the change.
                return Ok(previous_commit);
            }
            // Update previous_commit after the check, so it holds the commit before the change was detected.
            previous_commit = Some(commit.clone());
        }
    }

    // If no change was detected (all entries have the same hash), or the entry was not found,
    // return None or consider returning the oldest commit if previous_commit has been set.
    Ok(previous_commit)
}

pub fn meta_entry_from_commit_entry(
    repo: &LocalRepository,
    entry: &CommitEntry,
    commit_entry_readers: &[(Commit, CommitDirEntryReader)],
    revision: &str,
) -> Result<MetadataEntry, OxenError> {
    log::debug!("meta_entry_from_commit_entry: {:?}", entry.path);
    let size = util::fs::version_file_size(repo, entry)?;
    let Some(latest_commit) = get_latest_commit_for_entry(commit_entry_readers, entry)? else {
        log::error!("No latest commit for entry: {:?}", entry.path);
        return Err(OxenError::basic_str(format!(
            "No latest commit for entry: {:?}",
            entry.path
        )));
    };

    let base_name = entry
        .path
        .file_name()
        .ok_or(OxenError::file_has_no_name(&entry.path))?;

    let version_path = util::fs::version_path(repo, entry);

    let data_type = util::fs::file_data_type(&version_path);

    let is_indexed = if data_type == EntryDataType::Tabular {
        Some(
            index::workspaces::data_frames::is_queryable_data_frame_indexed(
                repo,
                &entry.path,
                &latest_commit,
            )?,
        )
    } else {
        None
    };

    return Ok(MetadataEntry {
        filename: String::from(base_name.to_string_lossy()),
        is_dir: false,
        size,
        latest_commit: Some(latest_commit.clone()),
        data_type,
        mime_type: util::fs::file_mime_type(&version_path),
        extension: util::fs::file_extension(&version_path),
        resource: Some(ParsedResource {
            commit: Some(latest_commit.clone()),
            branch: None,
            version: PathBuf::from(revision),
            path: entry.path.clone(),
            resource: PathBuf::from(revision).join(entry.path.clone()),
        }),
        // Not applicable for files YET, but we will also compute this metadata
        metadata: None,
        is_queryable: is_indexed,
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

/// List all files and directories in a directory given a specific commit
// This is wayyyy more complicated that it needs to be because we have these two separate dbs....
pub fn list_directory(
    repo: &LocalRepository,
    commit: &Commit,
    directory: &Path,
    revision: &str,
    page: usize,
    page_size: usize,
) -> Result<(PaginatedDirEntries, MetadataEntry), OxenError> {
    let resource = Some(ResourceVersion {
        path: directory.to_str().unwrap().to_string(),
        version: revision.to_string(),
    });

    // Instantiate these readers once so they can be efficiently passed down through and databases not re-opened
    let object_reader = get_object_reader(repo, &commit.id)?;
    let entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &commit.id, object_reader.clone())?;
    let commit_reader = CommitReader::new(repo)?;

    // Find all the commits once, so that we can re-use to find the latest commit per entry
    let mut commits = commit_reader.history_from_commit_id(&commit.id)?;

    // Sort on timestamp oldest to newest
    commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();
    for c in commits {
        let or = get_object_reader(repo, &c.id)?;
        let reader = CommitDirEntryReader::new(repo, &c.id, directory, or.clone())?;
        commit_entry_readers.push((c.clone(), reader));
    }

    // List the directories first, then the files
    let mut dir_paths: Vec<MetadataEntry> = vec![];
    for dir in entry_reader.list_dirs()? {
        if let Some(parent) = dir.parent() {
            if parent == directory || (parent == Path::new("") && directory == Path::new("")) {
                dir_paths.push(meta_entry_from_dir(
                    repo,
                    object_reader.clone(),
                    commit,
                    &dir,
                    &commit_reader,
                    revision,
                )?);
            }
        }
    }

    let mut file_paths: Vec<MetadataEntry> = vec![];
    let dir_entry_reader =
        CommitDirEntryReader::new(repo, &commit.id, directory, object_reader.clone())?;
    let total = dir_entry_reader.num_entries() + dir_paths.len();
    let total_pages = (total as f64 / page_size as f64).ceil() as usize;

    let offset = dir_paths.len();

    for entry in dir_entry_reader.list_entry_page_with_offset(page, page_size, offset)? {
        file_paths.push(meta_entry_from_commit_entry(
            repo,
            &entry,
            &commit_entry_readers,
            revision,
        )?)
    }

    // Combine all paths, starting with dirs if there are enough, else just files
    let start_page = if page == 0 { 0 } else { page - 1 };
    let start_idx = start_page * page_size;
    let mut entries = if dir_paths.len() < start_idx {
        file_paths
    } else {
        dir_paths.append(&mut file_paths);
        dir_paths
    };

    if entries.len() > page_size {
        let mut end_idx = start_idx + page_size;
        if end_idx > entries.len() {
            end_idx = entries.len();
        }

        entries = entries[start_idx..end_idx].to_vec();
    }

    let metadata = get_dir_entry_metadata(repo, commit, directory)?;
    let dir = meta_entry_from_dir(
        repo,
        object_reader,
        commit,
        directory,
        &commit_reader,
        revision,
    )?;

    Ok((
        PaginatedDirEntries {
            entries,
            resource,
            metadata: Some(metadata),
            page_size,
            page_number: page,
            total_pages,
            total_entries: total,
        },
        dir,
    ))
}

pub fn get_dir_entry_metadata(
    repo: &LocalRepository,
    commit: &Commit,
    directory: &Path,
) -> Result<MetadataDir, OxenError> {
    let data_types_path =
        core::cache::cachers::content_stats::dir_column_path(repo, commit, directory, "data_type");

    // let mime_types_path =
    //     core::cache::cachers::content_stats::dir_column_path(repo, commit, directory, "mime_type");

    // log::debug!(
    //     "list_directory reading data types from {}",
    //     data_types_path.display()
    // );

    if let Ok(data_type_df) = core::df::tabular::read_df(&data_types_path, DFOpts::empty()) {
        let dt_series: Vec<&str> = data_type_df
            .column("data_type")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        let count_series: Vec<i64> = data_type_df
            .column("count")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();

        let data_types: Vec<DataTypeCount> = dt_series
            .iter()
            .zip(count_series.iter())
            .map(|(&data_type, &count)| DataTypeCount {
                data_type: data_type.to_string(),
                count: count.try_into().unwrap(),
            })
            .collect();

        Ok(MetadataDir::new(data_types))
    } else {
        log::warn!("Unable to read {data_types_path:?}");
        Ok(MetadataDir::new(vec![]))
    }
}

/// Given a list of entries, compute the total in bytes size of all entries.
pub fn compute_entries_size(entries: &[CommitEntry]) -> Result<u64, OxenError> {
    let total_size: u64 = entries.into_par_iter().map(|e| e.num_bytes).sum();
    Ok(total_size)
}

pub fn compute_generic_entries_size(entries: &[Entry]) -> Result<u64, OxenError> {
    let total_size: u64 = entries.into_par_iter().map(|e| e.num_bytes()).sum();
    Ok(total_size)
}

pub fn compute_schemas_size(schemas: &[SchemaEntry]) -> Result<u64, OxenError> {
    let total_size: u64 = schemas.into_par_iter().map(|e| e.num_bytes).sum();
    Ok(total_size)
}

/// Given a list of entries, group them by their parent directory.
pub fn group_commit_entries_to_parent_dirs(
    entries: &[CommitEntry],
) -> HashMap<PathBuf, Vec<CommitEntry>> {
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

pub fn group_entries_to_parent_dirs(entries: &[Entry]) -> HashMap<PathBuf, Vec<Entry>> {
    let mut results: HashMap<PathBuf, Vec<Entry>> = HashMap::new();

    for entry in entries.iter() {
        if let Some(parent) = entry.path().parent() {
            results
                .entry(parent.to_path_buf())
                .or_default()
                .push(entry.clone());
        }
    }

    results
}

pub fn group_schemas_to_parent_dirs(
    schema_entries: &[SchemaEntry],
) -> HashMap<PathBuf, Vec<SchemaEntry>> {
    let mut results: HashMap<PathBuf, Vec<SchemaEntry>> = HashMap::new();

    for entry in schema_entries.iter() {
        if let Some(parent) = entry.path.parent() {
            results
                .entry(parent.to_path_buf())
                .or_default()
                .push(entry.clone());
        }
    }

    results
}

pub fn read_unsynced_entries(
    local_repo: &LocalRepository,
    last_commit: &Commit,
    this_commit: &Commit,
) -> Result<Vec<CommitEntry>, OxenError> {
    // Find and compare all entries between this commit and last
    let this_entry_reader = CommitEntryReader::new(local_repo, this_commit)?;

    let this_entries = this_entry_reader.list_entries()?;
    let grouped = api::local::entries::group_commit_entries_to_parent_dirs(&this_entries);
    log::debug!(
        "Checking {} entries in {} groups",
        this_entries.len(),
        grouped.len()
    );

    let object_reader = get_object_reader(local_repo, &last_commit.id)?;

    let mut entries_to_sync: Vec<CommitEntry> = vec![];
    for (dir, dir_entries) in grouped.iter() {
        // log::debug!("Checking {} entries from {:?}", dir_entries.len(), dir);

        let last_entry_reader =
            CommitDirEntryReader::new(local_repo, &last_commit.id, dir, object_reader.clone())?;
        let mut entries: Vec<CommitEntry> = dir_entries
            .into_par_iter()
            .filter(|entry| {
                // If hashes are different, or it is a new entry, we'll keep it
                let filename = entry.path.file_name().unwrap().to_str().unwrap();
                match last_entry_reader.get_entry(filename) {
                    Ok(Some(old_entry)) => {
                        if old_entry.hash != entry.hash {
                            return true;
                        }
                    }
                    Ok(None) => {
                        return true;
                    }
                    Err(err) => {
                        panic!("Error filtering entries to sync: {}", err)
                    }
                }
                false
            })
            .map(|e| e.to_owned())
            .collect();
        entries_to_sync.append(&mut entries);
    }

    log::debug!("Got {} entries to sync", entries_to_sync.len());

    Ok(entries_to_sync)
}

pub fn read_unsynced_schemas(
    local_repo: &LocalRepository,
    last_commit: &Commit,
    this_commit: &Commit,
) -> Result<Vec<SchemaEntry>, OxenError> {
    let this_schema_reader = SchemaReader::new(local_repo, &this_commit.id, None)?;
    let last_schema_reader = SchemaReader::new(local_repo, &last_commit.id, None)?;

    let this_schemas = this_schema_reader.list_schema_entries()?;
    let last_schemas = last_schema_reader.list_schema_entries()?;

    let mut schemas_to_sync: Vec<SchemaEntry> = vec![];

    let this_grouped = api::local::entries::group_schemas_to_parent_dirs(&this_schemas);
    let last_grouped = api::local::entries::group_schemas_to_parent_dirs(&last_schemas);

    let empty_vec = Vec::new();
    for (dir, dir_schemas) in this_grouped.iter() {
        let last_dir_schemas = last_grouped.get(dir).unwrap_or(&empty_vec);
        for schema in dir_schemas {
            let filename = schema.path.file_name().unwrap().to_str().unwrap();
            let last_schema = last_dir_schemas
                .iter()
                .find(|s| s.path.file_name().unwrap().to_str().unwrap() == filename);
            if last_schema.is_none() || last_schema.unwrap().hash != schema.hash {
                schemas_to_sync.push(schema.clone());
            }
        }
    }
    Ok(schemas_to_sync)
}

pub fn list_tabular_files_in_repo(
    local_repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<MetadataEntry>, OxenError> {
    let schema_reader = core::index::SchemaReader::new(local_repo, &commit.id, None)?;
    let schemas = schema_reader.list_schemas()?;

    let mut meta_entries: Vec<MetadataEntry> = vec![];
    let entry_reader = CommitEntryReader::new(local_repo, commit)?;
    let commit_reader = CommitReader::new(local_repo)?;
    let commits = commit_reader.list_all()?;

    for (path, _schema) in schemas.iter() {
        let entry = entry_reader.get_entry(path)?;

        if entry.is_some() {
            let parent = path.parent().ok_or(OxenError::file_has_no_parent(path))?;
            let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();
            for commit in &commits {
                let object_reader = get_object_reader(local_repo, &commit.id)?;
                let reader = CommitDirEntryReader::new(
                    local_repo,
                    &commit.id,
                    parent,
                    object_reader.clone(),
                )?;
                commit_entry_readers.push((commit.clone(), reader));
            }

            let metadata = meta_entry_from_commit_entry(
                local_repo,
                &entry.unwrap(),
                &commit_entry_readers,
                &commit.id,
            )?;
            if metadata.data_type == EntryDataType::Tabular {
                meta_entries.push(metadata);
            }
        }
    }

    Ok(meta_entries)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use uuid::Uuid;

    use crate::api;
    use crate::command;
    use crate::core;
    use crate::core::index;
    use crate::error::OxenError;
    use crate::test;
    use crate::util;

    #[test]
    fn test_api_local_entries_list_all() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits("labels", |repo| {
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
        test::run_select_data_repo_test_no_commits("labels", |repo| {
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
        test::run_select_data_repo_test_no_commits("train", |repo| {
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
    fn test_get_meta_entry_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let path = Path::new("annotations").join("train");
            let entry = api::local::entries::get_meta_entry(&repo, commit, &path)?;

            assert!(entry.is_dir);
            assert_eq!(entry.filename, "train");
            assert_eq!(Path::new(&entry.resource.unwrap().path), path);

            Ok(())
        })
    }

    #[test]
    fn test_get_meta_entry_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let path = test::test_nlp_classification_csv();
            let entry = api::local::entries::get_meta_entry(&repo, commit, &path)?;

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
    fn test_list_directories_top_level_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let (paginated, _dir) = api::local::entries::list_directory(
                &repo,
                commit,
                Path::new(""),
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
    fn test_list_directories_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let (paginated, _dir) = api::local::entries::list_directory(
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

            let (paginated, _dir) = api::local::entries::list_directory(
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
    fn test_list_directories_subset() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let (paginated, _dir) = api::local::entries::list_directory(
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
    fn test_list_directories_1_exactly_ten() -> Result<(), OxenError> {
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

            // Run the compute cache
            let force = true;
            core::cache::commit_cacher::run_all(&repo, &commit, force)?;

            let page_number = 1;
            let page_size = 10;

            let (paginated, _dir) = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new(""),
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
    fn test_list_directories_all_dirs_no_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 42 directories
            for n in 0..42 {
                let dirname = format!("dir_{:0>3}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }

            // Add and commit all the dirs and files
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?;

            // Run the compute cache
            let force = true;
            core::cache::commit_cacher::run_all(&repo, &commit, force)?;

            let page_number = 2;
            let page_size = 10;

            let (paginated, _dir) = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new(""),
                &commit.id,
                page_number,
                page_size,
            )?;

            for entry in paginated.entries.iter() {
                println!("{:?}", entry.filename);
            }

            assert_eq!(paginated.entries.first().unwrap().filename, "dir_010");

            println!("{paginated:?}");
            assert_eq!(paginated.total_entries, 42);
            assert_eq!(paginated.total_pages, 5);
            assert_eq!(paginated.entries.len(), 10);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_101_dirs_no_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 101 directories
            for n in 0..101 {
                let dirname = format!("dir_{:0>3}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }

            // Add and commit all the dirs and files
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?;

            // Run the compute cache
            let force = true;
            core::cache::commit_cacher::run_all(&repo, &commit, force)?;

            let page_number = 11;
            let page_size = 10;

            let (paginated, _dir) = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new(""),
                &commit.id,
                page_number,
                page_size,
            )?;

            for entry in paginated.entries.iter() {
                println!("{:?}", entry.filename);
            }

            assert_eq!(paginated.entries.first().unwrap().filename, "dir_100");

            println!("{paginated:?}");
            assert_eq!(paginated.total_entries, 101);
            assert_eq!(paginated.total_pages, 11);
            assert_eq!(paginated.entries.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_exactly_ten_page_two() -> Result<(), OxenError> {
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

            // Run the compute cache
            let force = true;
            core::cache::commit_cacher::run_all(&repo, &commit, force)?;

            let page_number = 2;
            let page_size = 10;

            let (paginated, _dir) = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new(""),
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
    fn test_list_directories_nine_entries_page_size_ten() -> Result<(), OxenError> {
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

            // Run the compute cache
            let force = true;
            core::cache::commit_cacher::run_all(&repo, &commit, force)?;

            let page_number = 1;
            let page_size = 10;

            let (paginated, _dir) = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new(""),
                &commit.id,
                page_number,
                page_size,
            )?;
            assert_eq!(paginated.total_entries, 9);
            assert_eq!(paginated.total_pages, 1);
            assert_eq!(paginated.entries.len(), 9);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_eleven_entries_page_size_ten() -> Result<(), OxenError> {
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

            // Run the compute cache
            let force = true;
            core::cache::commit_cacher::run_all(&repo, &commit, force)?;

            let page_number = 1;
            let page_size = 10;

            let (paginated, _dir) = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new(""),
                &commit.id,
                page_number,
                page_size,
            )?;
            assert_eq!(paginated.total_entries, 11);
            assert_eq!(paginated.total_pages, 2);
            assert_eq!(paginated.entries.len(), page_size);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_many_dirs_many_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create many directories
            let num_dirs = 32;
            for n in 0..num_dirs {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }

            // Create many files
            let num_files = 45;
            for n in 0..num_files {
                let filename = format!("file_{}.txt", n);
                let filepath = repo.path.join(filename);
                util::fs::write(filepath, format!("helloooo {}", n))?;
            }

            // Add and commit all the dirs and files
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?;

            // Run the compute cache
            let force = true;
            core::cache::commit_cacher::run_all(&repo, &commit, force)?;

            let page_number = 1;
            let page_size = 10;

            let (paginated, _dir) = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new(""),
                &commit.id,
                page_number,
                page_size,
            )?;
            assert_eq!(paginated.total_entries, num_dirs + num_files);
            assert_eq!(paginated.total_pages, 8);
            assert_eq!(paginated.entries.len(), page_size);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_one_dir_many_files_page_2() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create one directory
            let dir_path = repo.path.join("lonely_dir");
            util::fs::create_dir_all(&dir_path)?;
            let filename = "data.txt";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "All the lonely directories")?;

            // Create many files
            let num_files = 45;
            for n in 0..num_files {
                let filename = format!("file_{}.txt", n);
                let filepath = repo.path.join(filename);
                util::fs::write(filepath, format!("helloooo {}", n))?;
            }

            // Add and commit all the dirs and files
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?;

            // Run the compute cache
            let force = true;
            core::cache::commit_cacher::run_all(&repo, &commit, force)?;

            let page_number = 2;
            let page_size = 10;

            let (paginated, _dir) = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new(""),
                &commit.id,
                page_number,
                page_size,
            )?;

            assert_eq!(paginated.total_entries, num_files + 1);
            assert_eq!(paginated.total_pages, 5);
            assert_eq!(paginated.entries.len(), page_size);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_many_dir_some_files_page_2() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create many directories
            let num_dirs = 9;
            for n in 0..num_dirs {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }

            // Create many files
            let num_files = 8;
            for n in 0..num_files {
                let filename = format!("file_{}.txt", n);
                let filepath = repo.path.join(filename);
                util::fs::write(filepath, format!("helloooo {}", n))?;
            }

            // Add and commit all the dirs and files
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?;

            // Run the compute cache
            let force = true;
            core::cache::commit_cacher::run_all(&repo, &commit, force)?;

            let page_number = 2;
            let page_size = 10;

            let (paginated, _dir) = api::local::entries::list_directory(
                &repo,
                &commit,
                Path::new(""),
                &commit.id,
                page_number,
                page_size,
            )?;

            assert_eq!(paginated.total_entries, num_files + num_dirs);
            assert_eq!(paginated.total_pages, 2);
            assert_eq!(paginated.entries.len(), 7);

            Ok(())
        })
    }

    #[test]
    fn test_list_tabular() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create a deeply nested directory
            let dir_path = repo
                .path
                .join("data")
                .join("train")
                .join("images")
                .join("cats");
            util::fs::create_dir_all(&dir_path)?;

            // Add two tabular files to it
            let filename = "cats.tsv";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            let filename = "dogs.csv";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "1,2,3\nhello,world,sup\n")?;

            // And write a file in the same dir that is not tabular
            let filename = "README.md";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // And write a tabular file to the root dir
            let filename = "labels.tsv";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            // And write a non tabular file to the root dir
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            // Add and commit all
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?;

            // List files
            let entries = api::local::entries::list_tabular_files_in_repo(&repo, &commit)?;

            assert_eq!(entries.len(), 3);

            // Add another tabular file
            let filename = "dogs.tsv";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            // Add and commit all
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding additional file")?;

            let entries = api::local::entries::list_tabular_files_in_repo(&repo, &commit)?;

            assert_eq!(entries.len(), 4);

            // Remove the deeply nested dir
            util::fs::remove_dir_all(&dir_path)?;

            command::add(&repo, dir_path)?;
            let commit = command::commit(&repo, "Removing dir")?;

            let entries = api::local::entries::list_tabular_files_in_repo(&repo, &commit)?;
            assert_eq!(entries.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_file_metadata_shows_is_indexed() -> Result<(), OxenError> {
        // skip on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_empty_local_repo_test(|repo| {
            // Create a deeply nested directory
            let dir_path = repo
                .path
                .join("data")
                .join("train")
                .join("images")
                .join("cats");
            util::fs::create_dir_all(&dir_path)?;

            // Add two tabular files to it
            let filename_1 = "cats.tsv";
            let filepath_1 = dir_path.join(filename_1);
            util::fs::write(filepath_1, "1\t2\t3\nhello\tworld\tsup\n")?;

            let filename_2 = "dogs.csv";
            let filepath_2 = dir_path.join(filename_2);
            util::fs::write(filepath_2, "1,2,3\nhello,world,sup\n")?;

            let path_1 = PathBuf::from("data")
                .join("train")
                .join("images")
                .join("cats")
                .join(filename_1);

            let path_2 = PathBuf::from("data")
                .join("train")
                .join("images")
                .join("cats")
                .join(filename_2);

            // And write a file in the same dir that is not tabular
            let filename = "README.md";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all
            command::add(&repo, &repo.path)?;
            let commit = command::commit(&repo, "Adding all the data")?;

            // Get the metadata entries for the two dataframes
            let meta1 = api::local::entries::get_meta_entry(&repo, &commit, &path_1)?;
            let meta2 = api::local::entries::get_meta_entry(&repo, &commit, &path_2)?;

            let entry2 = api::local::entries::get_commit_entry(&repo, &commit, &path_2)?
                .expect("Failed: could not get commit entry");

            assert_eq!(meta1.is_queryable, Some(false));
            assert_eq!(meta2.is_queryable, Some(false));

            // Now index df2
            let workspace_id = Uuid::new_v4().to_string();
            let workspace = index::workspaces::create(&repo, &commit, workspace_id, false)?;
            index::workspaces::data_frames::index(&workspace, &entry2.path)?;

            // Now get the metadata entries for the two dataframes
            let meta1 = api::local::entries::get_meta_entry(&repo, &commit, &path_1)?;
            let meta2 = api::local::entries::get_meta_entry(&repo, &commit, &path_2)?;

            assert_eq!(meta1.is_queryable, Some(false));
            assert_eq!(meta2.is_queryable, Some(true));

            Ok(())
        })
    }
}
