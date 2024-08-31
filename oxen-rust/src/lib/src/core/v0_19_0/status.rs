use crate::constants::OXEN_HIDDEN_DIR;
use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::core::v0_19_0::structs::{EntryMetaData, EntryMetaDataWithPath};
use crate::error::OxenError;
use crate::model::MerkleTreeNodeType;
use crate::model::{
    EntryDataType, LocalRepository, MerkleHash, StagedData, StagedDirStats, StagedEntry,
    StagedEntryStatus, SummarizedStagedDirStats,
};
use crate::{repositories, util};

use filetime::FileTime;
use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{DBWithThreadMode, IteratorMode, SingleThreaded};
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::time::Duration;

use super::index::merkle_tree::node::MerkleTreeNodeData;
use super::index::merkle_tree::CommitMerkleTree;

pub fn status(repo: &LocalRepository) -> Result<StagedData, OxenError> {
    status_from_dir(repo, Path::new(""))
}

pub fn status_from_dir(
    repo: &LocalRepository,
    dir: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    let mut staged_data = StagedData::empty();

    let read_progress = ProgressBar::new_spinner();
    read_progress.set_style(ProgressStyle::default_spinner());
    read_progress.enable_steady_tick(Duration::from_millis(100));

    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let maybe_staged_db = if db_path.join("CURRENT").exists() {
        // Read the staged files from the staged db
        let opts = db::key_val::opts::default();
        let db: DBWithThreadMode<SingleThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_path), true)?;
        Some(db)
    } else {
        None
    };

    find_untracked_and_modified_paths(
        repo,
        &dir,
        &mut staged_data,
        &maybe_staged_db,
        &read_progress,
    )?;

    let Some(db) = maybe_staged_db else {
        return Ok(staged_data);
    };

    let (dir_entries, _) = read_staged_entries_below_path(repo, &db, &dir, &read_progress)?;
    read_progress.finish_and_clear();

    let mut summarized_dir_stats = SummarizedStagedDirStats {
        num_files_staged: 0,
        total_files: 0,
        paths: HashMap::new(),
    };

    log::debug!("dir_entries.len(): {:?}", dir_entries.len());
    for (dir, entries) in dir_entries {
        log::debug!(
            "dir_entries dir: {:?} entries.len(): {:?}",
            dir,
            entries.len()
        );
        let mut stats = StagedDirStats {
            path: dir,
            num_files_staged: 0,
            total_files: 0,
            status: StagedEntryStatus::Added,
        };
        for entry in entries {
            log::debug!("dir_entries entry: {:?}", entry);
            match entry.data_type {
                EntryDataType::Dir => {
                    stats.num_files_staged += 1;
                }
                _ => {
                    let staged_entry = StagedEntry {
                        hash: entry.hash.to_string(),
                        status: StagedEntryStatus::Added,
                    };
                    staged_data.staged_files.insert(entry.path, staged_entry);
                }
            }
        }
        summarized_dir_stats.add_stats(&stats);
    }

    staged_data.staged_dirs = summarized_dir_stats;

    Ok(staged_data)
}

pub fn read_staged_entries(
    repo: &LocalRepository,
    db: &DBWithThreadMode<SingleThreaded>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<EntryMetaDataWithPath>>, u64), OxenError> {
    read_staged_entries_below_path(repo, db, Path::new(""), read_progress)
}

pub fn read_staged_entries_below_path(
    repo: &LocalRepository,
    db: &DBWithThreadMode<SingleThreaded>,
    start_path: impl AsRef<Path>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<EntryMetaDataWithPath>>, u64), OxenError> {
    let start_path = start_path.as_ref();
    let mut total_entries = 0;
    let iter = db.iterator(IteratorMode::Start);
    let mut dir_entries: HashMap<PathBuf, Vec<EntryMetaDataWithPath>> = HashMap::new();
    for item in iter {
        match item {
            // key = file path
            // value = EntryMetaData
            Ok((key, value)) => {
                let key = str::from_utf8(&key)?;
                let path = Path::new(key);
                let entry: EntryMetaData = rmp_serde::from_slice(&value).unwrap();
                log::debug!("read_staged_entries key {} entry: {}", key, entry);

                let entry_w_path = EntryMetaDataWithPath {
                    path: path.to_path_buf(),
                    hash: entry.hash,
                    num_bytes: entry.num_bytes,
                    data_type: entry.data_type,
                    status: entry.status,
                    last_commit_id: MerkleHash::new(0),
                    last_modified_seconds: entry.last_modified_seconds,
                    last_modified_nanoseconds: entry.last_modified_nanoseconds,
                };

                if let Some(parent) = path.parent() {
                    log::debug!(
                        "read_staged_entries parent {:?} start_path {:?}",
                        parent,
                        start_path
                    );
                    let relative_start_path =
                        util::fs::path_relative_to_dir(&start_path, &repo.path)?;
                    if relative_start_path == PathBuf::from("")
                        || parent.starts_with(&relative_start_path)
                    {
                        dir_entries
                            .entry(parent.to_path_buf())
                            .or_default()
                            .push(entry_w_path);
                    }
                } else {
                    log::debug!("read_staged_entries root {:?}", path);
                    dir_entries
                        .entry(PathBuf::from(""))
                        .or_default()
                        .push(entry_w_path);
                }

                total_entries += 1;
                read_progress.set_message(format!("Found {} entries", total_entries));
            }
            Err(err) => {
                log::error!("Could not get staged entry: {}", err);
            }
        }
    }

    Ok((dir_entries, total_entries))
}

fn find_untracked_and_modified_paths(
    repo: &LocalRepository,
    start_path: impl AsRef<Path>,
    staged_data: &mut StagedData,
    staged_db: &Option<DBWithThreadMode<SingleThreaded>>,
    progress: &ProgressBar,
) -> Result<(), OxenError> {
    let mut seen_paths: HashSet<PathBuf> = HashSet::new();
    let maybe_head_commit = repositories::commits::head_commit_maybe(repo)?;

    // Candidate directories are the start path
    // and all the directories in the current tree
    // that are descendants of the start path
    let mut candidate_dirs: HashSet<PathBuf> = HashSet::new();
    candidate_dirs.insert(start_path.as_ref().to_path_buf());
    let dir_hashes = if let Some(head_commit) = maybe_head_commit {
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, &head_commit)?;
        for (dir, _) in &dir_hashes {
            let dir = repo.path.join(dir);
            if dir.starts_with(start_path.as_ref()) {
                candidate_dirs.insert(dir);
            }
        }
        dir_hashes
    } else {
        HashMap::new()
    };

    // List the directories in the current tree, and check if they have untracked files
    // Files in working directory as candidates
    let mut total_files = 0;
    for candidate_dir in &candidate_dirs {
        let relative_dir = util::fs::path_relative_to_dir(&candidate_dir, &repo.path)?;
        log::debug!(
            "find_untracked_and_modified_paths finding untracked files in {:?} relative {:?}",
            candidate_dir,
            relative_dir
        );
        let dir_node = if let Some(hash) = dir_hashes.get(&relative_dir) {
            log::debug!(
                "find_untracked_and_modified_paths dir node for {:?} is {:?}",
                relative_dir,
                hash
            );
            CommitMerkleTree::read_depth(repo, hash, 2)?
        } else {
            None
        };

        let read_dir = std::fs::read_dir(candidate_dir);
        if read_dir.is_ok() {
            for path in read_dir? {
                total_files += 1;
                progress.set_message(format!("Checking {} untracked files", total_files));

                let path = path?.path();
                let relative_path = util::fs::path_relative_to_dir(&path, &repo.path)?;
                log::debug!(
                    "find_untracked_and_modified_paths checking relative path {:?} in {:?}",
                    relative_path,
                    relative_dir
                );

                // Don't add the directories that are in the current tree
                if candidate_dirs.contains(&path) {
                    continue;
                }

                // Skip hidden .oxen files
                if relative_path.starts_with(OXEN_HIDDEN_DIR) {
                    continue;
                }

                // Skip duplicates
                if !seen_paths.insert(path.to_path_buf()) {
                    continue;
                }

                let file_name = path
                    .file_name()
                    .ok_or(OxenError::basic_str("path has no file name"))?;
                if let Some(node) = maybe_get_child_node(&file_name, &dir_node)? {
                    log::debug!(
                        "find_untracked_and_modified_paths checking if modified {:?}",
                        relative_path
                    );
                    if is_modified(&node, &path)? {
                        log::debug!(
                            "find_untracked_and_modified_paths file {:?} is modified",
                            path
                        );
                        staged_data.modified_files.push(relative_path);
                    }
                } else {
                    log::debug!("find_untracked_and_modified_paths adding untracked candidate from dir {:?}", relative_path);
                    if let Some(staged_db) = staged_db {
                        let key = relative_path.to_str().unwrap();
                        if staged_db.get(key.as_bytes())?.is_some() {
                            continue;
                        }
                    }

                    if path.is_file() {
                        staged_data.untracked_files.push(relative_path);
                    } else if path.is_dir() {
                        staged_data.untracked_dirs.push((relative_path, 0));
                    }
                }
            }
        }
    }

    Ok(())
}

fn maybe_get_child_node(
    path: impl AsRef<Path>,
    dir_node: &Option<MerkleTreeNodeData>,
) -> Result<Option<MerkleTreeNodeData>, OxenError> {
    log::debug!("checking is_untracked for {:?}", path.as_ref());
    let Some(node) = dir_node else {
        return Ok(None);
    };

    node.get_by_path(path)
}

fn is_modified(node: &MerkleTreeNodeData, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    // Check the file timestamps vs the commit timestamps
    let metadata = std::fs::metadata(path)?;
    let mtime = FileTime::from_last_modification_time(&metadata);

    let (node_modified_seconds, node_modified_nanoseconds) = match node.dtype {
        MerkleTreeNodeType::File => {
            let file = node.file()?;
            let node_modified_seconds = file.last_modified_seconds;
            let node_modified_nanoseconds = file.last_modified_nanoseconds;
            (node_modified_seconds, node_modified_nanoseconds)
        }
        MerkleTreeNodeType::Dir => {
            let dir = node.dir()?;
            let node_modified_seconds = dir.last_modified_seconds;
            let node_modified_nanoseconds = dir.last_modified_nanoseconds;
            (node_modified_seconds, node_modified_nanoseconds)
        }
        _ => {
            return Err(OxenError::basic_str("unsupported node type"));
        }
    };

    if node_modified_nanoseconds != mtime.nanoseconds()
        || node_modified_seconds != mtime.unix_seconds()
    {
        return Ok(true);
    }

    Ok(false)
}
