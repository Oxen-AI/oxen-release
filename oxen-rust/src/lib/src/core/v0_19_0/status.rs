use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::core::v0_19_0::structs::{EntryMetaData, EntryMetaDataWithPath};
use crate::error::OxenError;
use crate::model::{
    EntryDataType, LocalRepository, MerkleHash, StagedData, StagedDirStats, StagedEntry,
    StagedEntryStatus, SummarizedStagedDirStats,
};
use crate::{repositories, util};

use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{DBWithThreadMode, IteratorMode, SingleThreaded};
use std::collections::HashMap;
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


    let untracked_files = find_untracked_paths(repo, &dir, &read_progress)?;
    for file in untracked_files {
        log::debug!("untracked file: {}", file.display());
        staged_data.untracked_files.push(file);
    }

    // Read the staged files from the staged db
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);

    if !db_path.join("CURRENT").exists() {
        return Ok(staged_data);
    }

    let db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_path), true)?;

    let (dir_entries, _) = read_staged_entries_below_path(&db, &dir, &read_progress)?;
    read_progress.finish_and_clear();

    let mut summarized_dir_stats = SummarizedStagedDirStats {
        num_files_staged: 0,
        total_files: 0,
        paths: HashMap::new(),
    };

    for (dir, entries) in dir_entries {
        let mut stats = StagedDirStats {
            path: dir,
            num_files_staged: 0,
            total_files: 0,
            status: StagedEntryStatus::Added,
        };
        for entry in entries {
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
    db: &DBWithThreadMode<SingleThreaded>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<EntryMetaDataWithPath>>, u64), OxenError> {
    read_staged_entries_below_path(db, Path::new(""), read_progress)
}

pub fn read_staged_entries_below_path(
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
                    if Path::new("") == start_path || parent.starts_with(start_path) {
                        dir_entries
                            .entry(parent.to_path_buf())
                            .or_default()
                            .push(entry_w_path);
                    }
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

fn find_untracked_paths(
    repo: &LocalRepository,
    start_path: impl AsRef<Path>,
    progress: &ProgressBar,
) -> Result<Vec<PathBuf>, OxenError> {
    let mut untracked_files = Vec::new();
    let maybe_head_commit = repositories::commits::head_commit_maybe(repo)?;

    let maybe_tree = if let Some(head_commit) = maybe_head_commit {
        CommitMerkleTree::read_node(repo, &head_commit.hash()?, true)?
    } else {
        None
    };

    // Recursively walk the current start_path and see if there are any files that are not in the current tree
    log::debug!("finding untracked files in {:?}", start_path.as_ref());
    let read_dir = std::fs::read_dir(start_path);
    if read_dir.is_ok() {
        // Files in working directory as candidates
        let mut total_files = 0;
        for path in read_dir? {
            total_files += 1;
            progress.set_message(format!("Checking {} untracked files", total_files));

            let path = path?.path();

            // Skip hidden files
            if path.starts_with(OXEN_HIDDEN_DIR) {
                continue;
            }

            let path = util::fs::path_relative_to_dir(&path, &repo.path)?;
            if !is_untracked(&path, &maybe_tree)? {
                log::debug!("adding candidate from dir {:?}", path);
                untracked_files.push(path);
            }
        }
    }

    return Ok(untracked_files);
}

fn is_untracked(path: impl AsRef<Path>, root: &Option<MerkleTreeNodeData>) -> Result<bool, OxenError> {
    log::debug!("checking is_untracked for {:?}", path.as_ref());
    let Some(root) = root else {
        // If we don't have a tree, the path is untracked
        return Ok(true);
    };

    // If the path is not in the tree, it is untracked
    Ok(root.get_by_path(path)?.is_none())
}