use filetime::FileTime;
use glob::glob;
use jwalk::WalkDirGeneric;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tokio::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use rmp_serde::Serializer;
use serde::Serialize;

use crate::constants::{FILES_DIR, STAGED_DIR, VERSIONS_DIR};
use crate::core::db;
use crate::core::v0_19_0::structs::EntryMetaData;
use crate::model::{Commit, EntryDataType, MerkleHash, StagedEntryStatus};
use crate::{error::OxenError, model::LocalRepository};
use crate::{repositories, util};
use std::ops::AddAssign;

use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNodeData};

#[derive(Clone, Debug, Default)]
pub struct CumulativeStats {
    total_files: usize,
    total_bytes: u64,
    data_type_counts: HashMap<EntryDataType, usize>,
}

impl AddAssign<CumulativeStats> for CumulativeStats {
    fn add_assign(&mut self, other: CumulativeStats) {
        self.total_files += other.total_files;
        self.total_bytes += other.total_bytes;
        for (data_type, count) in other.data_type_counts {
            *self.data_type_counts.entry(data_type).or_insert(0) += count;
        }
    }
}

pub fn add(repo: &LocalRepository, path: impl AsRef<Path>) -> Result<(), OxenError> {
    // Collect paths that match the glob pattern either:
    // 1. In the repo working directory (untracked or modified files)
    // 2. In the commit entry db (removed files)

    // Start a timer
    let start = std::time::Instant::now();
    let path = path.as_ref();
    let mut paths: HashSet<PathBuf> = HashSet::new();
    if let Some(path_str) = path.to_str() {
        if util::fs::is_glob_path(path_str) {
            // Match against any untracked entries in the current dir
            for entry in glob(path_str)? {
                paths.insert(entry?);
            }
        } else {
            // Non-glob path
            paths.insert(path.to_owned());
        }
    }

    let stats = add_files(repo, &paths)?;

    // Stop the timer, and round the duration to the nearest second
    let duration = Duration::from_millis(start.elapsed().as_millis() as u64);
    log::debug!("---END--- oxen add: {:?} duration: {:?}", path, duration);

    println!(
        "🐂 oxen added {} files ({}) in {}",
        stats.total_files,
        bytesize::ByteSize::b(stats.total_bytes),
        humantime::format_duration(duration)
    );

    Ok(())
}

fn add_files(
    repo: &LocalRepository,
    paths: &HashSet<PathBuf>,
) -> Result<CumulativeStats, OxenError> {
    // To start, let's see how fast we can simply loop through all the paths
    // and and copy them into an index.

    // Create the versions dir if it doesn't exist
    let versions_path = util::fs::oxen_hidden_dir(&repo.path).join(VERSIONS_DIR);
    if !versions_path.exists() {
        util::fs::create_dir_all(versions_path)?;
    }

    // Lookup the head commit
    let maybe_head_commit = repositories::commits::head_commit_maybe(repo)?;

    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };
    for path in paths {
        if path.is_dir() {
            total += process_dir(repo, &maybe_head_commit, path.clone())?;
        } else if path.is_file() {
            // Process the file here
            let entry = add_file(repo, &maybe_head_commit, path)?;
            if let Some(entry) = entry {
                total.total_files += 1;
                total.total_bytes += entry.num_bytes;
                total
                    .data_type_counts
                    .entry(entry.data_type)
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }
    }

    Ok(total)
}

fn process_dir(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: PathBuf,
) -> Result<CumulativeStats, OxenError> {
    let start = std::time::Instant::now();

    let progress_1 = Arc::new(ProgressBar::new_spinner());
    progress_1.set_style(ProgressStyle::default_spinner());
    progress_1.enable_steady_tick(Duration::from_millis(100));

    let path = path.clone();
    let repo = repo.clone();
    let maybe_head_commit = maybe_head_commit.clone();
    let repo_path = repo.path.clone();
    let versions_path = util::fs::oxen_hidden_dir(&repo.path)
        .join(VERSIONS_DIR)
        .join(FILES_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    let byte_counter = Arc::new(AtomicU64::new(0));
    let added_file_counter = Arc::new(AtomicU64::new(0));
    let unchanged_file_counter = Arc::new(AtomicU64::new(0));
    let progress_1_clone = Arc::clone(&progress_1);

    let walk_dir = WalkDirGeneric::<(usize, EntryMetaData)>::new(&path).process_read_dir(
        move |_depth, dir, state, children| {
            let byte_counter_clone = Arc::clone(&byte_counter);
            let added_file_counter_clone = Arc::clone(&added_file_counter);
            let unchanged_file_counter_clone = Arc::clone(&unchanged_file_counter);

            let num_children = children.len();
            progress_1.set_message(format!(
                "Processing dir [{:?}] with {} entries",
                dir, num_children
            ));
            *state += 1;

            let dir_path = util::fs::path_relative_to_dir(dir, &repo_path).unwrap();
            let dir_node = maybe_load_directory(&repo, &maybe_head_commit, &dir_path).unwrap();

            // Curious why this is only < 300% CPU usage
            children.par_iter_mut().for_each(|dir_entry_result| {
                if let Ok(dir_entry) = dir_entry_result {
                    let total_bytes = byte_counter_clone.load(Ordering::Relaxed);
                    let path = dir_entry.path();
                    let duration = start.elapsed().as_secs_f32();
                    let mbps = (total_bytes as f32 / duration) / 1_000_000.0;

                    progress_1.set_message(format!(
                        "🐂 add {} files, {} unchanged ({}) {:.2} MB/s",
                        added_file_counter_clone.load(Ordering::Relaxed),
                        unchanged_file_counter_clone.load(Ordering::Relaxed),
                        bytesize::ByteSize::b(total_bytes),
                        mbps
                    ));
                    match process_add_file(&repo_path, &versions_path, &staged_db, &dir_node, &path)
                    {
                        Ok(Some(entry)) => {
                            if entry.data_type != EntryDataType::Dir {
                                byte_counter_clone.fetch_add(entry.num_bytes, Ordering::Relaxed);
                                added_file_counter_clone.fetch_add(1, Ordering::Relaxed);
                            }

                            dir_entry.client_state = entry;
                        }
                        Ok(None) => {
                            unchanged_file_counter_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            log::error!("Error adding file: {:?}", e);
                        }
                    }
                }
            });
        },
    );

    let mut cumulative_stats = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };
    for dir_entry in walk_dir.into_iter().flatten() {
        cumulative_stats.total_bytes += dir_entry.client_state.num_bytes;

        if dir_entry.client_state.data_type != EntryDataType::Dir
            && dir_entry.client_state.status != StagedEntryStatus::Unmodified
        {
            cumulative_stats.total_files += 1;
        }

        cumulative_stats
            .data_type_counts
            .entry(dir_entry.client_state.data_type)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        // progress_2.set_message(format!(
        //     "🐂 Added {} files {}",
        //     cumulative_stats.total_files,
        //     bytesize::ByteSize::b(cumulative_stats.total_bytes)
        // ));
    }

    progress_1_clone.finish_and_clear();

    Ok(cumulative_stats)
}

fn maybe_load_directory(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<MerkleTreeNodeData>, OxenError> {
    if let Some(head_commit) = maybe_head_commit {
        let dir_node = CommitMerkleTree::dir_from_path_with_children(repo, head_commit, path)?;
        Ok(dir_node)
    } else {
        Ok(None)
    }
}

fn get_file_node(
    dir_node: &Option<MerkleTreeNodeData>,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    if let Some(node) = dir_node {
        if let Some(node) = node.get_by_path(path)? {
            if let EMerkleTreeNode::File(file_node) = &node.node {
                Ok(Some(file_node.clone()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn add_file(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<EntryMetaData>, OxenError> {
    let repo_path = repo.path.clone();
    let versions_path = util::fs::oxen_hidden_dir(&repo.path)
        .join(VERSIONS_DIR)
        .join(FILES_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let mut maybe_dir_node = None;
    if let Some(head_commit) = maybe_head_commit {
        let path = util::fs::path_relative_to_dir(path, &repo_path)?;
        let parent_path = path.parent().unwrap_or(Path::new(""));
        maybe_dir_node =
            CommitMerkleTree::dir_from_path_with_children(repo, head_commit, parent_path)?;
    }

    process_add_file(
        &repo_path,
        &versions_path,
        &staged_db,
        &maybe_dir_node,
        path,
    )
}

fn process_add_file(
    repo_path: &Path,
    versions_path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    maybe_dir_node: &Option<MerkleTreeNodeData>,
    path: &Path,
) -> Result<Option<EntryMetaData>, OxenError> {
    let relative_path = util::fs::path_relative_to_dir(path, repo_path)?;
    let full_path = repo_path.join(&relative_path);
    if !full_path.is_file() {
        // If it's not a file - no need to add it
        // We handle directories by traversing the parents of files below
        return Ok(Some(EntryMetaData {
            hash: MerkleHash::new(0),
            num_bytes: 0,
            data_type: EntryDataType::Dir,
            status: StagedEntryStatus::Unmodified,
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
        }));
    }

    // Check if the file is already in the head commit
    let file_path = relative_path.file_name().unwrap();
    let maybe_file_node = get_file_node(maybe_dir_node, file_path)?;

    // This is ugly - but makes sure we don't have to rehash the file if it hasn't changed
    let (status, hash, num_bytes, mtime) = if let Some(file_node) = maybe_file_node {
        // first check if the file timestamp is different
        let metadata = std::fs::metadata(path)?;
        let mtime = FileTime::from_last_modification_time(&metadata);
        log::debug!("path: {:?}", path);
        log::debug!(
            "file_node.last_modified_seconds: {}",
            file_node.last_modified_seconds
        );
        log::debug!(
            "file_node.last_modified_nanoseconds: {}",
            file_node.last_modified_nanoseconds
        );
        log::debug!("mtime.unix_seconds(): {}", mtime.unix_seconds());
        log::debug!("mtime.nanoseconds(): {}", mtime.nanoseconds());
        log::debug!(
            "has_different_modification_time: {}",
            has_different_modification_time(&file_node, &mtime)
        );
        log::debug!("-----------------------------------");
        if has_different_modification_time(&file_node, &mtime) {
            let hash = util::hasher::get_hash_given_metadata(&full_path, &metadata)?;
            if file_node.hash.to_u128() != hash {
                (
                    StagedEntryStatus::Modified,
                    MerkleHash::new(hash),
                    file_node.num_bytes,
                    mtime,
                )
            } else {
                (
                    StagedEntryStatus::Unmodified,
                    MerkleHash::new(hash),
                    file_node.num_bytes,
                    mtime,
                )
            }
        } else {
            (
                StagedEntryStatus::Unmodified,
                file_node.hash,
                file_node.num_bytes,
                mtime,
            )
        }
    } else {
        let metadata = std::fs::metadata(path)?;
        let mtime = FileTime::from_last_modification_time(&metadata);
        let hash = util::hasher::get_hash_given_metadata(&full_path, &metadata)?;
        (
            StagedEntryStatus::Added,
            MerkleHash::new(hash),
            metadata.len(),
            mtime,
        )
    };

    // Don't have to add the file to the staged db if it hasn't changed
    if status == StagedEntryStatus::Unmodified {
        return Ok(None);
    }

    // Get the data type of the file
    let data_type = util::fs::file_data_type(&full_path);

    // Add the file to the versions db
    // Take first 2 chars of hash as dir prefix and last N chars as the dir suffix
    let dir_prefix_len = 2;
    let dir_name = hash.to_string();
    let dir_prefix = dir_name.chars().take(dir_prefix_len).collect::<String>();
    let dir_suffix = dir_name.chars().skip(dir_prefix_len).collect::<String>();
    let dst_dir = versions_path.join(dir_prefix).join(dir_suffix);

    if !dst_dir.exists() {
        util::fs::create_dir_all(&dst_dir).unwrap();
    }

    let dst = dst_dir.join("data");
    util::fs::copy(&full_path, &dst).unwrap();

    let entry = EntryMetaData {
        hash,
        data_type,
        num_bytes,
        status,
        last_modified_seconds: mtime.unix_seconds(),
        last_modified_nanoseconds: mtime.nanoseconds(),
    };

    let mut buf = Vec::new();
    entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
    staged_db
        .put(relative_path.to_str().unwrap(), &buf)
        .unwrap();

    // Add all the parent dirs to the staged db
    let mut parent_path = relative_path.to_path_buf();
    while let Some(parent) = parent_path.parent() {
        let relative_path = util::fs::path_relative_to_dir(parent, repo_path).unwrap();

        let dir_entry = EntryMetaData {
            data_type: EntryDataType::Dir,
            status: StagedEntryStatus::Added,
            ..Default::default()
        };

        let mut buf = Vec::new();
        dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db
            .put(relative_path.to_str().unwrap(), &buf)
            .unwrap();

        parent_path = parent.to_path_buf();

        if relative_path == Path::new("") {
            break;
        }
    }
    Ok(Some(entry))
}

pub fn has_different_modification_time(node: &FileNode, time: &FileTime) -> bool {
    node.last_modified_nanoseconds != time.nanoseconds()
        || node.last_modified_seconds != time.unix_seconds()
}
