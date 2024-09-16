use crate::core::db;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::repositories;
use crate::util;

use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::model::merkle_tree::node::FileNode;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use tokio::time::Duration;
use walkdir::WalkDir;

use crate::core::v0_19_0::add::CumulativeStats;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;
use std::sync::Arc;
use std::sync::Mutex;

use crate::constants::STAGED_DIR;
use crate::constants::VERSIONS_DIR;
use crate::model::merkle_tree::node::DirNode;
use crate::model::Commit;
use crate::model::StagedEntryStatus;

use rmp_serde::Serializer;
use serde::Serialize;

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::constants::FILES_DIR;
use crate::constants::OXEN_HIDDEN_DIR;

use rocksdb::{DBWithThreadMode, MultiThreaded};

pub async fn rm(
    paths: &HashSet<PathBuf>,
    repo: &LocalRepository,
    opts: &RmOpts,
) -> Result<(), OxenError> {
    if repo.is_shallow_clone() {
        return Err(OxenError::repo_is_shallow());
    }

    /*
    if opts.remote {
        return remove_remote(repo, opts).await;
    }
    */

    // TODO: Accurately calculate stats for remove_staged
    if opts.staged {
        return remove_staged(repo, paths);
    }

    remove(paths, repo, opts)
}

fn remove(
    paths: &HashSet<PathBuf>,
    repo: &LocalRepository,
    opts: &RmOpts,
) -> Result<(), OxenError> {
    let start = std::time::Instant::now();
    log::debug!("paths: {:?}", paths);

    let maybe_head_commit = repositories::commits::head_commit_maybe(repo)?;
    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };

    for path in paths {
        // Remove dirs
        if path.is_dir() {
            // Stage removed directory, searching all entries
            match remove_dir(repo, &maybe_head_commit, path.clone()) {
                Ok(dir_stats) => {
                    total += dir_stats;
                }
                Err(err) => {
                    println!("Err: {err:?}");
                    // TODO: Other error handling
                }
            }

            // Remove files
        } else if path.is_file() {
            match remove_file(repo, &maybe_head_commit, path) {
                Ok(entry) => {
                    if let Some(entry) = entry {
                        if let EMerkleTreeNode::File(file_node) = &entry.node.node {
                            let data_type = file_node.data_type.clone();
                            total.total_files += 1;
                            total.total_bytes += file_node.num_bytes;
                            total
                                .data_type_counts
                                .entry(data_type)
                                .and_modify(|count| *count += 1)
                                .or_insert(1);
                        }
                    }
                }
                Err(err) => {
                    println!("Err: {err:?}");
                    // TODO: Other error handling
                }
            }

            let full_path = repo.path.join(path);
            log::debug!("REMOVING FILE: {full_path:?}");
            if full_path.exists() {
                util::fs::remove_file(&full_path)?;
            }
        } else {
            let mut maybe_dir_node = None;
            log::debug!("Found non-existant path: {path:?}");
            if let Some(ref head_commit) = maybe_head_commit {
                let path = util::fs::path_relative_to_dir(path, &repo.path)?;
                let parent_path = path.parent().unwrap_or(Path::new(""));
                maybe_dir_node =
                    CommitMerkleTree::dir_with_children(repo, head_commit, parent_path)?;
            }

            if let Ok(Some(_dir_node)) = get_dir_node(&maybe_dir_node, path) {
                log::debug!("non-existant path {path:?} was dir. Calling remove_dir");
                remove_dir(repo, &maybe_head_commit, path.to_path_buf());
            } else if let Ok(Some(_file_node)) = get_file_node(&maybe_dir_node, path) {
                log::debug!("non-existant path {path:?} was file. Calling remove_file");
                let opts = RmOpts::from_path(path);
                remove_file(repo, &maybe_head_commit, path);
            }
        }

        // TODO: Refactor remove_dir to check paths in the merkle tree
        // That would allow this logic to safely happen within the loop above
        for path in paths {
            if path.is_dir() {
                // Remove dir from working directory
                let full_path = repo.path.join(path);
                log::debug!("REMOVING DIR: {full_path:?}");
                if full_path.exists() {
                    // user might have removed file manually before using `oxen rm`
                    util::fs::remove_dir_all(&full_path)?;
                }
            } else {
                let full_path = repo.path.join(path);
                log::debug!("REMOVING FILE: {full_path:?}");
                if full_path.exists() {
                    util::fs::remove_file(&full_path)?;
                }
            }
        }

        // Stop the timer, and round the duration to the nearest second
        let duration = Duration::from_millis(start.elapsed().as_millis() as u64);
        log::debug!("---END--- oxen rm: {:?} duration: {:?}", paths, duration);

        // TODO: Add function to CumulativeStats to output that print statement
        println!(
            "🐂 oxen removed {} files ({}) in {}",
            total.total_files,
            bytesize::ByteSize::b(total.total_bytes),
            humantime::format_duration(duration)
        );
    }

    Ok(())
}

pub fn remove_staged(repo: &LocalRepository, paths: &HashSet<PathBuf>) -> Result<(), OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    for path in paths {
        log::debug!("path: {:?}", path);

        if path.is_dir() {
            remove_staged_dir(repo, path, &staged_db)?;
        } else {
            remove_staged_file(repo, path, &staged_db)?;
        }
    }

    Ok(())
}

// TODO: should removing directories from the index require the recursive flag?

fn remove_staged_file(
    repo: &LocalRepository,
    relative_path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
) -> Result<(), OxenError> {
    log::debug!("Deleting entry: {relative_path:?}");
    staged_db.delete(relative_path.to_str().unwrap())?;

    Ok(())
}

fn remove_staged_dir(
    repo: &LocalRepository,
    path: &PathBuf,
    staged_db: &DBWithThreadMode<MultiThreaded>,
) -> Result<(), OxenError> {
    log::debug!("remove staged dir: {path:?}");

    let path = path.clone();

    let walker = WalkDir::new(&path).into_iter();
    for entry in walker.filter_entry(|e| e.file_type().is_dir() && e.file_name() != OXEN_HIDDEN_DIR)
    {
        log::debug!("entry: {entry:?}");
        let entry = entry.unwrap();
        let dir = entry.path();

        std::fs::read_dir(dir)?.for_each(|dir_entry_result| {
            log::debug!("dir_entry_result: {dir_entry_result:?}");
            if let Ok(dir_entry) = dir_entry_result {
                let path = dir_entry.path();

                if path.is_dir() {
                    remove_staged_dir(repo, &path, staged_db);
                }
                remove_staged_file(repo, &path, staged_db);
            }
        });
        log::debug!("Deleting entry: {dir:?}");
        staged_db.delete(dir.to_str().unwrap())?;
    }

    Ok(())
}

pub fn remove_file(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    println!("Remove file");
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
        maybe_dir_node = CommitMerkleTree::dir_with_children(repo, head_commit, parent_path)?;
    }

    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    process_remove_file(
        &repo_path,
        &versions_path,
        &staged_db,
        &maybe_dir_node,
        path,
        &seen_dirs,
    )
}

pub fn process_remove_file(
    repo_path: &Path,
    versions_path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    maybe_dir_node: &Option<MerkleTreeNode>,
    path: &Path,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let relative_path = util::fs::path_relative_to_dir(path, repo_path)?;
    let full_path = repo_path.join(&relative_path);

    // Find node to remove
    let file_path = relative_path.file_name().unwrap();

    let node: MerkleTreeNode = if let Some(file_node) = get_file_node(maybe_dir_node, file_path)? {
        MerkleTreeNode::from_file(file_node)
    } else {
        let error = format!("File {relative_path:?} must be committed to use `oxen rm`");
        return Err(OxenError::basic_str(error));
    };

    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Removed,
        node: node.clone(),
    };

    // Remove the file from the versions db
    // Take first 2 chars of hash as dir prefix and last N chars as the dir suffix
    let dir_prefix_len = 2;
    let dir_name = node.hash.to_string();
    let dir_prefix = dir_name.chars().take(dir_prefix_len).collect::<String>();
    let dir_suffix = dir_name.chars().skip(dir_prefix_len).collect::<String>();
    let dst_dir = versions_path.join(dir_prefix).join(dir_suffix);

    let dst = dst_dir.join("data");
    util::fs::remove_dir_all(&dst);

    // Write removed node to staged db
    log::debug!("writing removed file to staged db: {}", staged_entry);
    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();

    let relative_path_str = relative_path.to_str().unwrap();
    staged_db.put(relative_path_str, &buf).unwrap();

    // Add all the parent dirs to the staged db
    let mut parent_path = relative_path.to_path_buf();
    let mut seen_dirs = seen_dirs.lock().unwrap();
    while let Some(parent) = parent_path.parent() {
        let relative_path = util::fs::path_relative_to_dir(parent, repo_path).unwrap();
        parent_path = parent.to_path_buf();

        let relative_path_str = relative_path.to_str().unwrap();
        if !seen_dirs.insert(relative_path.to_owned()) {
            // Don't write the same dir twice
            continue;
        }

        // Ensures that removed entries don't have their parents re-added by oxen rm
        // RocksDB's DBWithThreadMode only has this function to check if a key exists in the DB, so I added the else condition to make this reliable

        let dir_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Added,
            node: MerkleTreeNode::default_dir_from_path(&relative_path),
        };

        log::debug!("writing dir to staged db: {}", dir_entry);
        let mut buf = Vec::new();
        dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db.put(relative_path_str, &buf).unwrap();

        if relative_path == Path::new("") {
            break;
        }
    }

    Ok(Some(staged_entry))
}

pub fn process_remove_file_and_parents(
    repo_path: &Path,
    versions_path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    maybe_dir_node: &Option<MerkleTreeNode>,
    path: &Path,
    dir: &Path,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let relative_path = util::fs::path_relative_to_dir(path, repo_path)?;
    let full_path = repo_path.join(&relative_path);

    // Find node to remove
    let file_path = relative_path.file_name().unwrap();

    // TODO: This might be buggy. What if we add a dir but also a file within the dir? will this throw an error then?
    let node: MerkleTreeNode = if let Some(file_node) = get_file_node(maybe_dir_node, file_path)? {
        MerkleTreeNode::from_file(file_node)
    } else {
        let error = format!("File {relative_path:?} must be committed to use `oxen rm`");
        return Err(OxenError::basic_str(error));
    };

    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Removed,
        node: node.clone(),
    };

    // Remove the file from the versions db
    // Take first 2 chars of hash as dir prefix and last N chars as the dir suffix
    let dir_prefix_len = 2;
    let dir_name = node.hash.to_string();
    let dir_prefix = dir_name.chars().take(dir_prefix_len).collect::<String>();
    let dir_suffix = dir_name.chars().skip(dir_prefix_len).collect::<String>();
    let dst_dir = versions_path.join(dir_prefix).join(dir_suffix);

    let dst = dst_dir.join("data");
    util::fs::remove_dir_all(&dst);

    // Write removed node to staged db
    log::debug!("writing removed file to staged db: {}", staged_entry);
    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();

    let relative_path_str = relative_path.to_str().unwrap();
    staged_db.put(relative_path_str, &buf).unwrap();

    // Add all the parent dirs to the staged db
    let mut parent_path = relative_path.to_path_buf();
    let mut seen_dirs = seen_dirs.lock().unwrap();

    // Stage parents as removed until we find the original dir
    while let Some(parent) = parent_path.parent() {
        let relative_path = util::fs::path_relative_to_dir(parent, repo_path).unwrap();

        if parent_path == dir {
            break;
        }

        parent_path = parent.to_path_buf();

        let relative_path_str = relative_path.to_str().unwrap();
        if !seen_dirs.insert(relative_path.to_owned()) {
            // Don't write the same dir twice
            continue;
        }

        let dir_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Removed,
            node: MerkleTreeNode::default_dir_from_path(&relative_path),
        };

        log::debug!("writing dir to staged db: {}", dir_entry);
        let mut buf = Vec::new();
        dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db.put(relative_path_str, &buf).unwrap();

        if relative_path == Path::new("") {
            break;
        }
    }

    // Stage the remaining parents as Added
    while let Some(parent) = parent_path.parent() {
        let relative_path = util::fs::path_relative_to_dir(parent, repo_path).unwrap();
        parent_path = parent.to_path_buf();

        let relative_path_str = relative_path.to_str().unwrap();
        if !seen_dirs.insert(relative_path.to_owned()) {
            // Don't write the same dir twice
            continue;
        }

        let dir_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Added,
            node: MerkleTreeNode::default_dir_from_path(&relative_path),
        };

        log::debug!("writing dir to staged db: {}", dir_entry);
        let mut buf = Vec::new();
        dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db.put(relative_path_str, &buf).unwrap();

        if relative_path == Path::new("") {
            break;
        }
    }

    Ok(Some(staged_entry))
}

pub fn remove_dir(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: PathBuf,
) -> Result<CumulativeStats, OxenError> {
    let versions_path = util::fs::oxen_hidden_dir(&repo.path)
        .join(VERSIONS_DIR)
        .join(FILES_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    process_remove_dir(repo, maybe_head_commit, &versions_path, &staged_db, path)
}

fn process_remove_dir(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    versions_path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
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

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    let byte_counter = Arc::new(AtomicU64::new(0));
    let removed_file_counter = Arc::new(AtomicU64::new(0));
    let unchanged_file_counter = Arc::new(AtomicU64::new(0));
    let progress_1_clone = Arc::clone(&progress_1);

    let mut cumulative_stats = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };

    let walker = WalkDir::new(&path).into_iter();
    for entry in walker.filter_entry(|e| e.file_type().is_dir() && e.file_name() != OXEN_HIDDEN_DIR)
    {
        let entry = entry.unwrap();
        let dir = entry.path();

        log::debug!("Entry is: {entry:?}");

        let byte_counter_clone = Arc::clone(&byte_counter);
        let removed_file_counter_clone = Arc::clone(&removed_file_counter);
        let unchanged_file_counter_clone = Arc::clone(&unchanged_file_counter);

        let dir_path = util::fs::path_relative_to_dir(dir, &repo_path).unwrap();
        let dir_node = maybe_load_directory(&repo, &maybe_head_commit, &dir_path).unwrap();
        let seen_dirs = Arc::new(Mutex::new(HashSet::new()));

        // Curious why this is only < 300% CPU usage
        std::fs::read_dir(dir)?.for_each(|dir_entry_result| {
            if let Ok(dir_entry) = dir_entry_result {
                log::debug!("Dir Entry is: {dir_entry:?}");
                let total_bytes = byte_counter_clone.load(Ordering::Relaxed);
                let path = dir_entry.path();
                let duration = start.elapsed().as_secs_f32();
                let mbps = (total_bytes as f32 / duration) / 1_000_000.0;

                progress_1.set_message(format!(
                    "🐂 remove {} files, {} unchanged ({}) {:.2} MB/s",
                    removed_file_counter_clone.load(Ordering::Relaxed),
                    unchanged_file_counter_clone.load(Ordering::Relaxed),
                    bytesize::ByteSize::b(total_bytes),
                    mbps
                ));

                let seen_dirs_clone = Arc::clone(&seen_dirs);
                match process_remove_file_and_parents(
                    &repo_path,
                    versions_path,
                    staged_db,
                    &dir_node,
                    &path,
                    &dir_path,
                    &seen_dirs_clone,
                ) {
                    Ok(Some(node)) => {
                        if let EMerkleTreeNode::File(file_node) = &node.node.node {
                            byte_counter_clone.fetch_add(file_node.num_bytes, Ordering::Relaxed);
                            removed_file_counter_clone.fetch_add(1, Ordering::Relaxed);
                            cumulative_stats.total_bytes += file_node.num_bytes;
                            cumulative_stats
                                .data_type_counts
                                .entry(file_node.data_type.clone())
                                .and_modify(|count| *count += 1)
                                .or_insert(1);
                        }
                    }
                    // TODO: Error handling
                    Err(e) => {
                        log::error!("Error adding file: {:?}", e);
                    }
                    _ => {
                        log::error!("Error adding file: file {dir_entry:?} not found in {dir:?}");
                    }
                }
            }
        });
    }

    progress_1_clone.finish_and_clear();
    Ok(cumulative_stats)
}

fn get_file_node(
    dir_node: &Option<MerkleTreeNode>,
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

fn get_dir_node(
    dir_node: &Option<MerkleTreeNode>,
    path: impl AsRef<Path>,
) -> Result<Option<DirNode>, OxenError> {
    if let Some(node) = dir_node {
        if let Some(node) = node.get_by_path(path)? {
            if let EMerkleTreeNode::Directory(dir_node) = &node.node {
                Ok(Some(dir_node.clone()))
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

fn maybe_load_directory(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    if let Some(head_commit) = maybe_head_commit {
        let dir_node = CommitMerkleTree::dir_with_children(repo, head_commit, path)?;
        Ok(dir_node)
    } else {
        Ok(None)
    }
}
