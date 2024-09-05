use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::repositories;
use crate::util;
use crate::constants;
use crate::core::db;
use crate::core::v0_19_0::add;
use core::time::Duration;

use crate::model::Commit;
use crate::model::StagedEntryStatus;
use crate::constants::VERSIONS_DIR;
use crate::constants::STAGED_DIR;
use crate::model::EntryDataType;


use rmp_serde::Serializer;
use serde::Serialize;


use glob::glob;
use std::collections::HashSet;
use std::path::{Path, PathBuf};


use rocksdb::{DBWithThreadMode, MultiThreaded};


pub async fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {

    if repo.is_shallow_clone() {
            return Err(OxenError::repo_is_shallow());
    }

    /*
    if opts.remote {
        return remove_remote(repo, opts).await;
    }    
    */

    remove_files(repo, opts)
}

fn remove_files(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {

    let start = std::time::Instant::now();
    let path: &Path = opts.path.as_ref();
    let paths: HashSet<PathBuf> = parse_glob_path(path, repo, opts.recursive)?;

    // TODO: Handle intermittent failure
    // TODO: Accurately calculate # of files removed for remove_staged
    if opts.staged {

        for path in paths {

            if path.is_dir() {
                //remove_staged_dir(path.as_ref(), repo)?;
            }
            
            remove_staged_file(path.as_ref(), repo)?;
        }

        println!("🐂 oxen removed {} staged files", paths.len());
        
    } else {

        let stats = core::v0_19_0::add::add_files(repo, paths);
        
        // Stop the timer, and round the duration to the nearest second
        let duration = Duration::from_millis(start.elapsed().as_millis() as u64);
        log::debug!("---END--- oxen rm: {:?} duration: {:?}", path, duration);

        println!(
            "🐂 oxen removed {} files ({}) in {}",
            stats.total_files,
            bytesize::ByteSize::b(stats.total_bytes),
            humantime::format_duration(duration)
        );
    }

    Ok(())
}

// This function is extracted out to check for directories occurs if opts.recursive isn't set 
fn parse_glob_path(path: &Path, repo: &LocalRepository, recursive: bool) -> Result<HashSet<PathBuf>, OxenError> {
    
    let mut paths: HashSet<PathBuf> = HashSet::new();

    if recursive {
        if let Some(path_str) = path.to_str() {
            if util::fs::is_glob_path(path_str) {
                // Match against any untracked entries in the current dir
                // Remove matched paths from repo
                for entry in glob(path_str)? {
                    let full_path = repo.path.join(entry?);
                    log::debug!("REMOVING: {full_path:?}");
                    if full_path.exists() {
                        // user might have removed dir manually before using `oxen rm`
                        util::fs::remove_dir_all(&full_path)?;
                    }
                    paths.insert(full_path);
                }
            } else {
                // Non-glob path
                let full_path = repo.path.join(path);
                log::debug!("REMOVING: {full_path:?}");
                if full_path.exists() {
        
                    util::fs::remove_dir_all(&full_path)?;
                };
                paths.insert(path.to_owned());
            }
        }
    } else {
        // TODO: get HashSet of merkle tree dirs
        if let Some(path_str) = path.to_str() {
            if util::fs::is_glob_path(path_str) {

                for entry in glob(path_str)? {

                    let full_path = repo.path.join(entry?);

                    // TODO: throw error if full_path matches a dir in the merkle tree
                    log::debug!("REMOVING: {full_path:?}");

                    if full_path.exists() {
                        
                        util::fs::remove_file(&full_path)?;
                    } 

                    paths.insert(full_path);
                }
            } else {
                // Non-glob path
                let full_path = repo.path.join(path);
                log::debug!("REMOVING: {full_path:?}");
                if full_path.exists() {
        
                    util::fs::remove_dir_all(&full_path)?;
                };
                paths.insert(path.to_owned());
            }
        }
    }

    Ok(paths)
}

fn remove_staged_file(
    relative_path: &Path,
    repo: &LocalRepository
) -> Result<(), OxenError> {


    let repo_path = &repo.path;
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
   
    staged_db.delete(relative_path.to_str().unwrap())?;
    Ok(())
}


fn rm_staged_dir(
    repo: &LocalRepository,
    path: PathBuf,
) -> Result<(), OxenError> {


    let path = path.clone();
    let repo = repo.clone();
    let repo_path = repo.path.clone();
    let maybe_head_commit = maybe_head_commit.clone();
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


        let byte_counter_clone = Arc::clone(&byte_counter);
        let added_file_counter_clone = Arc::clone(&added_file_counter);
        let unchanged_file_counter_clone = Arc::clone(&unchanged_file_counter);


        let dir_path = util::fs::path_relative_to_dir(dir, &repo_path).unwrap();
        let dir_node = maybe_load_directory(&repo, &maybe_head_commit, &dir_path).unwrap();
        let seen_dirs = Arc::new(Mutex::new(HashSet::new()));


        // Curious why this is only < 300% CPU usage
        std::fs::read_dir(dir)?.for_each(|dir_entry_result| {
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


                let seen_dirs_clone = Arc::clone(&seen_dirs);
                match process_add_file(
                    &repo_path,
                    &versions_path,
                    &staged_db,
                    &dir_node,
                    &path,
                    &seen_dirs_clone,
                ) {
                    Ok(Some(node)) => {
                        if let EMerkleTreeNode::File(file_node) = &node.node.node {
                            byte_counter_clone.fetch_add(file_node.num_bytes, Ordering::Relaxed);
                            added_file_counter_clone.fetch_add(1, Ordering::Relaxed);
                            cumulative_stats.total_bytes += file_node.num_bytes;
                            cumulative_stats
                                .data_type_counts
                                .entry(file_node.data_type.clone())
                                .and_modify(|count| *count += 1)
                                .or_insert(1);
                            if node.status != StagedEntryStatus::Unmodified {
                                cumulative_stats.total_files += 1;
                            }
                        }
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
    }


    progress_1_clone.finish_and_clear();


    Ok(cumulative_stats)
}




