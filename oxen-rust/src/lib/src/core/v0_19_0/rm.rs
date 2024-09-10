use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::repositories;
use crate::util;
use crate::constants;
use crate::core::db;


use crate::core;
use tokio::time::Duration;
use walkdir::WalkDir;

use crate::core::v0_19_0::add::CumulativeStats;
use crate::model::merkle_tree::node::EMerkleTreeNode;

use crate::model::Commit;
use crate::model::StagedEntryStatus;
use crate::constants::VERSIONS_DIR;
use crate::constants::STAGED_DIR;
use crate::model::EntryDataType;


use rmp_serde::Serializer;
use serde::Serialize;


use glob::glob;
use std::collections::HashSet;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::constants::FILES_DIR;
use crate::constants::OXEN_HIDDEN_DIR;


use rocksdb::{DBWithThreadMode, MultiThreaded};


pub async fn rm(paths: &HashSet<PathBuf>, repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
   
    println!("rm start");

    if repo.is_shallow_clone() {
            return Err(OxenError::repo_is_shallow());
    }

    /*
    if opts.remote {
        return remove_remote(repo, opts).await;
    }    
    */

    remove_files(paths, repo, opts)
}

fn remove_files(paths: &HashSet<PathBuf>, repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {

    let start = std::time::Instant::now();
    println!("paths: {:?}", paths);

    // TODO: Accurately calculate stats for remove_staged
    if opts.staged {

        println!("Staged: {:?}", paths);
        for path in paths {

            if path.is_dir() {
                remove_staged_dir(&repo, &path)?;
            } else {
                remove_staged_file(&repo, &path)?; 
            }  
        }

        println!("🐂 oxen removed {} staged files", paths.len());
        
    } else {

        let maybe_head_commit = repositories::commits::head_commit_maybe(repo)?;
        let mut total = CumulativeStats {
            total_files: 0,
            total_bytes: 0,
            data_type_counts: HashMap::new(),
        };

        // TODO: Right now, this will delete the file even if oxen rm fails. Is that an issue?
        // Iterate over paths, remove if necessary, match core logic for adding files and dirs respectively
        println!("Not Staged: {:?}", paths);
        for path in paths {

            if path.is_dir() {

                let full_path = repo.path.join(path);
                log::debug!("REMOVING FILE: {full_path:?}");
                if full_path.exists() {
                    // user might have removed file manually before using `oxen rm`
                    util::fs::remove_dir_all(&full_path)?;
                }
    
                match core::v0_19_0::add::add_dir(repo, &maybe_head_commit, path.clone()) {
                    Ok(dir_stats) => {
                        total += dir_stats;
                    },
                    Err(err) => {
                        println!("Err: {err:?}");
                        // TODO: Other error handling
                    }
                }
    
            } else {

                let full_path = repo.path.join(path);
                log::debug!("REMOVING FILE: {full_path:?}");
                if full_path.exists() {
                    // user might have removed file manually before using `oxen rm`
                    util::fs::remove_file(&full_path)?;
                }

                match core::v0_19_0::add::add_file(repo, &maybe_head_commit, path) {
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
                    },
                    Err(err) => {
                        println!("Err: {err:?}");
                    },
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


// TODO: should removing directories from the index require the recursive flag?

/* 
                        log::debug!("REMOVING: {full_path:?}");
                        if full_path.exists() {
                            // user might have removed dir manually before using `oxen rm`
                            util::fs::remove_paths(&full_path)?;
                        }
*/

fn remove_staged_file(
    repo: &LocalRepository,
    relative_path: &Path
) -> Result<(), OxenError> {

    println!("remove staged file: {relative_path:?}");

    let repo_path = &repo.path;
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    println!("db_path: {db_path:?}");

    // test//ez.tsv 
    // test/ez.tsv 
    // NEED: test\ez.tsv

    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    println!("Deleting entry: {relative_path:?}");
    staged_db.delete(relative_path.to_str().unwrap())?;
    Ok(())
}


fn remove_staged_dir(
    repo: &LocalRepository,
    path: &PathBuf
) -> Result<(), OxenError> {

    println!("REMOVE STAGED DIR");

    let path = path.clone();
    let repo = repo.clone();
    let repo_path = repo.path.clone();
    let versions_path = util::fs::oxen_hidden_dir(&repo.path)
        .join(VERSIONS_DIR)
        .join(FILES_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let walker = WalkDir::new(&path).into_iter();
    for entry in walker.filter_entry(|e| e.file_type().is_dir() && e.file_name() != OXEN_HIDDEN_DIR)
    {
        println!("entry: {entry:?}");
        let entry = entry.unwrap();
        let dir = entry.path();

        // Curious why this is only < 300% CPU usage
        std::fs::read_dir(dir)?.for_each(|dir_entry_result| {
            println!("dir_entry_result: {dir_entry_result:?}");
            if let Ok(dir_entry) = dir_entry_result {

                let path = dir_entry.path();
                
                println!("Deleting entry: {path:?}");
                match staged_db.delete(path.to_str().unwrap()) {
                    Err(err) => {
                        log::debug!("Err: {err:?}");
                    }
                    _ => {
                        log::debug!("Successfully removed file: {path:?}");
                    }
                }    
            }
        });

        // remove staged file: dir
    }

    Ok(())
}




