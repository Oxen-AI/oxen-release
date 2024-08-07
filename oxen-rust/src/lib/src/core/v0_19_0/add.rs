use glob::glob;
use jwalk::WalkDirGeneric;
use rayon::prelude::*;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::util;
use crate::util::progress_bar::spinner_with_msg;
use crate::{error::OxenError, model::LocalRepository};

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

    add_files(repo, &paths)?;

    // Stop the timer
    let duration = start.elapsed();
    log::debug!("---END--- oxen add: {:?} duration: {:?}", path, duration);

    println!("🐂 oxen add {:?} ({:?})", path, duration);
    Ok(())
}

fn add_files(repo: &LocalRepository, paths: &HashSet<PathBuf>) -> Result<(), OxenError> {
    // To start, let's see how fast we can simply loop through all the paths
    // and and copy them into an index.

    let progress = spinner_with_msg("Hashing files...");

    for path in paths {
        if path.is_dir() {
            process_dir(repo, path)?;
        } else if path.is_file() {
            // Process the file here
            // For example: hash_and_stage_file(repo, path)?;
        }
    }

    Ok(())
}

fn process_dir(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let walk_dir = WalkDirGeneric::<(usize, bool)>::new(path).process_read_dir(
        |depth, path, read_dir_state, children| {
            // 1. Custom sort
            // children.sort_by(|a, b| match (a, b) {
            //     (Ok(a), Ok(b)) => a.file_name.cmp(&b.file_name),
            //     (Ok(_), Err(_)) => Ordering::Less,
            //     (Err(_), Ok(_)) => Ordering::Greater,
            //     (Err(_), Err(_)) => Ordering::Equal,
            // });
            // 2. Custom filter
            // children.retain(|dir_entry_result| {
            //     dir_entry_result.as_ref().map(|dir_entry| {
            //         dir_entry.file_name
            //             .to_str()
            //             .map(|s| s.starts_with('.'))
            //             .unwrap_or(false)
            //     }).unwrap_or(false)
            // });
            // 3. Custom skip
            // children.iter_mut().for_each(|dir_entry_result| {
            //     if let Ok(dir_entry) = dir_entry_result {
            //         if dir_entry.depth == 2 {
            //             dir_entry.read_children_path = None;
            //         }
            //     }
            // });
            // 4. Custom state
            *read_dir_state += 1;
            children.first_mut().map(|dir_entry_result| {
                if let Ok(dir_entry) = dir_entry_result {
                    dir_entry.client_state = true;
                }
            });
        },
    );

    for entry in walk_dir {
        println!("{:?}", entry);
    }

    Ok(())
}
