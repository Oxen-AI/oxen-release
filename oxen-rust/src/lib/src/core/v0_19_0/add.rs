use glob::glob;
use jwalk::WalkDirGeneric;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tokio::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};

use crate::constants::{STAGED_DIR, VERSIONS_DIR};
use crate::core::db;
use crate::model::EntryDataType;
use crate::util;
use crate::{error::OxenError, model::LocalRepository};
use std::ops::AddAssign;

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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EntryMetaData {
    pub hash: u128,
    pub num_bytes: u64,
    pub data_type: EntryDataType,
}

impl Default for EntryMetaData {
    fn default() -> Self {
        EntryMetaData {
            hash: 0,
            num_bytes: 0,
            data_type: EntryDataType::Binary,
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

    // Stop the timer
    let duration = start.elapsed();
    log::debug!("---END--- oxen add: {:?} duration: {:?}", path, duration);

    println!(
        "🐂 oxen add {:?} added {} files ({}) in {:?}",
        path,
        stats.total_files,
        bytesize::ByteSize::b(stats.total_bytes),
        duration
    );

    Ok(())
}

fn add_files(
    repo: &LocalRepository,
    paths: &HashSet<PathBuf>,
) -> Result<CumulativeStats, OxenError> {
    // To start, let's see how fast we can simply loop through all the paths
    // and and copy them into an index.

    let versions_path = util::fs::oxen_hidden_dir(&repo.path).join(VERSIONS_DIR);
    if !versions_path.exists() {
        util::fs::create_dir_all(versions_path)?;
    }

    let m = MultiProgress::new();
    let progress_1 = m.add(ProgressBar::new_spinner());
    progress_1.set_style(ProgressStyle::default_spinner());
    progress_1.enable_steady_tick(Duration::from_millis(100));

    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };
    for path in paths {
        if path.is_dir() {
            total += process_dir(
                repo,
                path,
                &progress_1,
                // &progress_2
            )?;
        } else if path.is_file() {
            // Process the file here
            // For example: hash_and_stage_file(repo, path)?;
            todo!()
        }
    }

    Ok(total)
}

fn process_dir(
    repo: &LocalRepository,
    path: &Path,
    progress_1: &ProgressBar,
    // progress_2: &ProgressBar,
) -> Result<CumulativeStats, OxenError> {
    let repo_path = repo.path.clone();
    let versions_path = util::fs::oxen_hidden_dir(&repo.path).join(VERSIONS_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let walk_dir = WalkDirGeneric::<(usize, EntryMetaData)>::new(path).process_read_dir(
        move |_depth, _path, _state, children| {
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
            children.par_iter_mut().for_each(|dir_entry_result| {
                if let Ok(dir_entry) = dir_entry_result {
                    let path = dir_entry.path();
                    let path = util::fs::path_relative_to_dir(path, &repo_path).unwrap();

                    let entry = if path.is_file() {
                        // If we can't hash - nothing downstream will work, so panic!
                        let (hash, num_bytes) = util::hasher::get_hash_and_size(&path)
                            .unwrap_or_else(|_| panic!("Could not hash file: {:?}", path));
                        let data_type = util::fs::file_data_type(&path);
                        // println!("path {:?} hash {} num_bytes {} data_type {:?}", path, hash, num_bytes, data_type);

                        // Take first 2 chars of hash as dir prefix and last N chars as the dir suffix
                        let dir_prefix_len = 2;
                        let dir_name = format!("{:x}", hash);
                        let dir_prefix = dir_name.chars().take(dir_prefix_len).collect::<String>();
                        let dir_suffix = dir_name.chars().skip(dir_prefix_len).collect::<String>();
                        let dst_dir = versions_path.join(dir_prefix).join(dir_suffix);

                        if !dst_dir.exists() {
                            util::fs::create_dir_all(&dst_dir).unwrap();
                        }

                        let dst = dst_dir.join("data");
                        util::fs::copy(&path, &dst).unwrap();

                        EntryMetaData {
                            hash,
                            data_type,
                            num_bytes,
                        }
                    } else {
                        EntryMetaData {
                            data_type: EntryDataType::Dir,
                            ..Default::default()
                        }
                    };

                    if path != Path::new("") {
                        let mut buf = Vec::new();
                        entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
                        staged_db.put(path.to_str().unwrap(), &buf).unwrap();
                    }
                    dir_entry.client_state = entry;
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
        cumulative_stats
            .data_type_counts
            .entry(dir_entry.client_state.data_type)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        progress_1.set_message(format!(
            "🐂 Added {} files {}",
            cumulative_stats.total_files,
            bytesize::ByteSize::b(cumulative_stats.total_bytes)
        ));

        cumulative_stats.total_files += 1;
    }

    Ok(cumulative_stats)
}
