use glob::glob;
use jwalk::WalkDirGeneric;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tokio::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rmp_serde::Serializer;
use serde::Serialize;

use crate::constants::{STAGED_DIR, VERSIONS_DIR};
use crate::core::db;
use crate::core::v0_19_0::structs::EntryMetaData;
use crate::model::EntryDataType;
use crate::util;
use crate::{error::OxenError, model::LocalRepository};
use std::ops::AddAssign;

#[derive(Clone, Debug)]
pub struct CumulativeStats {
    total_files: usize,
    total_bytes: u64,
    data_type_counts: HashMap<EntryDataType, usize>,
}

impl Default for CumulativeStats {
    fn default() -> Self {
        CumulativeStats {
            total_files: 0,
            total_bytes: 0,
            data_type_counts: HashMap::new(),
        }
    }
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
        humantime::format_duration(duration).to_string()
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

    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };
    for path in paths {
        if path.is_dir() {
            total += process_dir(
                repo, path,
                // &progress_1,
                // &progress_2
            )?;
        } else if path.is_file() {
            // Process the file here
            let entry = add_file(repo, path)?;
            total.total_files += 1;
            total.total_bytes += entry.num_bytes;
            total
                .data_type_counts
                .entry(entry.data_type)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    }

    Ok(total)
}

fn process_dir(
    repo: &LocalRepository,
    path: &Path,
    // progress_1: &ProgressBar,
    // progress_2: &ProgressBar,
) -> Result<CumulativeStats, OxenError> {
    let start = std::time::Instant::now();

    let m = MultiProgress::new();
    let progress_1 = m.add(ProgressBar::new_spinner());
    progress_1.set_style(ProgressStyle::default_spinner());
    progress_1.enable_steady_tick(Duration::from_millis(100));
    // let progress_2 = m.add(ProgressBar::new_spinner());
    // progress_2.set_style(ProgressStyle::default_spinner());
    // progress_2.enable_steady_tick(Duration::from_millis(100));

    let repo_path = repo.path.clone();
    let versions_path = util::fs::oxen_hidden_dir(&repo.path).join(VERSIONS_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    let byte_counter = Arc::new(AtomicU64::new(0));
    let file_counter = Arc::new(AtomicU64::new(0));

    let walk_dir = WalkDirGeneric::<(usize, EntryMetaData)>::new(path)
        .parallelism(jwalk::Parallelism::RayonNewPool(num_cpus::get()))
        .process_read_dir(move |_depth, dir, _state, children| {
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
            let byte_counter_clone = Arc::clone(&byte_counter);
            let file_counter_clone = Arc::clone(&file_counter);

            let num_children = children.len();
            progress_1.set_message(format!(
                "Processing dir [{:?}] with {} entries",
                dir, num_children
            ));
            children.par_iter_mut().for_each(|dir_entry_result| {
                if let Ok(dir_entry) = dir_entry_result {
                    let total_bytes = byte_counter_clone.load(Ordering::Relaxed);
                    let path = dir_entry.path();
                    let duration = start.elapsed().as_secs_f32();
                    let mbps = (total_bytes as f32 / duration) / 1_000_000.0;

                    progress_1.set_message(format!(
                        "🐂 Added {} files ({}) {:.2} MB/s",
                        file_counter_clone.load(Ordering::Relaxed),
                        bytesize::ByteSize::b(total_bytes),
                        mbps
                    ));
                    match process_add_file(&repo_path, &versions_path, &staged_db, &path) {
                        Ok(entry) => {
                            byte_counter_clone.fetch_add(entry.num_bytes, Ordering::Relaxed);
                            file_counter_clone.fetch_add(1, Ordering::Relaxed);

                            dir_entry.client_state = entry;
                        }
                        Err(e) => {
                            log::error!("Error adding file: {:?}", e);
                        }
                    }
                }
            });
        });

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
        // progress_2.set_message(format!(
        //     "🐂 Added {} files {}",
        //     cumulative_stats.total_files,
        //     bytesize::ByteSize::b(cumulative_stats.total_bytes)
        // ));

        cumulative_stats.total_files += 1;
    }

    Ok(cumulative_stats)
}

fn add_file(repo: &LocalRepository, path: &Path) -> Result<EntryMetaData, OxenError> {
    let repo_path = repo.path.clone();
    let versions_path = util::fs::oxen_hidden_dir(&repo.path).join(VERSIONS_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    process_add_file(&repo_path, &versions_path, &staged_db, path)
}

fn process_add_file(
    repo_path: &Path,
    versions_path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    path: &Path,
) -> Result<EntryMetaData, OxenError> {
    let relative_path = util::fs::path_relative_to_dir(path, &repo_path).unwrap();
    let full_path = repo_path.join(&relative_path);
    let entry = if full_path.is_file() {
        // If we can't hash - nothing downstream will work, so panic!
        let (hash, num_bytes) = util::hasher::get_hash_and_size(&full_path)
            .unwrap_or_else(|_| panic!("Could not hash file: {:?}", full_path));
        let data_type = util::fs::file_data_type(&full_path);
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
        util::fs::copy(&full_path, &dst).unwrap();

        let entry = EntryMetaData {
            hash,
            data_type,
            num_bytes,
        };

        let mut buf = Vec::new();
        entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db
            .put(relative_path.to_str().unwrap(), &buf)
            .unwrap();

        // Add all the parent dirs to the staged db
        let mut parent_path = relative_path.to_path_buf();
        while let Some(parent) = parent_path.parent() {
            let relative_path = util::fs::path_relative_to_dir(&parent, &repo_path).unwrap();

            let dir_entry = EntryMetaData {
                data_type: EntryDataType::Dir,
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

        entry
    } else {
        let entry = EntryMetaData {
            data_type: EntryDataType::Dir,
            ..Default::default()
        };

        entry
    };

    Ok(entry)
}
