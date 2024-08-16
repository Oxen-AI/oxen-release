use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::core::v0_19_0::structs::{EntryMetaData, EntryMetaDataWithPath};
use crate::error::OxenError;
use crate::model::{
    EntryDataType, LocalRepository, StagedData, StagedDirStats, StagedEntry, StagedEntryStatus,
    SummarizedStagedDirStats,
};
use crate::util;

use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{DBWithThreadMode, IteratorMode, SingleThreaded};
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::time::Duration;

pub fn status(repo: &LocalRepository) -> Result<StagedData, OxenError> {
    let mut staged_data = StagedData::empty();
    // Read the staged files from the staged db
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_path), true)?;

    let read_progress = ProgressBar::new_spinner();
    read_progress.set_style(ProgressStyle::default_spinner());
    read_progress.enable_steady_tick(Duration::from_millis(100));

    let (dir_entries, total_entries) = read_staged_entries(&db, &read_progress)?;
    println!("Considering {} total entries", total_entries);

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
                        hash: format!("{:x}", entry.hash),
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

pub fn status_from_dir(
    repo: &LocalRepository,
    dir: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    todo!()
}

pub fn read_staged_entries(
    db: &DBWithThreadMode<SingleThreaded>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<EntryMetaDataWithPath>>, u64), OxenError> {
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
                    status: StagedEntryStatus::Added,
                    last_commit_id: 0,
                };

                if let Some(parent) = path.parent() {
                    dir_entries
                        .entry(parent.to_path_buf())
                        .or_default()
                        .push(entry_w_path);
                }

                total_entries += 1;
                read_progress.set_message(format!("Gathering {} entries to commit", total_entries));
            }
            Err(err) => {
                log::error!("Could not get staged entry: {}", err);
            }
        }
    }

    Ok((dir_entries, total_entries))
}
