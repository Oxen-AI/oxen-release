//! Computes metadata we can extract from the entry files

use indicatif::ProgressBar;
use rayon::prelude::*;

use crate::api;
use crate::constants::CACHE_DIR;
use crate::constants::{HISTORY_DIR, STATS_DIR};
use crate::core::index::{CommitDirEntryReader, CommitEntryReader};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;

pub fn compute(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::debug!("Running content_metadata");

    log::debug!("computing metadata {} -> {}", commit.id, commit.message);
    let commit_entry_reader = CommitEntryReader::new(repo, commit)?;

    // We're going to compute types per directory, and save them into a dataframe
    let dirs = commit_entry_reader.list_dirs()?;

    let db_path = util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join(STATS_DIR);

    log::debug!("Creating db path {:?}", db_path);

    let mut db = duckdb::Connection::open(db_path)?;
    db.execute_batch(
        r"CREATE TABLE IF NOT EXISTS metadata (
            id INTEGER NOT NULL, -- primary key,
            hash VARCHAR NOT NULL,
            directory VARCHAR NOT NULL,
            filename VARCHAR NOT NULL,
            path VARCHAR NOT NULL,
            num_bytes UINTEGER NOT NULL,
            data_type VARCHAR NOT NULL,
            mime_type VARCHAR NOT NULL,
            extension VARCHAR NOT NULL,
        );",
    )?;

    let mut tx = db.transaction()?;
    tx.set_drop_behavior(duckdb::DropBehavior::Commit);
    let mut appender = tx.appender("metadata")?;

    // Create DataFrames per directory of path, data_type, and mime_type
    for dir in dirs {
        let dir_entry_reader = CommitDirEntryReader::new(repo, &commit.id, &dir)?;
        let entries = dir_entry_reader.list_entries()?;

        let bar = ProgressBar::new(entries.len() as u64);

        // Compute in parallel
        let params = entries
            .par_iter()
            .map(|entry| {
                let path = util::fs::version_path(&repo, &entry);
                let mime_type = util::fs::file_mime_type(&path);
                let data_type = util::fs::datatype_from_mimetype(&path, &mime_type);

                let m = api::local::metadata::compute_metadata(&path).unwrap();
                let filename = entry.path.file_name().unwrap().to_str().unwrap();
                let entry_path = entry.path.to_string_lossy();
                let extension = util::fs::file_extension(&path);
                let data_type = format!("{:?}", data_type);

                bar.inc(1);

                (
                    entry.hash.clone(),
                    dir.to_string_lossy(),
                    filename.to_string(),
                    entry_path.to_string(),
                    m.size,
                    data_type,
                    mime_type,
                    extension,
                )
            })
            .collect::<Vec<_>>();
        bar.finish();

        // Write to DB in sequence, since we're using a transaction and duckdb doesn't support concurrent writes
        let bar = ProgressBar::new(entries.len() as u64);
        params.iter().enumerate().for_each(|(i, param)| {
            match appender.append_row(duckdb::params![
                i, &param.0, &param.1, &param.2, &param.3, &param.4, &param.5, &param.6, &param.7,
            ]) {
                Ok(_) => {}
                Err(e) => {
                    log::error!("Error appending row: {:?}", e);
                }
            }

            bar.inc(1);
        });
        bar.finish();
        log::debug!("Flushing appender....");
        appender.flush();
    }

    Ok(())
}
