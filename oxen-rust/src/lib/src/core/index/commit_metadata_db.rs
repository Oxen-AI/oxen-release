//! Database for commit metadata entries.
//!

use indicatif::ProgressBar;
use polars::lazy::dsl::sum;
use polars::prelude::col;
use polars::prelude::DataFrame;
use polars::prelude::IntoLazy;
use rayon::prelude::*;
use sql_query_builder as sql;
use std::path::{Path, PathBuf};

use crate::constants::{CACHE_DIR, HISTORY_DIR};
use crate::core::db::df_db;
use crate::core::index::CommitDirEntryReader;
use crate::error::OxenError;
use crate::model::metadata::to_duckdb_sql::ToDuckDBSql;
use crate::model::{Commit, DirMetadataItem, LocalRepository};
use crate::util;

use super::{CommitEntryReader, CommitReader};

pub fn db_path(repo: &LocalRepository, commit: &Commit, directory: impl AsRef<Path>) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join("metadata")
        .join(directory)
        .join("metadata.duckdb")
}

pub fn index_commit(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    // Read the commit entries
    let commit_entry_reader = CommitEntryReader::new(repo, commit)?;
    let dirs = commit_entry_reader.list_dirs()?;

    // Index into db
    for dir in dirs {
        index_directory(repo, commit, &dir)?;
    }

    Ok(())
}

pub fn index_directory(
    repo: &LocalRepository,
    commit: &Commit,
    directory: impl AsRef<Path>,
) -> Result<(), OxenError> {
    // TODO: do we add the directories themselves as entries in the parent db with is_dir?
    //       it would make pagination easier potentially, do we get any other benefits?

    let directory = directory.as_ref();
    let entry_reader = CommitDirEntryReader::new(repo, &commit.id, directory)?;
    let entries = entry_reader.list_entries()?;

    let commit_reader = CommitReader::new(repo)?;
    let num_entries = entries.len();
    let bar = ProgressBar::new(entries.len() as u64);

    log::debug!("compute metadata for {num_entries} entries for directory {directory:?} in commit: {commit:?}");

    // Compute the metadata in parallel
    let metas = entries
        .par_iter()
        .map(|entry| {
            // Takes some time to compute from_entry
            bar.inc(1);
            DirMetadataItem::from_entry(repo, entry, &commit_reader)
        })
        .collect::<Vec<_>>();

    bar.finish();

    log::debug!(
        "done compute metadata for {} entries in commit: {} -> '{}'",
        entries.len(),
        commit.id,
        commit.message
    );

    // Connect to db
    let mut conn = df_db::get_connection(db_path(repo, commit, directory))?;
    let table_name = df_db::create_table_if_not_exists(&conn, &DirMetadataItem::schema())?;

    // Create an appender transaction
    let mut tx = conn.transaction()?;
    tx.set_drop_behavior(duckdb::DropBehavior::Commit);
    let mut appender = tx.appender(&table_name)?;

    // Write to DB in sequence, since we're using a transaction and duckdb doesn't support concurrent writes
    let bar = ProgressBar::new(metas.len() as u64);
    metas.iter().for_each(|meta| {
        // TODO: Make this a more generic trait for structs to implement
        let params = meta.to_sql();
        let sql_params = params.as_slice();
        match appender.append_row(sql_params) {
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

    Ok(())
}

/// Aggregate up column from all children directories
pub fn aggregate_col(
    repo: &LocalRepository,
    commit: &Commit,
    directory: impl AsRef<Path>,
    column: impl AsRef<str>,
) -> Result<DataFrame, OxenError> {
    let directory = directory.as_ref();
    let dirs = CommitEntryReader::new(repo, commit)?.list_dir_children(directory)?;
    if dirs.is_empty() {
        return Err(OxenError::path_does_not_exist(directory));
    }

    let s = DirMetadataItem::schema();
    let column = column.as_ref();

    let mut combined_df: Option<DataFrame> = None;
    let table_name = s.name.unwrap();
    for dir in dirs {
        log::debug!("\n--------------PROC DIR {dir:?}-----------------\n");

        let conn = df_db::get_connection(db_path(repo, commit, &dir))?;

        let stmt = sql::Select::new()
            .select(&format!("{column}, count(*) AS count"))
            .group_by(column)
            .from(&table_name);

        let df = df_db::select(&conn, &stmt)?;
        log::debug!("df for dir {:?}: {:?}", dir, df);

        if df.is_empty() {
            continue;
        }

        // have to make sure the order is correct coming out of this query...
        let df = df
            .lazy()
            .select(&[col(&column), col("count")])
            .collect()
            .unwrap();

        log::debug!("SORTED df for dir {:?}: {:?}", dir, df);

        if let Some(cdf) = combined_df {
            log::debug!("START for dir {:?}: {:?}", dir, cdf);

            let stacked = cdf.vstack(&df).unwrap();

            log::debug!("STACKED for dir {:?}: {:?}", dir, stacked);

            let aggregated = stacked
                .lazy()
                .groupby([column])
                .agg([sum("count")])
                .select(&[col(&column), col("count")])
                .collect()
                .unwrap();
            combined_df = Some(aggregated);
        } else {
            combined_df = Some(df);
        }

        log::debug!("AGGREGATED df {:?}", combined_df);
        log::debug!("\n--------------DONE-----------------\n");
    }

    Ok(combined_df.unwrap())
}

/// Select entries from a directory
pub fn select(
    repo: &LocalRepository,
    commit: &Commit,
    directory: impl AsRef<Path>,
    offset: usize,
    limit: usize,
) -> Result<DataFrame, OxenError> {
    let directory = directory.as_ref();
    let conn = df_db::get_connection(db_path(repo, commit, directory))?;
    let s = DirMetadataItem::schema();
    let table_name = s.name.unwrap();
    let fields: Vec<String> = s.fields.iter().map(|f| f.name.to_owned()).collect();

    let stmt = sql::Select::new()
        .select(&fields.join(", "))
        .offset(&offset.to_string())
        .limit(&limit.to_string())
        .from(&table_name);

    let df = df_db::select(&conn, &stmt)?;
    Ok(df)
}

/// Recursively compute the full number of entries in a directory
pub fn full_size(
    repo: &LocalRepository,
    commit: &Commit,
    directory: impl AsRef<Path>,
) -> Result<(usize, usize), OxenError> {
    let dirs = CommitEntryReader::new(repo, commit)?.list_dir_children(directory)?;
    let s = DirMetadataItem::schema();
    let table_name = s.name.unwrap();
    let num_cols = s.fields.len();

    let mut num_rows = 0;
    for dir in dirs {
        let conn = df_db::get_connection(db_path(repo, commit, dir))?;
        num_rows += df_db::count(&conn, &table_name)?;
    }

    Ok((num_rows, num_cols))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::api;
    use crate::core::index::commit_metadata_db;
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_index_metadata_db() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commit = api::local::commits::get_head_commit(&repo)?;

            commit_metadata_db::index_commit(&repo, &commit)?;

            let offset = 0;
            let limit = 10;

            let directory = PathBuf::from("train");

            let df = commit_metadata_db::select(&repo, &commit, directory, offset, limit)?;

            println!("df:\n{:?}", df);

            assert!(false);

            Ok(())
        })
    }

    #[test]
    fn test_aggregate_metadata_db() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commit = api::local::commits::get_head_commit(&repo)?;

            commit_metadata_db::index_commit(&repo, &commit)?;

            let directory = PathBuf::from("");

            let df = commit_metadata_db::aggregate_col(&repo, &commit, directory, "data_type")?;

            println!("df:\n{:?}", df);

            assert!(false);

            Ok(())
        })
    }
}
