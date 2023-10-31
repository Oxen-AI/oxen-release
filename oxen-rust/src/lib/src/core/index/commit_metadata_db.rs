//! Database for commit metadata entries.
//!

use indicatif::ProgressBar;
use polars::lazy::dsl::sum;
use polars::prelude::col;
use polars::prelude::DataFrame;
use polars::prelude::IntoLazy;
use rayon::prelude::*;
use sql_query_builder as sql;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::constants::{CACHE_DIR, HISTORY_DIR};
use crate::core::db::df_db;
use crate::error::OxenError;
use crate::model::metadata::to_duckdb_sql::ToDuckDBSql;
use crate::model::{Commit, DirMetadataItem, LocalRepository};
use crate::util;

use super::{CommitEntryReader, CommitReader};

pub fn db_path(
    repo: &LocalRepository,
    commit: &Commit,
    // directory: impl AsRef<Path>
) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join("metadata")
        // .join(directory)
        .join("metadata.duckdb")
}

pub fn index_commit(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    // TODO: do we add the directories themselves as entries in the parent db with is_dir?
    //       it would make pagination easier potentially, do we get any other benefits?

    let entry_reader = CommitEntryReader::new(repo, commit)?;
    let entries = entry_reader.list_entries()?;

    let commit_reader = CommitReader::new(repo)?;
    let num_entries = entries.len();
    let bar = ProgressBar::new(entries.len() as u64);

    log::debug!("compute metadata for {num_entries} entries in commit: {commit:?}");

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
    let path = db_path(repo, commit);
    // Remove db if it is exists, since we might be recomputing
    if path.exists() {
        util::fs::remove_file(&path)?;
    }
    let mut conn = df_db::get_connection(path)?;
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
    let mut dirs = CommitEntryReader::new(repo, commit)?.list_dir_children(directory)?;
    dirs.push(directory.to_path_buf());

    // make sure they are uniq
    let dirs: HashSet<&PathBuf> = HashSet::from_iter(dirs.iter());

    if dirs.is_empty() {
        return Err(OxenError::path_does_not_exist(directory));
    }

    let conn = df_db::get_connection(db_path(repo, commit))?;

    let s = DirMetadataItem::schema();
    let column = column.as_ref();

    let mut combined_df: Option<DataFrame> = None;
    if dirs.is_empty() {
        return Ok(DataFrame::default());
    }

    let table_name = s.name.unwrap();
    log::debug!("aggregating dirs {:?}", dirs);
    for dir in dirs {
        let stmt = sql::Select::new()
            .select(&format!("{column}, count(*) AS count"))
            .where_clause(&format!("directory = '{}'", dir.to_string_lossy()))
            .group_by(column)
            .from(&table_name);

        let df = df_db::select(&conn, &stmt)?;
        // log::debug!("df for dir {:?}: {:?}", dir, df);

        if df.is_empty() {
            continue;
        }

        // have to make sure the order is correct coming out of this query...
        let df = df
            .lazy()
            .select(&[col(column), col("count")])
            .collect()
            .unwrap();

        // log::debug!("SORTED df for dir {:?}: {:?}", dir, df);

        if let Some(cdf) = combined_df {
            // log::debug!("START for dir {:?}: {:?}", dir, cdf);

            let stacked = cdf.vstack(&df).unwrap();

            // log::debug!("STACKED for dir {:?}: {:?}", dir, stacked);

            let aggregated = stacked
                .lazy()
                .groupby([column])
                .agg([sum("count")])
                .select(&[col(column), col("count")])
                .sort(column, Default::default())
                .collect()
                .unwrap();
            combined_df = Some(aggregated);
        } else {
            combined_df = Some(df);
        }

        // log::debug!("AGGREGATED df {:?}", combined_df);
        // log::debug!("\n--------------DONE-----------------\n");
    }

    // Make sure we don't unwrap an empty default
    if combined_df.is_none() {
        return Ok(DataFrame::default());
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
    let conn = df_db::get_connection(db_path(repo, commit))?;
    let s = DirMetadataItem::schema();
    let table_name = s.name.unwrap();
    let fields: Vec<String> = s.fields.iter().map(|f| f.name.to_owned()).collect();

    let stmt = sql::Select::new()
        .select(&fields.join(", "))
        .where_clause(&format!("directory = '{}'", directory.to_string_lossy()))
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
    let directory = directory.as_ref();
    let mut dirs = CommitEntryReader::new(repo, commit)?.list_dir_children(directory)?;
    dirs.push(directory.to_path_buf());

    let s = DirMetadataItem::schema();
    let table_name = s.name.unwrap();
    let num_cols = s.fields.len();

    let conn = df_db::get_connection(db_path(repo, commit))?;

    let mut num_rows = 0;
    for dir in dirs {
        num_rows += df_db::count_where(
            &conn,
            &table_name,
            format!("directory = '{}'", dir.to_string_lossy()),
        )?;
    }

    Ok((num_rows, num_cols))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::core::index::commit_metadata_db;
    use crate::error::OxenError;
    use crate::test;
    use crate::{api, command, util};

    #[test]
    fn test_index_metadata_db() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commit = api::local::commits::head_commit(&repo)?;

            commit_metadata_db::index_commit(&repo, &commit)?;

            let offset = 0;
            let limit = 10;

            let directory = PathBuf::from("train");

            let df = commit_metadata_db::select(&repo, &commit, directory, offset, limit)?;

            println!("df:\n{:?}", df);

            Ok(())
        })
    }

    #[test]
    fn test_aggregate_metadata_db() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commit = api::local::commits::head_commit(&repo)?;

            commit_metadata_db::index_commit(&repo, &commit)?;

            let directory = PathBuf::from("");

            let df = commit_metadata_db::aggregate_col(&repo, &commit, directory, "data_type")?;

            let df_str = format!("{:?}", df);

            // Add assert here
            assert_eq!(
                df_str,
                r"shape: (3, 2)
┌───────────┬───────┐
│ data_type ┆ count │
│ ---       ┆ ---   │
│ str       ┆ i64   │
╞═══════════╪═══════╡
│ image     ┆ 7     │
│ tabular   ┆ 7     │
│ text      ┆ 4     │
└───────────┴───────┘"
            );

            Ok(())
        })
    }

    #[test]
    fn test_aggregate_metadata_db_just_top_level_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // write ten text files to dir
            let dir = repo.path.join("train");
            util::fs::create_dir_all(&dir)?;
            for i in 0..10 {
                let path = dir.join(format!("file_{}.txt", i));
                util::fs::write_to_path(&path, format!("hello world {}", i))?;
            }
            command::add(&repo, &dir)?;
            command::commit(&repo, "adding ten text files")?;

            let commit = api::local::commits::head_commit(&repo)?;

            commit_metadata_db::index_commit(&repo, &commit)?;

            let directory = PathBuf::from("");

            let df = commit_metadata_db::aggregate_col(&repo, &commit, directory, "data_type")?;

            let df_str = format!("{:?}", df);
            println!("df:\n{:?}", df_str);

            // Add assert here
            assert_eq!(
                df_str,
                r"shape: (1, 2)
┌───────────┬───────┐
│ data_type ┆ count │
│ ---       ┆ ---   │
│ str       ┆ i64   │
╞═══════════╪═══════╡
│ text      ┆ 10    │
└───────────┴───────┘"
            );

            Ok(())
        })
    }
}
