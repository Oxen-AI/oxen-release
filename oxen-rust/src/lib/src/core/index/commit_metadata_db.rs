//! Database for commit metadata entries.
//!
//! Metadata consists of the following fields:
//!
//! id: uint64
//! hash: str
//! directory: str
//! filename: str
//! path: str
//! num_bytes: uint64
//! commit_id: str
//! timestamp: str
//! data_type: str
//! mime_type: str
//! extension: str
//!

use indicatif::ProgressBar;
use polars::prelude::DataFrame;
use rayon::prelude::*;
use std::path::{PathBuf, Path};
use duckdb::types::ToSql;

use crate::api;
use crate::constants::{HISTORY_DIR, CACHE_DIR};
use crate::core::db::df_db;
use crate::error::OxenError;
use crate::model::schema::{DataType, Field};
use crate::model::{Commit, LocalRepository, Schema, CommitEntry};
use crate::util;

use super::{CommitEntryReader, CommitReader};

struct Metadata {
    hash: String,
    directory: String,
    filename: String,
    path: String,
    num_bytes: u64,
    commit_id: String,
    timestamp: String,
    data_type: String,
    mime_type: String,
    extension: String,
}

impl Metadata {
    pub fn from_entry(
        repo: &LocalRepository,
        entry: &CommitEntry,
        commit: &Commit,
        commit_reader: &CommitReader,
    ) -> Self {
        let path = util::fs::version_path(&repo, &entry);
        let mime_type = util::fs::file_mime_type(&path);
        let data_type = util::fs::datatype_from_mimetype(&path, &mime_type);

        let size = api::local::metadata::get_file_size(&path).unwrap();
        let dir = entry.path.parent().unwrap_or(Path::new("")).to_string_lossy();
        let filename = entry.path.file_name().unwrap().to_str().unwrap();
        let entry_path = entry.path.to_string_lossy();
        let extension = util::fs::file_extension(&path);
        let data_type = format!("{:?}", data_type);
        let commit_id = entry.commit_id.clone();
        let commit = commit_reader.get_commit_by_id(&commit_id).unwrap().unwrap();
        let timestamp = commit
            .timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

        Metadata {
            hash: entry.hash.clone(),
            directory: dir.to_string(),
            filename: filename.to_string(),
            path: entry_path.to_string(),
            num_bytes: size,
            commit_id: commit_id,
            timestamp: timestamp,
            data_type: data_type,
            mime_type: mime_type,
            extension: extension,
        }
    }

    pub fn to_sql(&self) -> Vec<&dyn ToSql> {
        vec![
            &self.hash,
            &self.directory,
            &self.filename,
            &self.path,
            &self.num_bytes,
            &self.commit_id,
            &self.timestamp,
            &self.data_type,
            &self.mime_type,
            &self.extension,
        ]
    }
}

pub fn db_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join("metadata.duckdb")
}

pub fn schema() -> Schema {
    let fields = vec![
        Field {
            name: "hash".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "directory".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "filename".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "path".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "num_bytes".to_string(),
            dtype: DataType::UInt64.as_str().to_string(),
        },
        Field {
            name: "commit_id".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "timestamp".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "data_type".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "mime_type".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "extension".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
    ];
    Schema::new("metadata", fields)
}

pub fn index_commit(
    repo: &LocalRepository,
    commit: &Commit
) -> Result<(), OxenError> {
    // Read the commit entries
    let commit_entry_reader = CommitEntryReader::new(repo, commit)?;    
    let entries = commit_entry_reader.list_entries()?;

    // Index into db
    index_entries(repo, commit, &entries)?;

    Ok(())
}

pub fn index_entries(
    repo: &LocalRepository,
    commit: &Commit,
    entries: &[CommitEntry],
) -> Result<(), OxenError> {
    let commit_reader = CommitReader::new(repo)?;
    let bar = ProgressBar::new(entries.len() as u64);

    // Compute the metadata in parallel
    let metas = entries
        .par_iter()
        .map(|entry| {
            // Takes some time to compute from_entry
            bar.inc(1);
            Metadata::from_entry(repo, entry, commit, &commit_reader)
        })
        .collect::<Vec<_>>();

    bar.finish();

    // Connect to db
    let mut conn = df_db::get_connection(db_path(repo, commit))?;
    let table_name = df_db::create_table_if_not_exists(&conn, &schema())?;

    // Create an appender transaction
    let mut tx = conn.transaction()?;
    tx.set_drop_behavior(duckdb::DropBehavior::Commit);
    let mut appender = tx.appender(&table_name)?;

    // Write to DB in sequence, since we're using a transaction and duckdb doesn't support concurrent writes
    let bar = ProgressBar::new(metas.len() as u64);
    metas.iter().enumerate().for_each(|(i, meta)| {
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

pub fn list(
    repo: &LocalRepository,
    commit: &Commit,
    offset: usize,
    limit: usize,
) -> Result<DataFrame, OxenError> {
    let conn = df_db::get_connection(db_path(repo, commit))?;
    let s = schema();
    let table_name = df_db::create_table_if_not_exists(&conn, &s)?;
    let fields: Vec<String> = s.fields.iter().map(|f| f.name.to_owned()).collect();
    let df = df_db::select(&conn, table_name, &fields, limit, offset)?;
    Ok(df)
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::core::index::commit_metadata_db::{self, index_commit};
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_index_db() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commit = api::local::commits::get_head_commit(&repo)?;

            index_commit(&repo, &commit)?;

            let schema = commit_metadata_db::schema();


            Ok(())
        })
    }
}
