use crate::df::{tabular, DFOpts};
use crate::error::OxenError;
use crate::index::{CommitDirEntryReader, CommitReader};
use crate::model::{Commit, CommitEntry, DataFrameDiff, LocalRepository, Schema};
use crate::{constants, util};

use colored::Colorize;
use difference::{Changeset, Difference};
use polars::export::ahash::HashMap;
use polars::prelude::DataFrame;
use polars::prelude::IntoLazy;
use std::path::Path;

use super::SchemaReader;

pub fn diff(
    repo: &LocalRepository,
    commit_id: Option<&str>,
    path: &Path,
) -> Result<String, OxenError> {
    match _commit_or_head(repo, commit_id)? {
        Some(commit) => _diff_commit(repo, &commit, path),
        None => Err(OxenError::commit_id_does_not_exist(commit_id.unwrap())),
    }
}

fn _commit_or_head(
    repo: &LocalRepository,
    commit_id: Option<&str>,
) -> Result<Option<Commit>, OxenError> {
    let commit_reader = CommitReader::new(repo)?;
    if let Some(commit_id) = commit_id {
        commit_reader.get_commit_by_id(commit_id)
    } else {
        Ok(Some(commit_reader.head_commit()?))
    }
}

// TODO: Change API to take two commits
fn _diff_commit(repo: &LocalRepository, commit: &Commit, path: &Path) -> Result<String, OxenError> {
    if let Some(parent) = path.parent() {
        let relative_parent = util::fs::path_relative_to_dir(parent, &repo.path)?;
        let commit_entry_reader = CommitDirEntryReader::new(repo, &commit.id, &relative_parent)?;
        let file_name = path.file_name().unwrap();
        if let Ok(Some(entry)) = commit_entry_reader.get_entry(file_name) {
            if util::fs::is_tabular(path) {
                let commit_reader = CommitReader::new(repo)?;
                let commits = commit_reader.history_from_head()?;

                let current_commit = commits.first().unwrap();

                return diff_tabular(repo, current_commit, &entry.path);
            } else if util::fs::is_utf8(path) {
                // TODO: Change API to take two commits
                return diff_utf8(repo, &entry);
            }
            Err(OxenError::basic_str(format!(
                "Diff not supported for file: {path:?}"
            )))
        } else {
            Err(OxenError::file_does_not_exist_in_commit(path, &commit.id))
        }
    } else {
        Err(OxenError::file_has_no_parent(path))
    }
}

pub fn diff_utf8(repo: &LocalRepository, entry: &CommitEntry) -> Result<String, OxenError> {
    let current_path = repo.path.join(&entry.path);
    let version_path = util::fs::version_path(repo, entry);

    let original = util::fs::read_from_path(&version_path)?;
    let modified = util::fs::read_from_path(&current_path)?;
    let Changeset { diffs, .. } = Changeset::new(&original, &modified, "\n");

    let mut outputs: Vec<String> = vec![];
    for diff in diffs {
        match diff {
            Difference::Same(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!(" {split}\n").normal().to_string());
                }
            }
            Difference::Add(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!("+{split}\n").green().to_string());
                }
            }
            Difference::Rem(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!("-{split}\n").red().to_string());
                }
            }
        }
    }

    Ok(outputs.join(""))
}

pub fn diff_tabular(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<String, OxenError> {
    let schema_reader = SchemaReader::new(repo, &commit.id)?;
    if let Some(schema) = schema_reader.get_schema_for_file(path)? {
        let diff = compute_dataframe_diff(repo, commit, &schema, path)?;

        let mut results: Vec<String> = vec![];
        if let Some(rows) = diff.added_rows {
            results.push(format!("Added Rows\n\n{rows}\n\n"));
        }

        if let Some(rows) = diff.removed_rows {
            results.push(format!("Removed Rows\n\n{rows}\n\n"));
        }

        if let Some(cols) = diff.added_cols {
            results.push(format!("Added Columns\n\n{cols}\n\n"));
        }

        if let Some(cols) = diff.removed_cols {
            results.push(format!("Removed Columns\n\n{cols}\n\n"));
        }

        Ok(results.join("\n"))
    } else {
        Err(OxenError::schema_does_not_exist_for_file(path))
    }
}

fn compute_dataframe_diff(
    repo: &LocalRepository,
    commit: &Commit,
    versioned_schema: &Schema,
    path: &Path,
) -> Result<DataFrameDiff, OxenError> {
    let commit_entry_reader = CommitDirEntryReader::new(repo, &commit.id, path.parent().unwrap())?;
    let filename = Path::new(path.file_name().unwrap().to_str().unwrap());
    if let Some(entry) = commit_entry_reader.get_entry(filename)? {
        // Read current DF and get schema
        let current_path = repo.path.join(path);
        let versioned_path = util::fs::version_path(repo, &entry);
        let current_df = tabular::read_df(&current_path, DFOpts::empty())?;
        let current_schema = Schema::from_polars(&current_df.schema());

        // If schemas don't match, figure out which columns are different
        if versioned_schema.hash != current_schema.hash {
            compute_new_columns(
                &current_path,
                &versioned_path,
                &current_schema,
                versioned_schema,
            )
        } else {
            println!("Computing diff for {path:?}");
            // Schemas match, find added and removed rows
            // Read versioned df
            let versioned_df = tabular::read_df(&versioned_path, DFOpts::empty())?;
            compute_new_rows(current_df, versioned_df, versioned_schema)
        }
    } else {
        Err(OxenError::file_does_not_exist(path))
    }
}

fn compute_new_rows(
    current_df: DataFrame,
    versioned_df: DataFrame,
    versioned_schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    // Hash the rows
    let versioned_df = tabular::df_hash_rows(versioned_df)?;
    let current_df = tabular::df_hash_rows(current_df)?;

    // log::debug!("diff_current got current hashes {}", current_df);

    let current_hash_indices: HashMap<String, u32> = current_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    let versioned_hash_indices: HashMap<String, u32> = versioned_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    // Added is all the row hashes that are in current that are not in other
    let mut added_indices: Vec<u32> = current_hash_indices
        .iter()
        .filter(|(hash, _indices)| !versioned_hash_indices.contains_key(*hash))
        .map(|(_hash, index_pair)| *index_pair)
        .collect();
    added_indices.sort(); // so is deterministic and returned in correct order

    // Removed is all the row hashes that are in other that are not in current
    let mut removed_indices: Vec<u32> = versioned_hash_indices
        .iter()
        .filter(|(hash, _indices)| !current_hash_indices.contains_key(*hash))
        .map(|(_hash, index_pair)| *index_pair)
        .collect();
    removed_indices.sort(); // so is deterministic and returned in correct order

    // log::debug!("diff_current added_indices {:?}", added_indices);

    // log::debug!("diff_current removed_indices {:?}", removed_indices);

    // Take added from the current df
    let opts = DFOpts::from_schema_columns(versioned_schema);
    let current_df = tabular::transform_df(current_df.lazy(), opts)?;
    let added_rows = tabular::take(current_df.lazy(), added_indices)?;

    // Take removed from versioned df
    let opts = DFOpts::from_schema_columns(versioned_schema);
    let versioned_df = tabular::transform_df(versioned_df.lazy(), opts)?;
    let removed_rows = tabular::take(versioned_df.lazy(), removed_indices)?;

    Ok(DataFrameDiff {
        added_rows: if added_rows.height() > 0 {
            Some(added_rows)
        } else {
            None
        },
        removed_rows: if removed_rows.height() > 0 {
            Some(removed_rows)
        } else {
            None
        },
        added_cols: None,
        removed_cols: None,
    })
}

fn compute_new_columns(
    current_path: &Path,
    versioned_path: &Path,
    current_schema: &Schema,
    versioned_schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    let added_fields = current_schema.added_fields(versioned_schema);
    let removed_fields = current_schema.removed_fields(versioned_schema);

    let added_cols = if !added_fields.is_empty() {
        let opts = DFOpts::from_columns(added_fields);
        let df_added = tabular::read_df(current_path, opts)?;
        log::debug!("Got added col df: {}", df_added);
        if df_added.width() > 0 {
            Some(df_added)
        } else {
            None
        }
    } else {
        None
    };

    let removed_cols = if !removed_fields.is_empty() {
        let opts = DFOpts::from_columns(removed_fields);
        let df_removed = tabular::read_df(versioned_path, opts)?;
        log::debug!("Got removed col df: {}", df_removed);
        if df_removed.width() > 0 {
            Some(df_removed)
        } else {
            None
        }
    } else {
        None
    };

    Ok(DataFrameDiff {
        added_rows: None,
        removed_rows: None,
        added_cols,
        removed_cols,
    })
}
