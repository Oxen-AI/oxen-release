use duckdb::Connection;
use polars::frame::DataFrame;

use sql_query_builder::{Delete, Select};

use crate::api;
use crate::constants::MODS_DIR;
use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_COLS, TABLE_NAME};
use crate::core::db::workspace_df_db::select_cols_from_schema;
use crate::core::db::{df_db, workspace_df_db};
use crate::core::df::tabular;
use crate::core::index::CommitEntryReader;
use crate::core::index::{self, workspaces};
use crate::model::diff::tabular_diff::{
    TabularDiffDupes, TabularDiffMods, TabularDiffParameters, TabularDiffSchemas,
    TabularDiffSummary, TabularSchemaDiff,
};
use crate::model::diff::{AddRemoveModifyCounts, DiffResult, TabularDiff};

use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{Commit, CommitEntry, LocalRepository, Workspace};
use crate::opts::DFOpts;
use crate::{error::OxenError, util};
use std::path::{Path, PathBuf};

pub mod rows;

pub fn is_behind(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    let commit_path = previous_commit_ref_path(workspace, path);
    let commit_id = util::fs::read_from_path(commit_path)?;
    Ok(commit_id != workspace.commit.id)
}

pub fn previous_commit_ref_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("COMMIT_ID")
}

// used to be duckdb_path
pub fn duckdb_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("db")
}

pub fn count(workspace: &Workspace, path: impl AsRef<Path>) -> Result<usize, OxenError> {
    let db_path = duckdb_path(workspace, path);
    let conn = df_db::get_connection(db_path)?;

    let count = df_db::count(&conn, TABLE_NAME)?;
    Ok(count)
}

pub fn get_queryable_data_frame_workspace(
    repo: &LocalRepository,
    path: &PathBuf,
    commit: &Commit,
) -> Result<Workspace, OxenError> {
    if !util::fs::is_tabular(path) {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.",
        ));
    }

    let workspaces = index::workspaces::list(repo)?;

    for workspace in workspaces {
        // Ensure the workspace is not editable and matches the commit ID of the resource
        if !workspace.is_editable && workspace.commit == *commit {
            // Construct the path to the DuckDB resource within the workspace
            let workspace_file_db_path =
                index::workspaces::data_frames::duckdb_path(&workspace, path);

            // Check if the DuckDB file exists in the workspace's directory
            if workspace_file_db_path.exists() {
                // The file exists in this non-editable workspace, and the commit IDs match
                return Ok(workspace);
            }
        }
    }

    return Err(OxenError::QueryableWorkspaceNotFound());
}

pub fn is_queryable_data_frame_indexed(
    repo: &LocalRepository,
    path: &PathBuf,
    commit: &Commit,
) -> Result<bool, OxenError> {
    match index::workspaces::data_frames::get_queryable_data_frame_workspace(repo, path, commit) {
        Ok(_workspace) => Ok(true),
        Err(e) => match e {
            OxenError::QueryableWorkspaceNotFound() => Ok(false),
            _ => Err(e),
        },
    }
}

pub fn index(workspace: &Workspace, path: &Path) -> Result<(), OxenError> {
    // Is tabular just looks at the file extensions
    if !util::fs::is_tabular(path) {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.must be tabular.",
        ));
    }

    let repo = &workspace.base_repo;
    let commit = &workspace.commit;
    let reader = CommitEntryReader::new(repo, commit)?;
    let entry = reader.get_entry(path)?;
    let entry = match entry {
        Some(entry) => entry,
        None => return Err(OxenError::resource_not_found(path.to_string_lossy())),
    };

    let db_path = duckdb_path(workspace, &entry.path);

    let Some(parent) = db_path.parent() else {
        return Err(OxenError::basic_str(format!(
            "Failed to get parent directory for {:?}",
            db_path
        )));
    };

    if !parent.exists() {
        util::fs::create_dir_all(parent)?;
    }

    let conn = df_db::get_connection(db_path)?;
    if df_db::table_exists(&conn, TABLE_NAME)? {
        df_db::drop_table(&conn, TABLE_NAME)?;
    }
    let version_path = util::fs::version_path(repo, &entry);

    log::debug!(
        "core::index::workspaces::data_frames::index({:?}) got version path: {:?}",
        entry.path,
        version_path
    );

    df_db::index_file_with_id(&version_path, &conn)?;
    log::debug!(
        "core::index::workspaces::data_frames::index({:?}) finished!",
        entry.path
    );

    add_row_status_cols(&conn)?;

    // Save the current commit id so we know if the branch has advanced
    let commit_path = previous_commit_ref_path(workspace, path);
    util::fs::write_to_path(commit_path, &commit.id)?;

    Ok(())
}

pub fn query(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let db_path = duckdb_path(workspace, path);
    log::debug!("query_staged_df() got db_path: {:?}", db_path);

    let conn = df_db::get_connection(db_path)?;

    // Get the schema of this commit entry
    let repo = &workspace.base_repo;
    let commit = &workspace.commit;
    let schema = api::local::schemas::get_by_path_from_ref(repo, &commit.id, path)?
        .ok_or_else(|| OxenError::resource_not_found(path.to_string_lossy()))?;

    // Enrich w/ oxen cols
    let full_schema = workspace_df_db::enhance_schema_with_oxen_cols(&schema)?;

    let col_names = select_cols_from_schema(&schema)?;

    let select = Select::new().select(&col_names).from(TABLE_NAME);

    let df = df_db::select(&conn, &select, true, Some(&full_schema), Some(opts))?;

    Ok(df)
}

fn add_row_status_cols(conn: &Connection) -> Result<(), OxenError> {
    let query_status = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" VARCHAR DEFAULT '{}'",
        TABLE_NAME,
        DIFF_STATUS_COL,
        StagedRowStatus::Unchanged
    );
    conn.execute(&query_status, [])?;

    let query_hash = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" VARCHAR DEFAULT NULL",
        TABLE_NAME, DIFF_HASH_COL
    );
    conn.execute(&query_hash, [])?;
    Ok(())
}

pub fn unindex(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let db_path = duckdb_path(workspace, path);
    let conn = df_db::get_connection(db_path)?;
    df_db::drop_table(&conn, TABLE_NAME)?;

    Ok(())
}

pub fn is_indexed(workspace: &Workspace, path: &Path) -> Result<bool, OxenError> {
    log::debug!("checking dataset is indexed for {:?}", path);
    let db_path = duckdb_path(workspace, path);
    log::debug!("getting conn at path {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let table_exists = df_db::table_exists(&conn, TABLE_NAME)?;
    log::debug!("dataset_is_indexed() got table_exists: {:?}", table_exists);
    Ok(table_exists)
}

pub fn diff(workspace: &Workspace, path: impl AsRef<Path>) -> Result<DiffResult, OxenError> {
    let repo = &workspace.base_repo;
    let commit = &workspace.commit;
    let path = path.as_ref();
    // Get commit for the branch head
    log::debug!("diff_workspace_df got repo at path {:?}", repo.path);

    let entry = api::local::entries::get_commit_entry(repo, commit, path)?
        .ok_or(OxenError::entry_does_not_exist(path))?;

    if !is_indexed(workspace, path)? {
        return Err(OxenError::basic_str("Dataset is not indexed"));
    };

    let db_path = duckdb_path(workspace, entry.path);

    let conn = df_db::get_connection(db_path)?;

    let diff_df = workspace_df_db::df_diff(&conn)?;

    if diff_df.is_empty() {
        return Ok(DiffResult::Tabular(TabularDiff::empty()));
    }

    let row_mods = AddRemoveModifyCounts::from_diff_df(&diff_df)?;

    let schema = workspace_df_db::schema_without_oxen_cols(&conn, TABLE_NAME)?;

    let schemas = TabularDiffSchemas {
        left: schema.clone(),
        right: schema.clone(),
        diff: schema.clone(),
    };

    let diff_summary = TabularDiffSummary {
        modifications: TabularDiffMods {
            row_counts: row_mods,
            col_changes: TabularSchemaDiff::empty(),
        },
        schemas,
        dupes: TabularDiffDupes::empty(),
    };

    let diff_result = TabularDiff {
        contents: diff_df,
        parameters: TabularDiffParameters::empty(),
        summary: diff_summary,
    };

    Ok(DiffResult::Tabular(diff_result))
}

pub fn extract_dataset_to_versions_dir(
    workspace: &Workspace,
    entry: &CommitEntry,
) -> Result<(), OxenError> {
    let repo = &workspace.base_repo;
    let version_path = util::fs::version_path(repo, entry);
    let db_path = duckdb_path(workspace, entry.path.clone());
    let conn = df_db::get_connection(db_path)?;

    log::debug!("extracting to versions path: {:?}", version_path);

    // Filter out any with removed status before extracting
    let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
        "\"{}\" = '{}'",
        DIFF_STATUS_COL,
        StagedRowStatus::Removed
    ));
    conn.execute(&delete.to_string(), [])?;

    let df_before = tabular::read_df(&version_path, DFOpts::empty())?;
    log::debug!(
        "extract_dataset_to_versions_dir() got df_before: {:?}",
        df_before
    );

    match entry.path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") => export_csv(&version_path, &conn)?,
            Some("tsv") => export_tsv(&version_path, &conn)?,
            Some("json") | Some("jsonl") | Some("ndjson") => export_rest(&version_path, &conn)?,
            Some("parquet") => export_parquet(&version_path, &conn)?,
            _ => {
                return Err(OxenError::basic_str(
                    "File format not supported, must be tabular.",
                ))
            }
        },
        None => {
            return Err(OxenError::basic_str(
                "File format not supported, must be tabular.",
            ))
        }
    }

    let df_after = tabular::read_df(&version_path, DFOpts::empty())?;
    log::debug!(
        "extract_dataset_to_versions_dir() got df_after: {:?}",
        df_after
    );

    Ok(())
}

// TODONOW combine with versions dir export fn and genericize on path
pub fn extract_to_working_dir(
    workspace: &Workspace,
    entry: &CommitEntry,
) -> Result<PathBuf, OxenError> {
    let workspace_repo = &workspace.workspace_repo;

    let working_path = workspace_repo.path.join(entry.path.clone());
    let db_path = duckdb_path(workspace, entry.path.clone());
    let conn = df_db::get_connection(db_path)?;
    // Match on the extension
    if !working_path.exists() {
        util::fs::create_dir_all(
            working_path
                .parent()
                .expect("Failed to get parent directory"),
        )?;
    }

    log::debug!("created working path: {:?}", working_path);

    let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
        "\"{}\" = '{}'",
        DIFF_STATUS_COL,
        StagedRowStatus::Removed
    ));
    let res = conn.execute(&delete.to_string(), [])?;
    log::debug!("delete query result is: {:?}", res);

    match entry.path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") => export_csv(&working_path, &conn)?,
            Some("tsv") => export_tsv(&working_path, &conn)?,
            Some("json") | Some("jsonl") | Some("ndjson") => export_rest(&working_path, &conn)?,
            Some("parquet") => export_parquet(&working_path, &conn)?,
            _ => {
                return Err(OxenError::basic_str(
                    "File format not supported, must be tabular.",
                ))
            }
        },
        None => {
            return Err(OxenError::basic_str(
                "File format not supported, must be tabular.",
            ))
        }
    }

    let df_after = tabular::read_df(&working_path, DFOpts::empty())?;
    log::debug!("extract_to_working_dir() got df_after: {:?}", df_after);

    Ok(working_path)
}

pub fn unstage(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    unindex(workspace, &path)?;
    workspaces::stager::rm(workspace, &path)?;

    Ok(())
}

pub fn restore(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    // Unstage and then restage the df
    unindex(workspace, &path)?;

    // TODO: we could do this more granularly without a full reset
    index(workspace, path.as_ref())?;

    Ok(())
}

fn export_rest(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_rest()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}';",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    // let temp_select_query = Select::new().select("*").from(TABLE_NAME);
    // let temp_res = df_db::select(conn, &temp_select_query)?;
    // log::debug!("export_rest() got df: {:?}", temp_res);

    conn.execute(&query, [])?;
    Ok(())
}

fn export_csv(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_csv()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (HEADER, DELIMITER ',');",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    // let temp_select_query = Select::new().select("*").from(TABLE_NAME);

    // let temp_res = df_db::select(conn, &temp_select_query)?;
    // log::debug!("export_csv() got df: {:?}", temp_res);

    conn.execute(&query, [])?;

    Ok(())
}

fn export_tsv(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_tsv()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (HEADER, DELIMITER '\t');",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    conn.execute(&query, [])?;
    Ok(())
}

fn export_parquet(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_parquet()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");

    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (FORMAT PARQUET);",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );
    conn.execute(&query, [])?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;

    use crate::api;
    use crate::command;
    use crate::config::UserConfig;
    use crate::constants::OXEN_ID_COL;
    use crate::core::index::workspaces;
    use crate::error::OxenError;
    use crate::model::diff::DiffResult;
    use crate::model::NewCommitBody;
    use crate::opts::DFOpts;
    use crate::test;

    #[test]
    fn test_add_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = workspaces::create(&repo, &commit, workspace_id)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index dataset
            workspaces::data_frames::index(&workspace, &file_path)?;

            // Append row
            let json_data = json!({
                "file": "dawg1.jpg",
                "label": "dog",
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            workspaces::data_frames::rows::add(&workspace, &file_path, &json_data)?;

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 1);

            let diff = workspaces::data_frames::diff(&workspace, file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_delete_added_row_with_two_rows() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = workspaces::create(&repo, &commit, workspace_id)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index dataset
            workspaces::data_frames::index(&workspace, &file_path)?;

            // Append row
            let json_data = json!({
                "file": "dawg1.jpg",
                "label": "dog",
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let append_entry_1 =
                workspaces::data_frames::rows::add(&workspace, &file_path, &json_data)?;

            let append_1_id = append_entry_1.column(OXEN_ID_COL)?.get(0)?.to_string();
            let append_1_id = append_1_id.replace('"', "");

            let json_data = json!({
                "file": "dawg2.jpg",
                "label": "dog",
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let _append_entry_2 =
                workspaces::data_frames::rows::add(&workspace, &file_path, &json_data)?;

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 1);

            // List the staged mods
            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Delete the first append
            workspaces::data_frames::rows::delete(&workspace, &file_path, &append_1_id)?;

            // Should only be one mod now
            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_clear_changes() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = workspaces::create(&repo, &commit, workspace_id)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index the dataset
            workspaces::data_frames::index(&workspace, &file_path)?;

            // Append the data to staging area
            let json_data = json!({
                "file": "dawg1.jpg",
                "label": "dog",
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let append_entry_1 =
                workspaces::data_frames::rows::add(&workspace, &file_path, &json_data)?;
            let append_1_id = append_entry_1.column(OXEN_ID_COL)?.get(0)?;
            let append_1_id = append_1_id.get_str().unwrap();
            log::debug!("added the row");

            let json_data = json!({
                "file": "dawg2.jpg",
                "label": "dog",
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let append_entry_2 =
                workspaces::data_frames::rows::add(&workspace, &file_path, &json_data)?;
            let append_2_id = append_entry_2.column(OXEN_ID_COL)?.get(0)?;
            let append_2_id = append_2_id.get_str().unwrap();

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 1);

            // List the staged mods
            let diff = workspaces::data_frames::diff(&workspace, file_path.clone())?;

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                }
                _ => panic!("Expected tabular diff result"),
            }
            // Delete the first append
            workspaces::data_frames::rows::delete(&workspace, &file_path, append_1_id)?;

            // Delete the second append
            workspaces::data_frames::rows::delete(&workspace, &file_path, append_2_id)?;

            // Should be zero staged files
            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 0);

            log::debug!("about to diff staged");
            // Should be zero mods left
            let diff = workspaces::data_frames::diff(&workspace, file_path.clone())?;
            log::debug!("got diff staged");

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }
            Ok(())
        })
    }

    #[test]
    fn test_delete_committed_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = workspaces::create(&repo, &commit, workspace_id)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Index the dataset
            workspaces::data_frames::index(&workspace, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;

            let id_to_delete = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Stage a deletion
            workspaces::data_frames::rows::delete(&workspace, &file_path, &id_to_delete)?;

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 1);

            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            let status = command::status(&repo)?;
            log::debug!("got this status {:?}", status);

            // Commit the new file

            let new_commit = NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: "Deleting a row allegedly".to_string(),
            };
            let commit_2 = workspaces::commit(&workspace, &new_commit, branch_name)?;

            let file_1 = api::local::revisions::get_version_file_from_commit_id(
                &repo, &commit.id, &file_path,
            )?;

            let file_2 = api::local::revisions::get_version_file_from_commit_id(
                &repo,
                commit_2.id,
                &file_path,
            )?;

            let diff_result = api::local::diff::diff_files(file_1, file_2, vec![], vec![], vec![])?;

            match diff_result {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_modify_added_row() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = workspaces::create(&repo, &commit, workspace_id)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&workspace, &file_path)?;

            // Add a row
            let json_data = json!({
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let new_row = workspaces::data_frames::rows::add(&workspace, &file_path, &json_data)?;

            // 1 row added
            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            let id_to_modify = new_row.column(OXEN_ID_COL)?.get(0)?;
            let id_to_modify = id_to_modify.get_str().unwrap();

            let json_data = json!({
                "height": 101
            });

            workspaces::data_frames::rows::update(
                &workspace,
                &file_path,
                id_to_modify,
                &json_data,
            )?;
            // List the files that are changed - this file should be back into unchanged state
            let commit_entries = workspaces::stager::list_files(&workspace)?;
            log::debug!("found mod entries: {:?}", commit_entries);
            assert_eq!(commit_entries.len(), 1);

            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(modified_rows, 0);
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_delete_added_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = workspaces::create(&repo, &commit, workspace_id)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&workspace, &file_path)?;

            // Add a row
            let json_data = json!({
                "min_x": 13,
                "min_y": 14,
                "width": 100,
                "height": 100
            });
            let new_row = workspaces::data_frames::rows::add(&workspace, &file_path, &json_data)?;

            // 1 row added
            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            let id_to_delete = new_row.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Stage a deletion
            workspaces::data_frames::rows::delete(&workspace, &file_path, &id_to_delete)?;
            log::debug!("done deleting row");
            // List the files that are changed - this file should be back into unchanged state
            let commit_entries = workspaces::stager::list_files(&workspace)?;
            log::debug!("found mod entries: {:?}", commit_entries);
            assert_eq!(commit_entries.len(), 0);

            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_modify_row_back_to_original_state() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = workspaces::create(&repo, &commit, workspace_id)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&workspace, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;

            let id_to_modify = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_modify = id_to_modify.replace('"', "");

            let json_data = json!({
                "label": "doggo"
            });

            // Stage a modification
            workspaces::data_frames::rows::update(
                &workspace,
                &file_path,
                &id_to_modify,
                &json_data,
            )?;

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 1);

            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    assert_eq!(modified_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Now modify the row back to its original state
            let json_data = json!({
                "label": "dog"
            });

            let res = workspaces::data_frames::rows::update(
                &workspace,
                &file_path,
                &id_to_modify,
                &json_data,
            )?;

            log::debug!("res is... {:?}", res);

            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 0);

            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    assert_eq!(modified_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }
    #[test]
    fn test_restore_row() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = workspaces::create(&repo, &commit, workspace_id)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            // Index the dataset
            workspaces::data_frames::index(&workspace, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;

            let id_to_modify = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_modify = id_to_modify.replace('"', "");

            let json_data = json!({
                "label": "doggo"
            });

            // Stage a modification
            workspaces::data_frames::rows::update(
                &workspace,
                &file_path,
                &id_to_modify,
                &json_data,
            )?;

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 1);

            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    assert_eq!(modified_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Now restore the row
            let res =
                workspaces::data_frames::rows::restore(&workspace, &commit_entry, &id_to_modify)?;

            log::debug!("res is... {:?}", res);

            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 0);

            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(modified_rows, 0);
                    assert_eq!(added_rows, 0);
                    assert_eq!(removed_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_restore_row_delete() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = workspaces::create(&repo, &commit, workspace_id)?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            // Index the dataset
            workspaces::data_frames::index(&workspace, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(&workspace, &file_path, &page_opts)?;

            let id_to_delete = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Stage a deletion
            workspaces::data_frames::rows::delete(&workspace, &commit_entry.path, &id_to_delete)?;
            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 1);

            let diff = workspaces::data_frames::diff(&workspace, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Now restore the row
            workspaces::data_frames::rows::restore(&workspace, &commit_entry, &id_to_delete)?;

            let commit_entries = workspaces::stager::list_files(&workspace)?;
            assert_eq!(commit_entries.len(), 0);

            let diff = workspaces::data_frames::diff(&workspace, &file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(modified_rows, 0);
                    assert_eq!(added_rows, 0);
                    assert_eq!(removed_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }
}
