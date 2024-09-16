use polars::frame::DataFrame;

use crate::constants::{MODS_DIR, OXEN_HIDDEN_DIR, TABLE_NAME};
use crate::core;
use crate::core::db::data_frames::workspace_df_db::select_cols_from_schema;
use crate::core::db::data_frames::{df_db, workspace_df_db};
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, Workspace};
use crate::opts::DFOpts;
use crate::{repositories, util};
use sql_query_builder::{Delete, Select};

use crate::model::diff::tabular_diff::{
    TabularDiffDupes, TabularDiffMods, TabularDiffParameters, TabularDiffSchemas,
    TabularDiffSummary, TabularSchemaDiff,
};
use crate::model::diff::{AddRemoveModifyCounts, DiffResult, TabularDiff};

use std::path::{Path, PathBuf};

pub mod columns;
pub mod rows;
pub mod schemas;

pub fn is_behind(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    let commit_path = previous_commit_ref_path(workspace, path);
    let commit_id = util::fs::read_from_path(commit_path)?;
    Ok(commit_id != workspace.commit.id)
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

pub fn is_queryable_data_frame_indexed(
    repo: &LocalRepository,
    path: &PathBuf,
    commit: &Commit,
) -> Result<bool, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::index::workspaces::data_frames::is_queryable_data_frame_indexed(
                repo, commit, path,
            )
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::workspaces::data_frames::is_queryable_data_frame_indexed(
                repo, commit, path,
            )
        }
    }
}

pub fn index(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::index::workspaces::data_frames::index(workspace, path.as_ref())
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::workspaces::data_frames::index(workspace, path.as_ref())
        }
    }
}

pub fn unindex(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let conn = df_db::get_connection(db_path)?;
    df_db::drop_table(&conn, TABLE_NAME)?;

    Ok(())
}

pub fn restore(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    // Unstage and then restage the df
    unindex(workspace, &path)?;

    // TODO: we could do this more granularly without a full reset
    index(repo, workspace, path.as_ref())?;

    Ok(())
}

pub fn count(workspace: &Workspace, path: impl AsRef<Path>) -> Result<usize, OxenError> {
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let conn = df_db::get_connection(db_path)?;

    let count = df_db::count(&conn, TABLE_NAME)?;
    Ok(count)
}

pub fn query(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("query_staged_df() got db_path: {:?}", db_path);

    let conn = df_db::get_connection(db_path)?;

    // Get the schema of this commit entry
    let schema = df_db::get_schema(&conn, TABLE_NAME)?;

    // Enrich w/ oxen cols
    let full_schema = workspace_df_db::enhance_schema_with_oxen_cols(&schema)?;

    let col_names = select_cols_from_schema(&schema)?;

    let select = Select::new().select(&col_names).from(TABLE_NAME);

    let df = df_db::select(&conn, &select, true, Some(&full_schema), Some(opts))?;

    Ok(df)
}

pub fn diff(workspace: &Workspace, path: impl AsRef<Path>) -> Result<DataFrame, OxenError> {
    let file_path = path.as_ref();
    let staged_db_path = repositories::workspaces::data_frames::duckdb_path(workspace, file_path);
    let conn = df_db::get_connection(staged_db_path)?;
    let diff_df = workspace_df_db::df_diff(&conn)?;
    Ok(diff_df)
}

pub fn full_diff(workspace: &Workspace, path: impl AsRef<Path>) -> Result<DiffResult, OxenError> {
    let repo = &workspace.base_repo;
    let commit = &workspace.commit;
    let path = path.as_ref();
    // Get commit for the branch head
    log::debug!("diff_workspace_df got repo at path {:?}", repo.path);

    repositories::CommitMerkleTree::from_path_recursive(repo, commit, path)?;

    if !is_indexed(workspace, path)? {
        return Err(OxenError::basic_str("Dataset is not indexed"));
    };

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

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

pub fn duckdb_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("db")
}

pub fn previous_commit_ref_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("COMMIT_ID")
}

pub fn column_changes_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("column_changes")
}

pub fn row_changes_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspace
        .dir()
        .join(OXEN_HIDDEN_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("row_changes")
}
