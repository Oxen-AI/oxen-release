use duckdb::Connection;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, EXCLUDE_OXEN_COLS, TABLE_NAME};
use crate::core::db::data_frames::df_db;
use crate::core::v0_19_0::index::CommitMerkleTree;
use sql_query_builder::Delete;

use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode};
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{Commit, EntryDataType, LocalRepository, MerkleHash, Workspace};
use crate::repositories;
use crate::{error::OxenError, util};
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub mod columns;
pub mod rows;
pub mod schemas;

pub fn is_queryable_data_frame_indexed(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    match get_queryable_data_frame_workspace(repo, path, commit) {
        Ok(_workspace) => Ok(true),
        Err(e) => match e {
            OxenError::QueryableWorkspaceNotFound() => Ok(false),
            _ => Err(e),
        },
    }
}

// Annoying that we have to pass in the path and the file node here
pub fn is_queryable_data_frame_indexed_from_file_node(
    repo: &LocalRepository,
    file_node: &FileNode,
    path: &Path,
) -> Result<bool, OxenError> {
    match get_queryable_data_frame_workspace_from_file_node(repo, &file_node.last_commit_id, path) {
        Ok(_workspace) => Ok(true),
        Err(e) => match e {
            OxenError::QueryableWorkspaceNotFound() => Ok(false),
            _ => Err(e),
        },
    }
}

pub fn get_queryable_data_frame_workspace_from_file_node(
    repo: &LocalRepository,
    commit_id: &MerkleHash,
    path: &Path,
) -> Result<Workspace, OxenError> {
    let workspaces = repositories::workspaces::list(repo)?;

    for workspace in workspaces {
        // Ensure the workspace is not editable and matches the commit ID of the resource
        if !workspace.is_editable && workspace.commit.id == commit_id.to_string() {
            // Construct the path to the DuckDB resource within the workspace
            let workspace_file_db_path =
                repositories::workspaces::data_frames::duckdb_path(&workspace, path);

            // Check if the DuckDB file exists in the workspace's directory
            if workspace_file_db_path.exists() {
                // The file exists in this non-editable workspace, and the commit IDs match
                return Ok(workspace);
            }
        }
    }

    Err(OxenError::QueryableWorkspaceNotFound())
}

pub fn get_queryable_data_frame_workspace(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    commit: &Commit,
) -> Result<Workspace, OxenError> {
    let path = path.as_ref();
    log::debug!("get_queryable_data_frame_workspace path: {:?}", path);
    let file_node = repositories::tree::get_file_by_path(repo, commit, path)?
        .ok_or(OxenError::path_does_not_exist(path))?;
    if file_node.data_type != EntryDataType::Tabular {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.",
        ));
    }
    get_queryable_data_frame_workspace_from_file_node(
        repo,
        &MerkleHash::from_str(&commit.id)?,
        path,
    )
}

pub fn index(workspace: &Workspace, path: &Path) -> Result<(), OxenError> {
    // Is tabular just looks at the file extensions
    let file_node =
        repositories::tree::get_file_by_path(&workspace.base_repo, &workspace.commit, path)?
            .ok_or(OxenError::path_does_not_exist(path))?;
    if file_node.data_type != EntryDataType::Tabular {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.",
        ));
    }

    log::debug!("core::v0_19_0::workspaces::data_frames::index({:?})", path);

    let repo = &workspace.base_repo;
    let commit = &workspace.commit;

    log::debug!(
        "core::v0_19_0::workspaces::data_frames::index({:?}) got commit {:?}",
        path,
        commit
    );

    let commit_merkle_tree = CommitMerkleTree::from_path(repo, commit, path, true)?;
    let file_hash = commit_merkle_tree.root.hash;

    log::debug!(
        "core::v0_19_0::workspaces::data_frames::index({:?}) got file hash {:?}",
        path,
        file_hash
    );

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);

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
    let version_path = util::fs::version_path_from_node(repo, file_hash.to_string(), path);

    log::debug!(
        "core::v0_19_0::index::workspaces::data_frames::index({:?}) got version path: {:?}",
        path,
        version_path
    );

    let extension = match &commit_merkle_tree.root.node {
        EMerkleTreeNode::File(file_node) => file_node.extension.clone(),
        _ => {
            return Err(OxenError::basic_str("File node is not a file node"));
        }
    };

    df_db::index_file_with_id(&version_path, &conn, &extension)?;
    log::debug!(
        "core::v0_19_0::index::workspaces::data_frames::index({:?}) finished!",
        path
    );

    add_row_status_cols(&conn)?;

    // Save the current commit id so we know if the branch has advanced
    let commit_path =
        repositories::workspaces::data_frames::previous_commit_ref_path(workspace, path);
    util::fs::write_to_path(commit_path, &commit.id)?;

    Ok(())
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

pub fn extract_file_node_to_working_dir(
    workspace: &Workspace,
    dir_path: impl AsRef<Path>,
    file_node: &FileNode,
) -> Result<PathBuf, OxenError> {
    let dir_path = dir_path.as_ref();
    log::debug!(
        "extract_file_node_to_working_dir dir_path: {:?} file_node: {}",
        dir_path,
        file_node
    );
    let workspace_repo = &workspace.workspace_repo;
    let path = PathBuf::from(file_node.name.clone());

    let working_path = workspace_repo.path.join(&path);
    log::debug!("extracting file node to working dir: {:?}", working_path);
    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &path);
    let conn = df_db::get_connection(db_path)?;
    // Match on the extension
    if !working_path.exists() {
        util::fs::create_dir_all(
            working_path
                .parent()
                .expect("Failed to get parent directory"),
        )?;
    }

    let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
        "\"{}\" = '{}'",
        DIFF_STATUS_COL,
        StagedRowStatus::Removed
    ));
    let res = conn.execute(&delete.to_string(), [])?;
    log::debug!("delete query result is: {:?}", res);

    let excluded_cols = get_existing_excluded_columns(&conn, TABLE_NAME)?;
    let sql = format!("SELECT * EXCLUDE ({}) FROM '{}'", excluded_cols, TABLE_NAME);
    let query = wrap_sql_for_export(&sql, &working_path);
    log::debug!("extracting file node to working dir query: {:?}", query);
    conn.execute(&query, [])?;

    Ok(working_path)
}

pub fn valid_export_extensions() -> Vec<&'static str> {
    vec!["csv", "tsv", "parquet", "jsonl", "json", "ndjson"]
}

pub fn is_valid_export_extension(path: impl AsRef<Path>) -> bool {
    let path = path.as_ref();
    let extension = path
        .extension()
        .unwrap_or_default()
        .to_str()
        .unwrap_or_default();
    valid_export_extensions().contains(&extension)
}

pub fn wrap_sql_for_export(sql: &str, path: impl AsRef<Path>) -> String {
    let path = path.as_ref();
    let extension = path
        .extension()
        .unwrap_or_default()
        .to_str()
        .unwrap_or_default();
    match extension {
        "csv" => format!(
            "COPY ({}) TO '{}' (HEADER, DELIMITER ',');",
            sql,
            path.to_string_lossy()
        ),
        "tsv" => format!(
            "COPY ({}) TO '{}' (HEADER, DELIMITER '\t');",
            sql,
            path.to_string_lossy()
        ),
        "parquet" => format!(
            "COPY ({}) TO '{}' (FORMAT PARQUET);",
            sql,
            path.to_string_lossy()
        ),
        "jsonl" | "ndjson" => format!(
            "COPY ({}) TO '{}' (FORMAT JSON);",
            sql,
            path.to_string_lossy()
        ),
        "json" => format!(
            "COPY ({}) TO '{}' (FORMAT JSON, ARRAY true);",
            sql,
            path.to_string_lossy()
        ),
        _ => sql.to_string(),
    }
}

fn get_existing_excluded_columns(conn: &Connection, table_name: &str) -> Result<String, OxenError> {
    // Query to get existing columns in the table
    let existing_cols_query = format!(
        "SELECT column_name FROM information_schema.columns WHERE table_name = '{}'",
        table_name
    );

    let mut stmt = conn.prepare(&existing_cols_query)?;
    let existing_cols: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .filter_map(Result::ok)
        .collect();

    // Filter excluded columns to only those that exist in the table
    let filtered_excluded_cols: Vec<String> = EXCLUDE_OXEN_COLS
        .iter()
        .filter(|col| existing_cols.contains(&col.to_string()))
        .map(|col| format!("\"{}\"", col))
        .collect();

    Ok(filtered_excluded_cols.join(", "))
}
