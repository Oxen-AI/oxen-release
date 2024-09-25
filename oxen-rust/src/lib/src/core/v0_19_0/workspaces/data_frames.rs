use duckdb::Connection;

use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, TABLE_NAME};
use crate::core::db::data_frames::df_db;
use crate::core::v0_19_0::index::CommitMerkleTree;

use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{Commit, EntryDataType, LocalRepository, Workspace};
use crate::repositories;
use crate::{error::OxenError, util};
use std::path::{Path, PathBuf};

pub mod columns;
pub mod rows;

pub fn is_queryable_data_frame_indexed(
    repo: &LocalRepository,
    commit: &Commit,
    path: &PathBuf,
) -> Result<bool, OxenError> {
    match get_queryable_data_frame_workspace(repo, path, commit) {
        Ok(_workspace) => Ok(true),
        Err(e) => match e {
            OxenError::QueryableWorkspaceNotFound() => Ok(false),
            _ => Err(e),
        },
    }
}

pub fn get_queryable_data_frame_workspace(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    commit: &Commit,
) -> Result<Workspace, OxenError> {
    let path = path.as_ref();

    let commit_merkle_tree = CommitMerkleTree::from_path(repo, commit, path, true)?;
    let file_hash = commit_merkle_tree.root.hash;

    let version_path = util::fs::version_path_from_node(repo, &file_hash.to_string(), path);

    let data_type = util::fs::file_data_type(&version_path);

    if data_type != EntryDataType::Tabular {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.",
        ));
    }

    let workspaces = repositories::workspaces::list(repo)?;

    for workspace in workspaces {
        // Ensure the workspace is not editable and matches the commit ID of the resource
        if !workspace.is_editable && workspace.commit == *commit {
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

pub fn index(workspace: &Workspace, path: &Path) -> Result<(), OxenError> {
    // Is tabular just looks at the file extensions
    if !util::fs::is_tabular(path) {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.must be tabular.",
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
    let version_path = util::fs::version_path_from_node(repo, &file_hash.to_string(), path);

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
