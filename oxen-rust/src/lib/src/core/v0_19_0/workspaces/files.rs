use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::constants::FILES_DIR;
use crate::constants::STAGED_DIR;
use crate::constants::VERSIONS_DIR;
use crate::core::db;
use crate::core::v0_19_0::add::process_add_file;
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::error::OxenError;
use crate::model::workspace::Workspace;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::util;

pub fn add(workspace: &Workspace, filepath: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let base_repo = &workspace.base_repo;

    // Stage the file using the repositories::add method
    let commit = workspace.commit.clone();
    p_add_file(base_repo, workspace_repo, &Some(commit), filepath)?;

    // Return the relative path of the file in the workspace
    let relative_path = util::fs::path_relative_to_dir(filepath, &workspace_repo.path)?;
    Ok(relative_path)
}

pub fn delete(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    todo!()
}

pub fn exists(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    todo!()
}

fn p_add_file(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let repo_path = workspace_repo.path.clone();
    let versions_path = util::fs::oxen_hidden_dir(&base_repo.path)
        .join(VERSIONS_DIR)
        .join(FILES_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let mut maybe_dir_node = None;
    if let Some(head_commit) = maybe_head_commit {
        let path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
        let parent_path = path.parent().unwrap_or(Path::new(""));
        maybe_dir_node = CommitMerkleTree::dir_with_children(base_repo, head_commit, parent_path)?;
    }

    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    process_add_file(
        &repo_path,
        &versions_path,
        &staged_db,
        &maybe_dir_node,
        path,
        &seen_dirs,
    )
}
