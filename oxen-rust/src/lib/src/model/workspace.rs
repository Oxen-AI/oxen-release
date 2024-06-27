use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::api;
use crate::constants;
use crate::constants::WORKSPACE_NAME;
use crate::constants::{OXEN_HIDDEN_DIR, WORKSPACES_DIR, WORKSPACE_COMMIT_ID};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;

fn workspace_dir(repo: &LocalRepository, workspace_id_hash: &str) -> PathBuf {
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(WORKSPACES_DIR)
        .join(workspace_id_hash)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Workspace {
    pub id: String,
    // Workspaces have a base repository that they are created in .oxen/
    pub base_repo: LocalRepository,
    // And a sub repository that is just to make changes in
    // .oxen/workspaces/<workspace_id>/.oxen/
    pub workspace_repo: LocalRepository,
    // .oxen/workspaces/<workspace_id>/.oxen/WORKSPACE_COMMIT_ID
    pub commit: Commit,
}

impl Workspace {
    /// Loads a workspace from the filesystem. Must call create() first to create the workspace.
    ///
    /// Returns an error if the workspace does not exist
    pub fn new(repo: &LocalRepository, workspace_id: impl AsRef<str>) -> Result<Self, OxenError> {
        let workspace_id = workspace_id.as_ref();
        // They may pass in a string that is not a valid directory name, so we hash it
        // We keep workspace_name as the original string for display purposes
        let workspace_id_hash = util::hasher::hash_str_sha256(workspace_id);
        log::debug!(
            "workspace::new got workspace_id: {workspace_id:?} hash: {workspace_id_hash:?}"
        );

        let workspace_dir = workspace_dir(repo, &workspace_id_hash);
        let commit_id_path = workspace_dir
            .join(OXEN_HIDDEN_DIR)
            .join(WORKSPACE_COMMIT_ID);

        if !commit_id_path.exists() {
            return Err(OxenError::workspace_not_found(workspace_id.into()));
        }

        let commit_id = util::fs::read_from_path(commit_id_path)?;
        let Some(commit) = api::local::commits::get_by_id(repo, &commit_id)? else {
            return Err(OxenError::basic_str(format!(
                "Workspace {} has invalid commit_id {}",
                workspace_id, commit_id
            )));
        };
        Ok(Workspace {
            id: workspace_id.to_owned(),
            base_repo: repo.clone(),
            workspace_repo: LocalRepository::new(&workspace_dir)?,
            commit,
        })
    }

    /// Creates a new workspace and saves it to the filesystem
    pub fn create(
        base_repo: &LocalRepository,
        commit: &Commit,
        workspace_id: impl AsRef<str>,
    ) -> Result<Self, OxenError> {
        let workspace_id = workspace_id.as_ref();
        let workspace_name = workspace_id.to_owned();
        // They may pass in a string that is not a valid directory name, so we hash it
        // We keep workspace_name as the original string for display purposes
        let workspace_id_hash = util::hasher::hash_str_sha256(workspace_id);
        let workspace_dir = workspace_dir(base_repo, &workspace_id_hash);
        let oxen_dir = workspace_dir.join(OXEN_HIDDEN_DIR);
        log::debug!("index::workspaces::create called! {:?}", oxen_dir);

        if oxen_dir.exists() {
            log::debug!(
                "index::workspaces::create already have oxen repo directory {:?}",
                oxen_dir
            );
            return Err(OxenError::basic_str(format!(
                "Workspace {} already exists",
                workspace_id
            )));
        }

        log::debug!("index::workspaces::create Initializing oxen repo! 🐂");

        let workspace_repo = Self::init_workspace_repo(base_repo, &workspace_dir)?;
        // write the commit_id to the workspace dir
        let commit_id_path = workspace_repo
            .path
            .join(OXEN_HIDDEN_DIR)
            .join(WORKSPACE_COMMIT_ID);
        log::debug!(
            "index::workspaces::create writing commit_id to workspace_dir: {commit_id_path:?}"
        );
        util::fs::write_to_path(&commit_id_path, &commit.id)?;

        // write the workspace name to the workspace dir
        let workspace_name_path = workspace_dir.join(OXEN_HIDDEN_DIR).join(WORKSPACE_NAME);
        util::fs::write_to_path(workspace_name_path, workspace_name)?;

        Ok(Workspace {
            id: workspace_id.to_owned(),
            base_repo: base_repo.clone(),
            workspace_repo,
            commit: commit.clone(),
        })
    }

    pub fn list(repo: &LocalRepository) -> Result<Vec<Self>, OxenError> {
        let workspaces_dir = repo.path.join(OXEN_HIDDEN_DIR).join(WORKSPACES_DIR);
        log::debug!("workspace::list got workspaces_dir: {workspaces_dir:?}");
        if !workspaces_dir.exists() {
            return Ok(vec![]);
        }

        let workspaces_hashes = util::fs::list_dirs_in_dir(&workspaces_dir)?;
        log::debug!("workspace::list got workspaces_hashes: {workspaces_hashes:?}");

        let workspaces = workspaces_hashes
            .iter()
            .map(|workspace_hash| {
                // read the workspace name from the workspace dir
                let workspace_name_path = workspace_hash.join(OXEN_HIDDEN_DIR).join(WORKSPACE_NAME);
                let workspace_name = util::fs::read_from_path(workspace_name_path)?;
                Workspace::new(repo, workspace_name)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(workspaces)
    }

    fn init_workspace_repo(
        repo: &LocalRepository,
        workspace_dir: &Path,
    ) -> Result<LocalRepository, OxenError> {
        let oxen_hidden_dir = repo.path.join(OXEN_HIDDEN_DIR);
        let workspace_hidden_dir = workspace_dir.join(OXEN_HIDDEN_DIR);
        log::debug!("init_workspace_repo {workspace_hidden_dir:?}");
        util::fs::create_dir_all(&workspace_hidden_dir)?;

        let dirs_to_copy = vec![
            constants::COMMITS_DIR,
            constants::HISTORY_DIR,
            constants::REFS_DIR,
            constants::HEAD_FILE,
            constants::OBJECTS_DIR,
        ];

        for dir in dirs_to_copy {
            let oxen_dir = oxen_hidden_dir.join(dir);
            let target_dir = workspace_hidden_dir.join(dir);

            log::debug!("init_workspace_repo copying {dir} dir {oxen_dir:?} -> {target_dir:?}");
            if oxen_dir.is_dir() {
                util::fs::copy_dir_all(oxen_dir, target_dir)?;
            } else {
                util::fs::copy(oxen_dir, target_dir)?;
            }
        }

        LocalRepository::new(workspace_dir)
    }

    /// Returns the path to the workspace directory
    pub fn dir(&self) -> PathBuf {
        let workspace_id_hash = util::hasher::hash_str_sha256(&self.id);
        workspace_dir(&self.base_repo, &workspace_id_hash)
    }
}
