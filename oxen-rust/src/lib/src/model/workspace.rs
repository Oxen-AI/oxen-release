use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::api;
use crate::constants;
use crate::constants::{OXEN_HIDDEN_DIR, WORKSPACES_DIR, WORKSPACE_COMMIT_ID};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;

fn workspace_dir(repo: &LocalRepository, workspace_id: &str) -> PathBuf {
    // Just in case they pass in the email or some other random string, hash it for nice dir name
    let workspace_id_hash = util::hasher::hash_str_sha256(workspace_id);
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
        let workspace_dir = workspace_dir(repo, workspace_id);
        let commit_id_path = workspace_dir
            .join(OXEN_HIDDEN_DIR)
            .join(WORKSPACE_COMMIT_ID);
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
        let workspace_dir = workspace_dir(base_repo, workspace_id);
        let oxen_dir = workspace_dir.join(OXEN_HIDDEN_DIR);
        if !oxen_dir.exists() {
            log::debug!("index::workspaces::create already have oxen repo directory");
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

        Ok(Workspace {
            id: workspace_id.to_owned(),
            base_repo: base_repo.clone(),
            workspace_repo,
            commit: commit.clone(),
        })
    }

    fn init_workspace_repo(
        repo: &LocalRepository,
        workspace_dir: &Path,
    ) -> Result<LocalRepository, OxenError> {
        let oxen_hidden_dir = repo.path.join(OXEN_HIDDEN_DIR);
        let staging_oxen_dir = workspace_dir.join(OXEN_HIDDEN_DIR);
        log::debug!("Creating staging_oxen_dir {staging_oxen_dir:?}");
        util::fs::create_dir_all(&staging_oxen_dir)?;

        let dirs_to_copy = vec![
            constants::COMMITS_DIR,
            constants::HISTORY_DIR,
            constants::REFS_DIR,
            constants::HEAD_FILE,
            constants::OBJECTS_DIR,
        ];

        for dir in dirs_to_copy {
            let oxen_dir = oxen_hidden_dir.join(dir);
            let workspace_dir = staging_oxen_dir.join(dir);

            log::debug!("Copying {dir} dir {oxen_dir:?} -> {workspace_dir:?}");
            if oxen_dir.is_dir() {
                util::fs::copy_dir_all(oxen_dir, workspace_dir)?;
            } else {
                util::fs::copy(oxen_dir, workspace_dir)?;
            }
        }

        LocalRepository::new(workspace_dir)
    }

    /// Returns the path to the workspace directory
    pub fn dir(&self) -> PathBuf {
        workspace_dir(&self.base_repo, &self.id)
    }
}
