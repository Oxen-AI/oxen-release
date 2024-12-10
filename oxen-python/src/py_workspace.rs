use std::path::{Path, PathBuf};
use liboxen::api;
use liboxen::config::UserConfig;
use liboxen::model::NewCommitBody;
use pyo3::prelude::*;
use uuid::Uuid;

use crate::py_commit::PyCommit;
use crate::py_remote_repo::PyRemoteRepo;
use crate::error::PyOxenError;
use crate::py_staged_data::PyStagedData;

#[derive(Clone)]
#[pyclass]
pub struct PyWorkspace {
    pub repo: PyRemoteRepo,
    pub branch_name: String,
    pub id: String,
}

#[pymethods]
impl PyWorkspace {
    #[new]
    #[pyo3(signature = (repo, branch_name, name, path))]
    fn new(repo: PyRemoteRepo, branch_name: String, name: Option<String>, path: Option<String>) -> Result<Self, PyOxenError> {
        let name = name.unwrap_or_else(|| {
            format!("workspace-{}", Uuid::new_v4())
        });

        let workspace = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::client::workspaces::create_with_path(
                &repo.repo,
                &branch_name,
                &name,
                Path::new(&path.unwrap_or("/".to_string())),
                Some(name.clone())
            )
            .await
        })?;

        Ok(Self { repo, branch_name, id: workspace.id })
    }

    fn id(&self) -> String {
        self.id.clone()
    }

    fn branch(&self) -> String {
        self.branch_name.clone()
    }

    fn status(&self, path: PathBuf) -> Result<PyStagedData, PyOxenError> {
        let remote_status = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::client::workspaces::changes::list(
                &self.repo.repo,
                &self.id,
                &path,
                liboxen::constants::DEFAULT_PAGE_NUM,
                liboxen::constants::DEFAULT_PAGE_SIZE,
            )
            .await
        })?;

        // Convert remote status to a PyStagedData using the from method
        Ok(PyStagedData::from(remote_status))
    }

    fn add(&self, src: PathBuf, dst: String) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::client::workspaces::files::post_file(
                &self.repo.repo,
                &self.id,
                &dst,
                src,
            )
            .await
        })?;
        Ok(())
    }

    fn rm(&self, path: PathBuf) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::client::workspaces::files::rm(&self.repo.repo, &self.id, path).await
        })?;
        Ok(())
    }

    fn commit(&self, message: String, should_delete: bool, branch_name: Option<String>) -> Result<PyCommit, PyOxenError> {
        let branch_name = branch_name.unwrap_or(self.branch_name.clone());
        let user = UserConfig::get()?.to_user();
        let commit = NewCommitBody { message, author: user.name, email: user.email };
        let commit = pyo3_asyncio::tokio::get_runtime().block_on(async {
            let commit = api::client::workspaces::commit(
                &self.repo.repo,
                &branch_name,
                &self.id,
                &commit,
            ).await?;
            Ok(PyCommit { commit })
        });

        if !should_delete {
            // Commit will delete the workspace, since they are tied to commits
            // so we create a new one off the branch if success
            pyo3_asyncio::tokio::get_runtime().block_on(async {
                api::client::workspaces::create(
                    &self.repo.repo,
                    &branch_name,
                    &self.id,
                )
                .await
            })?;
        }

        commit
    }
}