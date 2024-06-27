use std::path::PathBuf;
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
    #[pyo3(signature = (repo, branch_name, id))]
    fn new(repo: PyRemoteRepo, branch_name: String, id: Option<String>) -> Result<Self, PyOxenError> {
        let id = id.unwrap_or_else(|| {
            format!("workspace-{}", Uuid::new_v4())
        });

        pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::workspaces::create(
                &repo.repo,
                &id,
                &branch_name,
            )
            .await
        })?;


        Ok(Self { repo, branch_name, id })
    }

    fn status(&self, path: PathBuf) -> Result<PyStagedData, PyOxenError> {
        let remote_status = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::workspaces::changes::list(
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
            api::remote::workspaces::files::add(
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
            api::remote::workspaces::files::rm(&self.repo.repo, &self.id, path).await
        })?;
        Ok(())
    }

    fn commit(&self, message: String, branch_name: Option<String>) -> Result<PyCommit, PyOxenError> {
        let branch_name = branch_name.unwrap_or(self.branch_name.clone());
        let user = UserConfig::get()?.to_user();
        let commit = NewCommitBody { message, author: user.name, email: user.email };
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            let commit = api::remote::workspaces::commit(
                &self.repo.repo,
                &branch_name,
                &self.id,
                &commit,
            ).await?;
            Ok(PyCommit { commit })
        })
    }
}