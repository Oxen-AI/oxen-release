use liboxen::model::LocalRepository;
use liboxen::opts::CloneOpts;
use pyo3::prelude::*;

use liboxen::api;
use liboxen::command;

use std::path::PathBuf;

use crate::error::PyOxenError;
use crate::py_commit::PyCommit;
use crate::py_staged_data::PyStagedData;

#[pyclass]
pub struct PyRepo {
    path: PathBuf,
}

#[pymethods]
impl PyRepo {
    #[new]
    #[pyo3(signature = (path))]
    fn py_new(path: PathBuf) -> PyResult<Self> {
        Ok(Self { path })
    }

    pub fn path(&self) -> PyResult<String> {
        Ok(self.path.to_string_lossy().to_string())
    }

    pub fn init(&self) -> Result<(), PyOxenError> {
        command::init(&self.path)?;
        Ok(())
    }

    pub fn clone(&mut self, url: &str, branch: &str, shallow: bool) -> Result<(), PyOxenError> {
        let repo = pyo3_asyncio::tokio::get_runtime().block_on(async {
            let opts = CloneOpts {
                url: url.to_string(),
                dst: self.path.clone(),
                branch: branch.to_string(),
                shallow,
            };
            command::clone(&opts).await
        })?;

        // cd repo_path
        self.path = repo.path;

        Ok(())
    }

    pub fn add(&self, path: PathBuf) -> Result<(), PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path).unwrap();
        command::add(&repo, path).unwrap();
        Ok(())
    }

    pub fn status(&self) -> Result<PyStagedData, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let status = command::status(&repo)?;
        Ok(PyStagedData { data: status })
    }

    pub fn commit(&self, message: &str) -> Result<PyCommit, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let commit = command::commit(&repo, message)?;
        Ok(PyCommit { commit })
    }

    pub fn log(&self) -> Result<Vec<PyCommit>, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let commits = api::local::commits::list(&repo)?;
        Ok(commits.iter().map(|c| PyCommit { commit: c.clone() }).collect())
    }

    pub fn set_remote(&self, name: &str, url: &str) -> Result<(), PyOxenError> {
        let mut repo = LocalRepository::from_dir(&self.path)?;
        command::config::set_remote(&mut repo, name, url)?;
        Ok(())
    }

    pub fn push(&self, remote: &str, branch: &str) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            let repo = LocalRepository::from_dir(&self.path)?;
            command::push_remote_branch(&repo, remote, branch).await
        })?;
        Ok(())
    }

    pub fn pull(&self, remote: &str, branch: &str) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            let repo = LocalRepository::from_dir(&self.path)?;
            command::pull_remote_branch(&repo, remote, branch).await
        })?;
        Ok(())
    }
}
