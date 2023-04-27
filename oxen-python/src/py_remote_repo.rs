use pyo3::prelude::*;

use liboxen::api;
use liboxen::model::{Remote, RemoteRepository};
use pyo3::exceptions::PyValueError;

use crate::branch::PyBranch;
use crate::error::PyOxenError;

#[pyclass]
pub struct PyRemoteRepo {
    repo: RemoteRepository,
}

#[pymethods]
impl PyRemoteRepo {
    #[new]
    #[pyo3(signature = (repo, host))]
    fn py_new(repo: String, host: String) -> PyResult<Self> {
        let (namespace, repo_name) = match repo.split_once('/') {
            Some((namespace, repo_name)) => (namespace.to_string(), repo_name.to_string()),
            None => {
                return Err(PyValueError::new_err(
                    "Invalid repo name, must be in format namespace/repo_name",
                ))
            }
        };

        Ok(Self {
            repo: RemoteRepository {
                namespace,
                name: repo_name,
                remote: Remote {
                    url: host,
                    name: String::from(liboxen::constants::DEFAULT_REMOTE_NAME),
                },
            },
        })
    }

    fn create(&mut self) -> Result<PyRemoteRepo, PyOxenError> {
        let result = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::repositories::create_no_root(
                &self.repo.namespace,
                &self.repo.name,
                &self.repo.remote.url,
            )
            .await
        })?;

        self.repo = result;

        Ok(PyRemoteRepo {
            repo: self.repo.clone(),
        })
    }

    fn exists(&self) -> Result<bool, PyOxenError> {
        let exists = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::remote::repositories::exists(&self.repo).await })?;

        Ok(exists)
    }

    fn delete(&self) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::remote::repositories::delete(&self.repo).await })?;

        Ok(())
    }

    fn url(&self) -> &str {
        self.repo.url()
    }

    fn get_branch(&self, branch_name: String) -> PyResult<PyBranch> {
        log::info!("Get branch... {branch_name}");

        let branch = pyo3_asyncio::tokio::get_runtime().block_on(async {
            log::info!("From repo... {}", self.repo.remote.url);
            api::remote::branches::get_by_name(&self.repo, &branch_name).await
        });

        match branch {
            Ok(Some(branch)) => Ok(PyBranch {
                name: branch.name,
                commit_id: branch.commit_id,
            }),
            _ => Err(PyValueError::new_err("could not get branch")),
        }
    }
}
