use pyo3::prelude::*;

use liboxen::model::{Remote, RemoteRepository};
use liboxen::{api, command};
use pyo3::exceptions::PyValueError;

use crate::branch::PyBranch;
use crate::error::PyOxenError;

use std::path::PathBuf;

#[pyclass]
pub struct PyRemoteRepo {
    repo: RemoteRepository,
    host: String,
    revision: String
}

#[pymethods]
impl PyRemoteRepo {
    #[new]
    #[pyo3(signature = (repo, host, revision))]
    fn py_new(repo: String, host: String, revision: String) -> PyResult<Self> {
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
                namespace: namespace.to_owned(),
                name: repo_name.to_owned(),
                remote: Remote {
                    url: liboxen::api::endpoint::remote_url_from_host(
                        &host, &namespace, &repo_name,
                    ),
                    name: String::from(liboxen::constants::DEFAULT_REMOTE_NAME),
                },
            },
            host,
            revision
        })
    }

    fn url(&self) -> &str {
        self.repo.url()
    }

    fn namespace(&self) -> &str {
        &self.repo.namespace
    }

    fn name(&self) -> &str {
        &self.repo.name
    }

    fn create(&mut self) -> Result<PyRemoteRepo, PyOxenError> {
        let result = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::repositories::create_no_root(
                &self.repo.namespace,
                &self.repo.name,
                &self.host,
            )
            .await
        })?;

        self.repo = result;

        Ok(PyRemoteRepo {
            repo: self.repo.clone(),
            host: self.host.clone(),
            revision: self.revision.clone()
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

    fn download(
        &self,
        remote_path: PathBuf,
        local_path: PathBuf,
        committish: String,
    ) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            command::remote::download(&self.repo, &remote_path, &local_path, &committish).await
        })?;

        Ok(())
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
