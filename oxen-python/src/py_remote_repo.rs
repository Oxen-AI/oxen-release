
use liboxen::model::file::{FileContents, FileNew};
use pyo3::prelude::*;

use liboxen::config::UserConfig;
use liboxen::model::{Remote, RemoteRepository, RepoNew};
use liboxen::{api, repositories};

use pyo3::exceptions::PyValueError;
use std::path::PathBuf;

use crate::error::PyOxenError;
use crate::py_branch::PyBranch;
use crate::py_commit::PyCommit;

use crate::py_paginated_dir_entries::PyPaginatedDirEntries;

#[derive(Clone)]
#[pyclass]
pub struct PyRemoteRepo {
    pub repo: RemoteRepository,
    #[pyo3(get)]
    pub host: String,
    #[pyo3(get)]
    pub revision: String,
    #[pyo3(get)]
    pub scheme: String,
}

#[pymethods]
impl PyRemoteRepo {
    #[new]
    #[pyo3(signature = (repo, host, revision="main", scheme="https"))]
    fn py_new(repo: String, host: String, revision: &str, scheme: &str) -> PyResult<Self> {
        let (namespace, repo_name) = match repo.split_once('/') {
            Some((namespace, repo_name)) => (namespace.to_string(), repo_name.to_string()),
            None => {
                return Err(PyValueError::new_err(
                    format!("Invalid repo name, must be in format namespace/repo_name. Got {}", repo),
                ))
            }
        };

        Ok(Self {
            repo: RemoteRepository {
                namespace: namespace.to_owned(),
                name: repo_name.to_owned(),
                remote: Remote {
                    url: liboxen::api::endpoint::remote_url_from_namespace_name_scheme(
                        &host, &namespace, &repo_name, scheme
                    ),
                    name: String::from(liboxen::constants::DEFAULT_REMOTE_NAME),
                },
                is_empty: false,
                min_version: Some(liboxen::constants::MIN_OXEN_VERSION.to_string()),
            },
            revision: revision.to_string(),
            scheme: scheme.to_string(),
            host,
        })
    }

    fn __repr__(&self) -> String {
        format!("RemoteRepo(namespace='{}', name='{}', url='{}')", self.namespace(), self.name(), self.url())
    }

    fn __str__(&self) -> String {
        format!("{}/{}", self.namespace(), self.name())
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

    fn revision(&self) -> &str {
        &self.revision
    }

    fn set_revision(&mut self, new_revision: String) {
        self.revision = new_revision;
    }

    fn create(&mut self, empty: bool, is_public: bool) -> Result<PyRemoteRepo, PyOxenError> {
        let result = pyo3_asyncio::tokio::get_runtime().block_on(async {
            if empty {
                let mut repo = RepoNew::from_namespace_name_host(
                    self.repo.namespace.clone(),
                    self.repo.name.clone(),
                    self.host.clone(),
                );
                repo.is_public = Some(is_public);
                repo.scheme = Some(self.scheme.clone());
                api::client::repositories::create_empty(repo).await
            } else {
                let config = UserConfig::get()?;
                let user = config.to_user();
                let files: Vec<FileNew> = vec![FileNew {
                    path: PathBuf::from("README.md"),
                    contents: FileContents::Text(format!("# {}\n", &self.repo.name)),
                    user: user.clone()
                }];
                let mut repo = RepoNew::from_files(&self.repo.namespace, &self.repo.name, files);
                repo.host = Some(self.host.clone());
                repo.is_public = Some(is_public);
                repo.scheme = Some(self.scheme.clone());
                api::client::repositories::create(repo).await
            }
        })?;

        self.repo = result;

        Ok(PyRemoteRepo {
            repo: self.repo.clone(),
            host: self.host.clone(),
            revision: self.revision.clone(),
            scheme: self.scheme.clone(),
        })
    }

    fn exists(&self) -> Result<bool, PyOxenError> {
        let exists = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::client::repositories::exists(&self.repo).await })?;

        Ok(exists)
    }

    fn delete(&self) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::client::repositories::delete(&self.repo).await })?;

        Ok(())
    }

    fn download(&self, remote_path: PathBuf, local_path: PathBuf, revision: &str) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            if !revision.is_empty() {
                repositories::download(&self.repo, &remote_path, &local_path, revision).await
            } else {
                repositories::download(&self.repo, &remote_path, &local_path, &self.revision).await
            }
        })?;

        Ok(())
    }

    fn log(&self) -> Result<Vec<PyCommit>, PyOxenError> {
        let log = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::client::commits::list_commit_history(&self.repo, &self.revision).await
        })?;
        Ok(log.iter().map(|c| PyCommit { commit: c.clone() }).collect())
    }

    fn list_branches(&self) -> Result<Vec<PyBranch>, PyOxenError> {
        let branches = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::client::branches::list(&self.repo).await })?;
        Ok(branches
            .iter()
            .map(|b| PyBranch::new(b.name.clone(), b.commit_id.clone()))
            .collect())
    }

    fn ls(&self, path: PathBuf, page_num: usize, page_size: usize) -> Result<PyPaginatedDirEntries, PyOxenError> {
        let result = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::client::dir::list(
                &self.repo,
                &self.revision,
                &path,
                page_num,
                page_size,
            )
            .await
        })?;

        // Convert remote status to a PyStagedData using the from method
        Ok(PyPaginatedDirEntries::from(result))
    }

    fn get_branch(&self, branch_name: String) -> PyResult<PyBranch> {
        log::info!("Get branch... {branch_name}");

        let branch = pyo3_asyncio::tokio::get_runtime().block_on(async {
            log::info!("From repo... {}", self.repo.remote.url);
            api::client::branches::get_by_name(&self.repo, &branch_name).await
        });

        match branch {
            Ok(Some(branch)) => Ok(PyBranch::from(branch)),
            _ => Err(PyValueError::new_err("could not get branch")),
        }
    }

    fn get_commit(&self, commit_id: String) -> PyResult<PyCommit> {
        let commit = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::client::commits::get_by_id(&self.repo, &commit_id).await });
        match commit {
            Ok(Some(commit)) => Ok(PyCommit { commit }),
            _ => Err(PyValueError::new_err("could not get commit id {commit_id}")),
        }
    }

    fn create_branch(&self, new_name: String) -> PyResult<PyBranch> {
        let branch = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::client::branches::create_from_branch(&self.repo, &new_name, &self.revision).await
        });

        match branch {
            Ok(branch) => Ok(PyBranch::from(branch)),
            _ => Err(PyValueError::new_err("Could not get or create branch")),
        }
    }

    fn checkout(&mut self, revision: String) -> PyResult<()> {
        let branch = self.get_branch(revision.clone());
        if let Ok(branch) = branch {
            self.set_revision(branch.name().to_string());
            return Ok(());
        }

        let commit = self.get_commit(revision.clone());
        match commit {
            Ok(commit) => {
                self.set_revision(commit.commit.id);
                Ok(())
            },
            _ => Err(PyValueError::new_err(format!("{} is not a valid branch name or commit id. Consider creating it with `create_branch`", revision)))
        }
    }
}
