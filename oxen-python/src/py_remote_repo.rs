use pyo3::prelude::*;

use std::collections::HashMap;


use liboxen::model::{CommitBody, Remote, RemoteRepository, StagedData, StagedEntry, StagedEntryStatus};
use liboxen::{api, command};
use liboxen::api;
use liboxen::config::UserConfig;

use pyo3::exceptions::PyValueError;
use std::path::PathBuf;

use crate::branch::PyBranch;
use crate::py_commit::PyCommit;
use crate::error::PyOxenError;

use std::path::PathBuf;
use crate::py_staged_data::PyStagedData;


#[pyclass]
pub struct PyRemoteRepo {
    repo: RemoteRepository,
    host: String,
    revision: String,
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
            revision,
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

    fn revision(&self) -> &str {
        &self.revision
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
            revision: self.revision.clone(),
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
    ) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            command::remote::download(&self.repo, &remote_path, &local_path, &self.revision).await
        })?;

        Ok(())
    }

    fn add(&self, branch_name: String, directory_name: String, path: PathBuf,) -> Result<(), PyOxenError> {
        let user_id = UserConfig::identifier()?;
        pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::remote::staging::add_file(&self.repo, &branch_name, &user_id, &directory_name, path).await})?;
        Ok(())
    }

    fn commit(&self, branch_name: String, message: String) -> Result<(), PyOxenError> {
        let user_id = UserConfig::identifier()?;
        let user = UserConfig::get()?.to_user();
        let commit = CommitBody { message, user };
        pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::remote::staging::commit_staged(&self.repo, &branch_name, &user_id, &commit).await })?;
        Ok(())
    }

    fn log(&self, branch_name_or_commit_id: String) -> Result<Vec<PyCommit>, PyOxenError> {
        let log = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::remote::commits::list_commit_history(&self.repo, &branch_name_or_commit_id).await })?;
        Ok(log.iter().map(|c| PyCommit { commit: c.clone() }).collect())
    }

    fn list_branches(&self) -> Result<Vec<PyBranch>, PyOxenError> {
        let branches = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::remote::branches::list(&self.repo).await })?;
        Ok(branches.iter().map(|b| PyBranch { name: b.name.clone(), commit_id: b.commit_id.clone() }).collect())
    }
    
    fn status(&self, branch_name: String, path: PathBuf) -> Result<PyStagedData, PyOxenError> {
        let user_id = UserConfig::identifier()?;
        let remote_status = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::remote::staging::status(&self.repo, &branch_name, &user_id, &path, liboxen::constants::DEFAULT_PAGE_NUM, liboxen::constants::DEFAULT_PAGE_SIZE).await})?;
        
        let mut status = StagedData::empty();
        status.added_dirs = remote_status.added_dirs;
        let added_files: HashMap<PathBuf, StagedEntry> =
        HashMap::from_iter(remote_status.added_files.entries.into_iter().map(|e| {
            (
                PathBuf::from(e.filename),
                StagedEntry::empty_status(StagedEntryStatus::Added),
            )
        }));
        let added_mods: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.modified_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename),
                    StagedEntry::empty_status(StagedEntryStatus::Modified),
                )
            }));
        status.added_files = added_files.into_iter().chain(added_mods).collect();

        Ok(PyStagedData { data: status })
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

    fn create_or_get_branch(&self, branch_name: String) -> PyResult<PyBranch> {
        log::info!("Create or get branch... {branch_name}");

        let branch = pyo3_asyncio::tokio::get_runtime().block_on(async {
            log::info!("From repo...{}", self.repo.remote.url);
            api::remote::branches::create_or_get(&self.repo, &branch_name).await
        });

        match branch {
            Ok(branch) => Ok(PyBranch {
                name: branch.name,
                commit_id: branch.commit_id
            }),
            _ => Err(PyValueError::new_err("could not get / create branch"))
        }
    }

    fn create_from_or_get_branch(&self, new_name: String, from_name: String) -> PyResult<PyBranch> {
        log::info!("create from or get branch... {new_name} from {from_name}");
        log::info!("From repo... {}", self.repo.remote.url);
        let branch = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::branches::create_from_or_get(&self.repo, &new_name, &from_name).await
        });

        match branch {
            Ok(branch) => Ok(PyBranch {
                name: branch.name,
                commit_id: branch.commit_id
            }),
            _ => Err(PyValueError::new_err("Could not get or create branch"))
        }
    }
}
