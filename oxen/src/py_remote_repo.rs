use liboxen::error::OxenError;
use liboxen::model::entry::mod_entry::ModType;
use liboxen::model::file::FileNew;
use liboxen::opts::DFOpts;
use pyo3::prelude::*;

use liboxen::config::UserConfig;
use liboxen::model::{NewCommitBody, ContentType, Remote, RemoteRepository, RepoNew};
use liboxen::{api, command};

use pyo3::exceptions::PyValueError;
use std::path::PathBuf;

use crate::error::PyOxenError;
use crate::py_branch::PyBranch;
use crate::py_commit::PyCommit;

use crate::py_staged_data::PyStagedData;
use crate::py_paginated_dir_entries::PyPaginatedDirEntries;

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
                    "Invalid repo name, must be in format namespace/repo_name",
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
                api::remote::repositories::create_empty(repo).await
            } else {
                let config = UserConfig::get()?;
                let user = config.to_user();
                let files: Vec<FileNew> = vec![FileNew {
                    path: PathBuf::from("README.md"),
                    contents: format!("# {}\n", &self.repo.name),
                    user: user.clone()
                }];
                let mut repo = RepoNew::from_files(&self.repo.namespace, &self.repo.name, files);
                repo.host = Some(self.host.clone());
                repo.is_public = Some(is_public);
                repo.scheme = Some(self.scheme.clone());
                api::remote::repositories::create(repo).await
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
            .block_on(async { api::remote::repositories::exists(&self.repo).await })?;

        Ok(exists)
    }

    fn delete(&self) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::remote::repositories::delete(&self.repo).await })?;

        Ok(())
    }

    fn download(&self, remote_path: PathBuf, local_path: PathBuf, revision: &str) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            if !revision.is_empty() {
                command::remote::download(&self.repo, &remote_path, &local_path, revision).await
            } else {
                command::remote::download(&self.repo, &remote_path, &local_path, &self.revision).await
            }
        })?;

        Ok(())
    }

    fn add(&self, src: PathBuf, dst: String) -> Result<(), PyOxenError> {
        let user_id = UserConfig::identifier()?;
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::staging::add_file(
                &self.repo,
                &self.revision,
                &user_id,
                &dst,
                src,
            )
            .await
        })?;
        Ok(())
    }

    fn remove(&self, path: PathBuf) -> Result<(), PyOxenError> {
        let user_id = UserConfig::identifier()?;
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::staging::rm_file(&self.repo, &self.revision, &user_id, path).await
        })?;
        Ok(())
    }

    fn commit(&self, message: String) -> Result<PyCommit, PyOxenError> {
        let user_id = UserConfig::identifier()?;
        let user = UserConfig::get()?.to_user();
        let commit = NewCommitBody { message, author: user.name, email: user.email };
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            let commit = api::remote::staging::commit(&self.repo, &self.revision, &user_id, &commit).await?;
            Ok(PyCommit { commit })
        })
    }

    fn log(&self) -> Result<Vec<PyCommit>, PyOxenError> {
        let log = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::commits::list_commit_history(&self.repo, &self.revision).await
        })?;
        Ok(log.iter().map(|c| PyCommit { commit: c.clone() }).collect())
    }

    fn get_df_size(&self, path: PathBuf) -> Result<(usize, usize), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            let mut opts = DFOpts::empty();
            opts.slice = Some("0..1".to_string());

            let response = api::remote::df::get(
                &self.repo,
                &self.revision,
                path,
                DFOpts::empty(),
            )
            .await?;

            Ok((response.data_frame.source.size.width, response.data_frame.source.size.height))
        })
    }

    fn get_df_row(&self, path: PathBuf, row: usize) -> Result<String, PyOxenError> {
        let data = pyo3_asyncio::tokio::get_runtime().block_on(async {
            let mut opts = DFOpts::empty();
            opts.slice = Some(format!("{}..{}", row, row+1));

            let response = api::remote::df::get(
                &self.repo,
                &self.revision,
                path,
                opts,
            )
            .await?;

            // convert view to json string
            match serde_json::to_string(&response.data_frame.view.data) {
                Ok(json) => Ok(json),
                Err(e) => Err(OxenError::basic_str(format!("Could not convert view to json: {}", e)))
            }
        })?;
        Ok(data)
    }

    fn get_df_slice(
        &self,
        path: PathBuf,
        start: usize,
        end: usize,
        columns: Vec<String>
    ) -> Result<String, PyOxenError> {
        let data = pyo3_asyncio::tokio::get_runtime().block_on(async {
            let mut opts = DFOpts::empty();
            opts.slice = Some(format!("{}..{}", start, end));

            if columns.len() > 0 {
                // turn columns into comma separated list
                let columns = columns.join(",");
                opts.columns = Some(columns);
            }

            let response = api::remote::df::get(
                &self.repo,
                &self.revision,
                path,
                opts,
            )
            .await?;

            // convert view to json string
            match serde_json::to_string(&response.data_frame.view.data) {
                Ok(json) => Ok(json),
                Err(e) => Err(OxenError::basic_str(format!("Could not convert view to json: {}", e)))
            }
        })?;
        Ok(data)
    }

    fn add_df_row(&self, path: PathBuf, data: String) -> Result<(), PyOxenError> {
        let user_id = UserConfig::identifier()?;
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::staging::modify_df(
                &self.repo,
                &self.revision,
                &user_id,
                &path,
                data,
                ContentType::Json,
                ModType::Append,
            )
            .await
        })?;
        Ok(())
    }

    fn restore_df(&self, path: PathBuf) -> Result<(), PyOxenError> {
        let user_id = UserConfig::identifier()?;
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::staging::restore_df(&self.repo, &self.revision, &user_id, &path).await
        })?;
        Ok(())
    }

    fn list_branches(&self) -> Result<Vec<PyBranch>, PyOxenError> {
        let branches = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::remote::branches::list(&self.repo).await })?;
        Ok(branches
            .iter()
            .map(|b| PyBranch::new(b.name.clone(), b.commit_id.clone(), false))
            .collect())
    }

    fn status(&self, path: PathBuf) -> Result<PyStagedData, PyOxenError> {
        let user_id = UserConfig::identifier()?;
        let remote_status = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::staging::status(
                &self.repo,
                &self.revision,
                &user_id,
                &path,
                liboxen::constants::DEFAULT_PAGE_NUM,
                liboxen::constants::DEFAULT_PAGE_SIZE,
            )
            .await
        })?;

        // Convert remote status to a PyStagedData using the from method
        Ok(PyStagedData::from(remote_status))
    }

    fn ls(&self, path: PathBuf, page_num: usize, page_size: usize) -> Result<PyPaginatedDirEntries, PyOxenError> {
        let result = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::dir::list(
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
            api::remote::branches::get_by_name(&self.repo, &branch_name).await
        });

        match branch {
            Ok(Some(branch)) => Ok(PyBranch::from(branch)),
            _ => Err(PyValueError::new_err("could not get branch")),
        }
    }

    fn get_commit(&self, commit_id: String) -> PyResult<PyCommit> {
        let commit = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { api::remote::commits::get_by_id(&self.repo, &commit_id).await });
        match commit {
            Ok(Some(commit)) => Ok(PyCommit { commit }),
            _ => Err(PyValueError::new_err("could not get commit id {commit_id}")),
        }
    }

    fn create_branch(&self, new_name: String) -> PyResult<PyBranch> {
        let branch = pyo3_asyncio::tokio::get_runtime().block_on(async {
            api::remote::branches::create_from_or_get(&self.repo, &new_name, &self.revision).await
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
