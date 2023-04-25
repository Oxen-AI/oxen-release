
use pyo3::prelude::*;

use liboxen::api;
use liboxen::model::{RemoteRepository, Remote};
use pyo3::exceptions::PyValueError;

use crate::branches::PyBranch;

#[pyclass]
pub struct PyRemoteRepo {
    namespace: String,
    repo_name: String,
    url: String
}

#[pymethods]
impl PyRemoteRepo {
    #[new]
    #[pyo3(signature = (repo, host=String::from("hub.oxen.ai"), use_ssl=true))]
    fn py_new(repo: String, host: String, use_ssl: bool) -> PyResult<Self> {

        let (namespace, repo_name) = match repo.split_once('/') {
            Some((namespace, repo_name)) => (namespace.to_string(), repo_name.to_string()),
            None => return Err(PyValueError::new_err("Invalid repo name")),
        };

        let url = if use_ssl {
            format!("https://{}/{}", host, repo)
        } else {
            format!("http://{}/{}", host, repo)
        };
        Ok(Self { namespace, repo_name, url })
    }

    fn get_branch(&self, branch_name: String) -> PyResult<PyBranch> {
        log::info!("Get branch... {branch_name}");

        let branch = pyo3_asyncio::tokio::get_runtime().block_on(async {
            
            let repo = RemoteRepository {
                namespace: self.namespace.clone(),
                name: self.repo_name.clone(),
                remote: Remote {
                    url: self.url.clone(),
                    name: String::from("origin"),
                }
            };

            log::info!("From repo... {}", repo.remote.url);
            api::remote::branches::get_by_name(&repo, &branch_name).await
        });

        match branch {
            Ok(Some(branch)) => Ok(PyBranch { name: branch.name, commit_id: branch.commit_id }),
            _ => Err(PyValueError::new_err("could not get branch")),
        }
    }
}