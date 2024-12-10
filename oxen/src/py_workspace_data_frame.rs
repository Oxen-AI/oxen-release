use std::path::{PathBuf, Path};
use liboxen::api;
use liboxen::config::UserConfig;
use liboxen::error::OxenError;
use liboxen::model::{NewCommitBody, RemoteRepository};
use liboxen::opts::DFOpts;
use liboxen::view::JsonDataFrameViews;
use pyo3::prelude::*;

use crate::py_commit::PyCommit;
use crate::py_workspace::PyWorkspace;
use crate::error::PyOxenError;

#[pyfunction]
pub fn is_indexed(workspace: PyWorkspace, workspace_id: String, path: PathBuf) -> Result<bool, PyOxenError> {
    let repo = workspace.repo.repo;
    let data = pyo3_asyncio::tokio::get_runtime()
        .block_on(async {
            api::client::workspaces::data_frames::is_indexed(
                &repo,
                &workspace_id,
                &path
            ).await 
        })?;
    Ok(data)
}

#[pyfunction]
pub fn index(workspace: PyWorkspace, workspace_id: String, path: PathBuf) -> Result<(), PyOxenError> {
    let repo = workspace.repo.repo;
    pyo3_asyncio::tokio::get_runtime()
        .block_on(async {
            api::client::workspaces::data_frames::index(
                &repo,
                &workspace_id,
                &path
            ).await 
        })?;
    Ok(())
}

fn _get(
    repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<JsonDataFrameViews, PyOxenError> {
    let path = path.as_ref();
    let opts = DFOpts::empty();

    let data = pyo3_asyncio::tokio::get_runtime()
        .block_on(async {
            api::client::workspaces::data_frames::get(repo, &workspace_id, path, &opts).await 
        })?;

    let Some(data_frame) = data.data_frame else {
        return Err(OxenError::basic_str(format!("Failed to get data frame for path: {:?}", path)).into())
    };

    Ok(data_frame)
}

#[pyclass]
pub struct PyWorkspaceDataFrame {
    workspace: PyWorkspace,
    path: PathBuf,
    _first_page: JsonDataFrameViews,
}

#[pymethods]
impl PyWorkspaceDataFrame {
    #[new]
    #[pyo3(signature = (workspace, path))]
    fn new(workspace: PyWorkspace, path: PathBuf) -> Result<Self, PyOxenError> {
        // Index the data frame (will simply return if already indexed)
        index(workspace.clone(), workspace.id.clone(), path.clone())?;

        // Fetch the first page so that it is 
        // quick to look up size and other pagination params
        let df = _get(&workspace.repo.repo, &workspace.id, &path)?;
        Ok(Self { workspace, path, _first_page: df })
    }

    fn size(&mut self) -> Result<(usize, usize), PyOxenError> {
        let df = _get(&self.workspace.repo.repo, &self.workspace.id, &self.path)?;
        let size = &df.view.size;
        let width = size.width;
        let height = size.height;
        self._first_page = df;
        Ok((width, height))
    }

    fn page_size(&self) -> usize {
        self._first_page.view.pagination.page_size
    }

    fn total_pages(&self) -> usize {
        self._first_page.view.pagination.total_pages
    }

    pub fn is_indexed(&self) -> Result<bool, PyOxenError> {
        let is_indexed = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { 
                api::client::workspaces::data_frames::is_indexed(
                    &self.workspace.repo.repo,
                    &self.workspace.id,
                    &self.path,
                )
                .await 
            })?;
        Ok(is_indexed)
    }

    pub fn index(&self) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime()
            .block_on(async { 
                api::client::workspaces::data_frames::index(
                    &self.workspace.repo.repo,
                    &self.workspace.id,
                    &self.path,
                )
                .await
            })?;

        Ok(())
    }

    fn list(&self, page: Option<usize>) -> Result<String, PyOxenError> {
        let mut opts = DFOpts::empty();
        opts.page = page;

        let data = pyo3_asyncio::tokio::get_runtime()
            .block_on(async {
                api::client::workspaces::data_frames::get(
                    &self.workspace.repo.repo,
                    &self.workspace.id,
                    &self.path,
                    &opts
                ).await 
            })?;
        
        // Extract the serde_json::Value from the JsonDataFrameView
        let view = data.data_frame.unwrap().view.data;

        // convert json to String
        let result: String = serde_json::to_string(&view).unwrap();
        Ok(result)
    }

    fn get_row_by_id(&self, id: String) -> Result<String, PyOxenError> {
        let data = pyo3_asyncio::tokio::get_runtime()
            .block_on(async {
                api::client::workspaces::data_frames::rows::get(
                    &self.workspace.repo.repo,
                    &self.workspace.id,
                    &self.path,
                    &id.as_str(),
                )
                .await
            })?;
        let view = data.data_frame.view.data;

        // convert json to String
        let result: String = serde_json::to_string(&view).unwrap();
        Ok(result)
    }

    fn insert_row(&self, data: String) -> Result<String, PyOxenError> {
        let Ok(_) = serde_json::from_str::<serde_json::Value>(&data) else {
            return Err(OxenError::basic_str(format!("Failed to parse json data: {}", data)).into())
        };

        let (_, Some(row_id)) = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { 
                api::client::workspaces::data_frames::rows::add(
                    &self.workspace.repo.repo,
                    &self.workspace.id,
                    &self.path,
                    data.to_string(),
                )
                .await
            })? else {
                return Err(OxenError::basic_str("Failed to insert data").into())
            };

        Ok(row_id)
    }

    fn update_row(&self, id: String, data: String) -> Result<String, PyOxenError> {
        let Ok(_) = serde_json::from_str::<serde_json::Value>(&data) else {
            return Err(OxenError::basic_str(format!("Failed to parse json data: {}", data)).into())
        };

        let view = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { 
                api::client::workspaces::data_frames::rows::update(
                    &self.workspace.repo.repo,
                    &self.workspace.id,
                    &self.path,
                    &id.as_str(),
                    data,
                )
                .await
            })?;
        let view = view.data_frame.view.data;

        let result: String = serde_json::to_string(&view).unwrap();
        Ok(result)
    }

    fn delete_row(&self, id: String) -> Result<(), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime()
            .block_on(async {
                api::client::workspaces::data_frames::rows::delete(
                    &self.workspace.repo.repo,
                    &self.workspace.id,
                    &self.path,
                    &id.as_str(),
                )
                .await
            })?;
        Ok(())
    }

    fn restore(&self) -> Result<(), PyOxenError> {
        let repo = &self.workspace.repo.repo;

        pyo3_asyncio::tokio::get_runtime()
            .block_on(async {
                api::client::workspaces::data_frames::restore(
                    repo,
                    &self.workspace.id,
                    &self.path
                ).await
            })?;

        Ok(())
    }

    fn commit(&self, branch: &str, message: &str) -> Result<PyCommit, PyOxenError> {
        let user = UserConfig::get()?;
        let repo = &self.workspace.repo.repo;

        let commit = NewCommitBody {
            message: message.to_string(),
            author: user.name,
            email: user.email,
        };

        let commit = pyo3_asyncio::tokio::get_runtime()
            .block_on(async {
                api::client::workspaces::commit(
                    repo,
                    branch,
                    &self.workspace.id,
                    &commit,
                ).await
            })?;
        Ok(commit.into())
    }
}
