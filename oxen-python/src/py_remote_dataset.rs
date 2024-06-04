use std::path::{PathBuf, Path};
use liboxen::api;
use liboxen::config::UserConfig;
use liboxen::error::OxenError;
use liboxen::model::{NewCommitBody, RemoteRepository};
use liboxen::opts::DFOpts;
use liboxen::view::JsonDataFrameViews;
use pyo3::prelude::*;

use crate::py_commit::PyCommit;
use crate::py_remote_repo::PyRemoteRepo;
use crate::error::PyOxenError;

#[pyfunction]
pub fn index_dataset(repo: PyRemoteRepo, path: PathBuf) -> Result<(), PyOxenError> {
    let revision = repo.revision;
    let repo = repo.repo;
    let user_id = UserConfig::identifier()?;

    pyo3_asyncio::tokio::get_runtime()
        .block_on(async { 
            api::remote::staging::index_dataset(
                &repo,
                &revision,
                &user_id,
                &path,
            )
            .await
        })?;

    Ok(())
}

fn _get_df(
    repo: &RemoteRepository,
    revision: impl AsRef<str>,
    user_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<JsonDataFrameViews, PyOxenError> {
    let revision = revision.as_ref();
    let user_id = user_id.as_ref();
    let path = path.as_ref();

    let opts = DFOpts::empty();

    let data = pyo3_asyncio::tokio::get_runtime()
        .block_on(async {
            api::remote::df::get_staged(repo, &revision, &user_id, path, opts).await 
        })?;
    Ok(data.data_frame)
}

#[pyclass]
pub struct PyRemoteDataset {
    repo: PyRemoteRepo,
    path: PathBuf,
    _first_page: JsonDataFrameViews,
}

#[pymethods]
impl PyRemoteDataset {
    #[new]
    #[pyo3(signature = (repo, path))]
    fn new(repo: PyRemoteRepo, path: PathBuf) -> PyResult<Self> {
        let revision = &repo.revision;
        let Ok(user_id) = UserConfig::identifier() else {
            panic!("User ID not found");
        };

        // Fetch the first page so that it is 
        // quick to look up size and other pagination params
        let df = _get_df(&repo.repo, revision, &user_id, &path)?;
        Ok(Self { repo, path, _first_page: df })
    }

    fn size(&self) -> Result<(usize, usize), PyOxenError> {
        let size = &self._first_page.view.size;
        let width = size.width;
        let height = size.height;

        Ok((width, height))
    }

    fn page_size(&self) -> usize {
        self._first_page.view.pagination.page_size
    }

    fn total_pages(&self) -> usize {
        self._first_page.view.pagination.total_pages
    }

    fn list(&self, page: Option<usize>) -> Result<String, PyOxenError> {
        let revision = &self.repo.revision;
        let mut opts = DFOpts::empty();
        opts.page = page;

        let user_id = UserConfig::identifier()?;

        let data = pyo3_asyncio::tokio::get_runtime()
            .block_on(async {
                api::remote::df::get_staged(&self.repo.repo, revision, &user_id, &self.path, opts).await 
            })?;
        
        // Extract the serde_json::Value from the JsonDataFrameView
        let view = data.data_frame.view.data;

        // convert json to String
        let result: String = serde_json::to_string(&view).unwrap();
        Ok(result)
    }

    fn get_row_by_id(&self, id: String) -> Result<String, PyOxenError> {
        let user_id = UserConfig::identifier()?;

        let data = pyo3_asyncio::tokio::get_runtime()
            .block_on(async {
                api::remote::staging::get_row(
                    &self.repo.repo,
                    &self.repo.revision,
                    &user_id,
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
        let user_id = UserConfig::identifier()?;

        let (_, Some(row_id)) = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { 
                api::remote::staging::modify_df(
                    &self.repo.repo,
                    &self.repo.revision,
                    &user_id,
                    &self.path,
                    data,
                    liboxen::model::ContentType::Json,
                    liboxen::model::entry::mod_entry::ModType::Append,
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

        let user_id = UserConfig::identifier()?;
        let view = pyo3_asyncio::tokio::get_runtime()
            .block_on(async { 
                api::remote::staging::modify_df::update_row(
                    &self.repo.repo,
                    &self.repo.revision,
                    &user_id,
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
        let user_id = UserConfig::identifier()?;
        pyo3_asyncio::tokio::get_runtime()
            .block_on(async {
                api::remote::staging::modify_df::delete_row(
                    &self.repo.repo,
                    &self.repo.revision,
                    &user_id,
                    &self.path,
                    &id.as_str(),
                )
                .await
            })?;
        Ok(())
    }

    fn restore(&self) -> Result<(), PyOxenError> {
        let user_id = UserConfig::identifier()?;
        let repo = &self.repo.repo;
        let revision = &self.repo.revision;

        pyo3_asyncio::tokio::get_runtime()
            .block_on(async {
                api::remote::staging::restore_df(repo, revision, &user_id, &self.path).await
            })?;

        Ok(())
    }

    fn commit(&self, message: &str) -> Result<PyCommit, PyOxenError> {
        let user = UserConfig::get()?;
        let user_id = UserConfig::identifier()?;
        let repo = &self.repo.repo;
        let revision = &self.repo.revision;
        // convert path to linux style
        let path = &self.path.to_string_lossy();
        let path = path.replace("\\", "/");

        let commit = NewCommitBody {
            message: message.to_string(),
            author: user.name,
            email: user.email,
        };

        let commit = pyo3_asyncio::tokio::get_runtime()
            .block_on(async {
                api::remote::staging::commit_file(repo, revision, &user_id, &commit, &path).await
            })?;
        Ok(commit.into())
    }
}
