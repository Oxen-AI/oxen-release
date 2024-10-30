use std::path::PathBuf;
use liboxen::api;
use liboxen::error::OxenError;
use liboxen::opts::DFOpts;
use pyo3::prelude::*;

use crate::py_remote_repo::PyRemoteRepo;
use crate::error::PyOxenError;

#[pyclass]
pub struct PyRemoteDataFrame {
    repo: PyRemoteRepo,
    path: PathBuf,
}

#[pymethods]
impl PyRemoteDataFrame {
    #[new]
    #[pyo3(signature = (repo, path))]
    fn new(repo: PyRemoteRepo, path: String) -> Result<Self, PyOxenError> {
        Ok(Self { repo, path: PathBuf::from(path) })
    }

    fn size(&self) -> Result<(usize, usize), PyOxenError> {
        pyo3_asyncio::tokio::get_runtime().block_on(async {
            let mut opts = DFOpts::empty();
            opts.slice = Some("0..1".to_string());

            let response = api::client::data_frames::get(
                &self.repo.repo,
                &self.repo.revision,
                &self.path,
                DFOpts::empty(),
            )
            .await?;

            Ok((response.data_frame.source.size.width, response.data_frame.source.size.height))
        })
    }

    fn get_row_by_index(&self, row: usize) -> Result<String, PyOxenError> {
        let data = pyo3_asyncio::tokio::get_runtime().block_on(async {
            let mut opts = DFOpts::empty();
            opts.slice = Some(format!("{}..{}", row, row+1));

            let response = api::client::data_frames::get(
                &self.repo.repo,
                &self.repo.revision,
                &self.path,
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

    fn get_slice(
        &self,
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

            let response = api::client::data_frames::get(
                &self.repo.repo,
                &self.repo.revision,
                &self.path,
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
}