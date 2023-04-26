
use liboxen::model::LocalRepository;
use pyo3::prelude::*;

use liboxen::command;

use std::path::PathBuf;

use crate::error::PyOxenError;
use crate::py_staged_data::StagedData;

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

    pub fn init(&self) -> Result<(), PyOxenError> {
        command::init(&self.path)?;
        Ok(())
    }

    pub fn add(&self, path: PathBuf) -> Result<(), PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        command::add(&repo, path)?;
        Ok(())
    }

    pub fn status(&self) -> Result<StagedData, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let status = command::status(&repo)?;
        Ok(StagedData { data: status })
    }
}