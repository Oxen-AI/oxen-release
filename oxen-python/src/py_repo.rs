
use liboxen::model::LocalRepository;
use pyo3::prelude::*;

use liboxen::command;

use std::path::PathBuf;

use crate::error::PyOxenError;
use crate::py_staged_data::PyStagedData;
use crate::py_commit::PyCommit;

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

    pub fn status(&self) -> Result<PyStagedData, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let status = command::status(&repo)?;
        Ok(PyStagedData { data: status })
    }

    pub fn commit(&self, message: &str) -> Result<PyCommit, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let commit = command::commit(&repo, message)?;
        Ok(PyCommit { commit: commit })
    }

    pub fn log(&self) -> Result<Vec<PyCommit>, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let log = command::log(&repo)?;
        Ok(log.iter().map(|c| PyCommit { commit: c.clone() }).collect())
    }
}