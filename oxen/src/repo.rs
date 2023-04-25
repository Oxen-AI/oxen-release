
use liboxen::model::LocalRepository;
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;

use liboxen::command;

use std::path::PathBuf;

use crate::error::PyOxenError;

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

    pub fn init(&self) -> PyResult<()> {
        match command::init(&self.path) {
            Ok(_) => {
                log::info!("Success!");
                Ok(())
            },
            Err(err) => {
                log::error!("Error: {}", err);
                Err(PyValueError::new_err("could not init repo"))
            }
        }
    }

    pub fn add(&self, path: PathBuf) -> Result<(), PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        command::add(&repo, path)?;
        Ok(())
    }
}