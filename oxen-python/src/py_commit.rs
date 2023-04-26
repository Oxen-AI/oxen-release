use pyo3::prelude::*;

use liboxen::model::Commit as OxenCommit;

// use crate::error::PyOxenError;

#[pyclass]
pub struct PyCommit {
    pub commit: OxenCommit,
}

#[pymethods]
impl PyCommit {
    pub fn commit_id(&self) -> PyResult<String> {
        Ok(self.commit.id.to_string())
    }
}
