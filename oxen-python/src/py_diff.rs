use liboxen::error::OxenError;
use pyo3::prelude::*;

use liboxen::model::diff::DiffResult;

use crate::diff::PyTabularDiff;
use crate::diff::PyTextDiff;
use crate::error::PyOxenError;

#[pyclass]
pub struct PyDiff {
    pub diff: DiffResult,
}

#[pymethods]
impl PyDiff {
    fn __repr__(&self) -> String {
        format!("PyDiff(format={:?})", self.format())
    }

    #[getter]
    pub fn format(&self) -> String {
        match &self.diff {
            DiffResult::Tabular(_diff) => "tabular".to_string(),
            DiffResult::Text(_diff) => "text".to_string(),
        }
    }

    #[getter]
    pub fn tabular(&self) -> Result<PyTabularDiff, PyOxenError> {
        match &self.diff {
            DiffResult::Tabular(diff) => Ok(PyTabularDiff::from(diff)),
            _ => Err(OxenError::basic_str("Diff is not tabular").into()),
        }
    }

    #[getter]
    pub fn text(&self) -> Result<PyTextDiff, PyOxenError> {
        match &self.diff {
            DiffResult::Text(diff) => Ok(PyTextDiff::from(diff)),
            _ => Err(OxenError::basic_str("Diff is not text").into()),
        }
    }
}
