
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

use liboxen::model::diff::TextDiffSummary;
use liboxen::model::diff::TextDiff;


#[pyclass]
pub struct PyTextDiff {
    pub summary: TextDiffSummary,
    pub contents: TextDiff,
}

#[pymethods]
impl PyTextDiff {
    fn __repr__(&self) -> String {
        let df = self.data.as_ref();
        format!("PyTextDiff")
    }

    #[getter]
    fn data(&self) -> PyResult<PyDataFrame> {
        Ok(self.data.clone())
    }
}