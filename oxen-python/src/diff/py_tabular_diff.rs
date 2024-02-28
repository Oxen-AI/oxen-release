
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

use liboxen::model::diff::AddRemoveModifyCounts;
use liboxen::model::schema::Schema;

#[pyclass]
pub struct PyTabularDiffMods {
    pub rows: AddRemoveModifyCounts,
}

#[pyclass]
pub struct PyTabularDiffSummary {
    pub modifications: PyTabularDiffMods,
    pub schema: Schema,
}

#[pyclass]
pub struct PyTabularDiff {
    pub summary: PyTabularDiffSummary,
    pub data: PyDataFrame,
}

#[pymethods]
impl PyTabularDiff {
    fn __repr__(&self) -> String {
        let df = self.data.as_ref();
        format!("PyTabularDiff(shape=({},{}))", df.height(), df.width())
    }

    #[getter]
    fn data(&self) -> PyResult<PyDataFrame> {
        Ok(self.data.clone())
    }
}