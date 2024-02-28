
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
    pub contents: PyDataFrame,
}

#[pymethods]
impl PyTabularDiff {
    fn __repr__(&self) -> String {
        let df = self.contents.as_ref();
        format!("PyTabularDiff(shape=({},{}))\n{:?}", df.height(), df.width(), df)
    }

    #[getter]
    fn contents(&self) -> PyResult<PyDataFrame> {
        Ok(self.contents.clone())
    }
}