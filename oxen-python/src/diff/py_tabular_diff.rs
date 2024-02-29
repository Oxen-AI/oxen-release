
use liboxen::model::diff::tabular_diff::TabularDiff;
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

impl From<&TabularDiff> for PyTabularDiff {
    fn from(other: &TabularDiff) -> Self {
        let df = &other.contents;
        let summary = &other.summary;
        let rows = AddRemoveModifyCounts {
            added: summary.modifications.row_counts.added,
            removed: summary.modifications.row_counts.removed,
            modified: summary.modifications.row_counts.modified,
        };
        let mods = PyTabularDiffMods {
            rows,
        };
        let summary = PyTabularDiffSummary {
            modifications: mods,
            schema: Schema::from_polars(&df.schema()),
        };
        let contents = PyDataFrame(df.clone());
        Self { summary, contents }
    }
}

impl From<TabularDiff> for PyTabularDiff {
    fn from(other: TabularDiff) -> Self {
        Self::from(&other)
    }
}
