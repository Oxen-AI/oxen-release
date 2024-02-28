
use pyo3::prelude::*;
use std::path::PathBuf;

use liboxen::command;
use liboxen::model::schema::Schema;
use liboxen::model::diff::AddRemoveModifyCounts;
use liboxen::view::compare::CompareResult;
use liboxen::error::OxenError;
use crate::error::PyOxenError;

use pyo3_polars::PyDataFrame;

pub mod py_tabular_diff;
pub use py_tabular_diff::{PyTabularDiff, PyTabularDiffSummary, PyTabularDiffMods};

#[pyfunction]
pub fn diff_tabular(
    left: PathBuf,
    right: PathBuf,
    keys: Vec<String>,
    targets: Vec<String>,
) -> Result<PyTabularDiff, PyOxenError> {
    let displays: Vec<String> = vec![];
    let result = command::diff::diff_tabular(&left, &right, keys, targets, displays)?;

    // TODO: This should be a DiffResult or GenericDiff or something 
    // - get rid of the references to "compare" in the codebase
    match result {
        CompareResult::Tabular((ct, df)) => {
            let summary = ct.summary.unwrap();
            let rows = AddRemoveModifyCounts {
                added: summary.modifications.added_rows,
                removed: summary.modifications.removed_rows,
                modified: summary.modifications.modified_rows,
            };
            let mods = PyTabularDiffMods {
                rows,
            };
            let summary = PyTabularDiffSummary {
                modifications: mods,
                schema: Schema::from_polars(&df.schema()),
            };
            let data = PyDataFrame(df);
            Ok(PyTabularDiff { summary, data })
        }
        CompareResult::Text(_) => {
            Err(PyOxenError::from(OxenError::basic_str("Text files are not supported")))
        }
    }
}