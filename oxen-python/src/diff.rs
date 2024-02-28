
use pyo3::prelude::*;
use std::path::PathBuf;

use liboxen::command;
use liboxen::model::schema::Schema;
use liboxen::model::diff::AddRemoveModifyCounts;
use liboxen::model::diff::DiffResult;
use liboxen::error::OxenError;
use crate::error::PyOxenError;

use pyo3_polars::PyDataFrame;

pub mod py_tabular_diff;
pub use py_tabular_diff::{PyTabularDiff, PyTabularDiffSummary, PyTabularDiffMods};

#[pyfunction]
pub fn diff_paths(
    path_1: PathBuf,
    keys: Vec<String>,
    targets: Vec<String>,
    path_2: Option<PathBuf>,
    repo_dir: Option<PathBuf>,
    revision_1: Option<String>,
    revision_2: Option<String>
) -> Result<PyTabularDiff, PyOxenError> {
    let result = command::diff(
        path_1,
        path_2,
        keys,
        targets,
        repo_dir,
        revision_1,
        revision_2
    )?;

    // TODO: This should be a DiffResult or GenericDiff or something
    // - get rid of the references to "compare" in the codebase
    match result {
        DiffResult::Tabular(result) => {
            let df = result.contents;
            let summary = result.summary;
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
            let contents = PyDataFrame(df);
            Ok(PyTabularDiff { summary, contents })
        }
        DiffResult::Text(_) => {
            Err(PyOxenError::from(OxenError::basic_str("Text files are not supported")))
        }
    }
}