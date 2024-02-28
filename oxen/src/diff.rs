
use pyo3::prelude::*;
use std::path::PathBuf;

use liboxen::command;
use liboxen::view::compare::CompareResult;
use liboxen::error::OxenError;
use crate::error::PyOxenError;

use pyo3_polars::PyDataFrame;

#[pyfunction]
pub fn diff_tabular(
    left: PathBuf,
    right: PathBuf,
    keys: Vec<String>,
    targets: Vec<String>,
) -> Result<PyDataFrame, PyOxenError> {
    let displays: Vec<String> = vec![];
    let result = command::diff::diff_tabular(&left, &right, keys, targets, displays)?;

    match result {
        CompareResult::Tabular((_, df)) => {
            Ok(PyDataFrame(df))
        }
        CompareResult::Text(_) => {
            Err(PyOxenError::from(OxenError::basic_str("Text files are not supported")))
        }
    }
}