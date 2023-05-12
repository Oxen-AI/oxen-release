//! Oxen Utils
//!

use pyo3::prelude::*;
use std::path::PathBuf;
use pyo3_polars::PyDataFrame;

use crate::error::PyOxenError;

use liboxen::{util, opts::DFOpts};

/// Checks if a path is tabular
#[pyfunction]
pub fn is_tabular(path: PathBuf) -> PyResult<bool> {
    Ok(util::fs::is_tabular(&path))
}

/// Checks if a path is tabular
#[pyfunction]
pub fn read_df(path: PathBuf) -> Result<PyDataFrame, PyOxenError> {
    let opts = DFOpts::empty();
    let df = liboxen::core::df::tabular::read_df(path, opts)?;
    Ok(PyDataFrame(df))
}

