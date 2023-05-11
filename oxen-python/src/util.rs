//! Oxen Utils
//!

use pyo3::prelude::*;
use std::path::PathBuf;

use liboxen::util;

/// Checks if a path is tabular
#[pyfunction]
pub fn is_tabular(path: PathBuf) -> PyResult<bool> {
    Ok(util::fs::is_tabular(&path))
}
