


use pyo3::prelude::*;
use std::path::PathBuf;

use liboxen::{core::df::tabular, opts::DFOpts};
use crate::error::PyOxenError;

use pyo3_polars::PyDataFrame;

#[pyfunction]
pub fn load(
    path: PathBuf
) -> Result<PyDataFrame, PyOxenError> {
    let opts = DFOpts::empty();
    let df = tabular::read_df(path, opts)?;
    Ok(PyDataFrame(df))
}

#[pyfunction]
pub fn save(
    df: PyDataFrame,
    path: PathBuf
) -> Result<(), PyOxenError> {
    let mut df = df.as_ref().clone();
    tabular::write_df(&mut df, path)?;
    Ok(())
}