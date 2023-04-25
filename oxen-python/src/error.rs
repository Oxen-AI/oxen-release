
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use liboxen::error::OxenError;

pub struct PyOxenError(OxenError);

impl From<PyOxenError> for PyErr {
    fn from(error: PyOxenError) -> Self {
        PyValueError::new_err(error.0.to_string())
    }
}

impl From<OxenError> for PyOxenError {
    fn from(other: OxenError) -> Self {
        Self(other)
    }
}
