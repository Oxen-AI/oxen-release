use pyo3::prelude::*;

#[pyclass]
pub struct PyBranch {
    pub name: String,
    pub commit_id: String,
}

