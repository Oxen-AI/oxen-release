use liboxen::model::Branch;
use pyo3::prelude::*;

#[pyclass]
pub struct PyBranch {
    _branch: Branch,
}

#[pymethods]
impl PyBranch {
    #[new]
    #[pyo3(signature = (name, commit_id, is_head))]
    pub fn new(name: String, commit_id: String, is_head: bool) -> Self {
        Self {
            _branch: Branch {
                name,
                commit_id,
                is_head,
            },
        }
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self._branch.name
    }

    #[getter]
    pub fn commit_id(&self) -> &str {
        &self._branch.commit_id
    }
}

impl From<Branch> for PyBranch {
    fn from(branch: Branch) -> PyBranch {
        PyBranch { _branch: branch }
    }
}
