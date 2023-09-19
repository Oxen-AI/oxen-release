use pyo3::prelude::*;

use liboxen::model::Commit as OxenCommit;

// use crate::error::PyOxenError;

#[pyclass]
pub struct PyCommit {
    pub commit: OxenCommit,
}

#[pymethods]
impl PyCommit {
    fn __repr__(&self) -> String {
        format!("PyCommit(id={}, message={}, author={}, email={}, timestamp={}, parent_ids=[{}])", self.commit.id, self.commit.message, self.commit.author, self.commit.email, self.commit.timestamp, self.commit.parent_ids.join(", "))
    }

    fn __str__(&self) -> String {
        self.commit.id.to_owned()
    }

    #[getter]
    pub fn id(&self) -> String {
        self.commit.id.to_string()
    }

    #[getter]
    pub fn commit_id(&self) -> String {
        self.commit.id.to_string()
    }

    #[getter]
    pub fn message(&self) -> String {
        self.commit.message.to_string()
    }

    #[getter]
    pub fn author(&self) -> String {
        self.commit.author.to_string()
    }

    #[getter]
    pub fn email(&self) -> String {
        self.commit.email.to_string()
    }

    #[getter]
    pub fn timestamp(&self) -> String {
        self.commit.timestamp.to_string()
    }

    #[getter]
    pub fn parent_ids(&self) -> Vec<String> {
        self.commit.parent_ids.to_owned()
    }
}
