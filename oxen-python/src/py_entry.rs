use pyo3::prelude::*;

use liboxen::model::MetadataEntry;

#[pyclass]
pub struct PyEntry {
    _entry: MetadataEntry,
}

#[pymethods]
impl PyEntry {
    fn __repr__(&self) -> String {
        self._entry.filename.to_owned()
    }

    fn __str__(&self) -> String {
        self._entry.filename.to_owned()
    }

    #[getter]
    pub fn filename(&self) -> &str {
        &self._entry.filename
    }

    #[getter]
    pub fn data_type(&self) -> String {
        self._entry.data_type.to_string()
    }

    #[getter]
    pub fn mime_type(&self) -> &str {
        &self._entry.mime_type
    }

    #[getter]
    pub fn size(&self) -> u64 {
        self._entry.size
    }
}

impl From<MetadataEntry> for PyEntry {
    fn from(entry: MetadataEntry) -> PyEntry {
        PyEntry { _entry: entry }
    }
}