use pyo3::prelude::*;

use liboxen::model::MetadataEntry;

#[pyclass]
pub struct PyEntry {
    _entry: MetadataEntry,
}

#[pymethods]
impl PyEntry {
    fn __repr__(&self) -> String {
        format!("PyEntry(filename={}, is_dir={} data_type={}, mime_type={}, size={})", self._entry.filename, self._entry.is_dir, self._entry.data_type, self._entry.mime_type, self._entry.size)
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