use pyo3::prelude::*;

use liboxen::view::PaginatedDirEntries;

use crate::py_entry::PyEntry;

#[pyclass]
pub struct PyPaginatedDirEntries {
    _entries: PaginatedDirEntries,
}

#[pymethods]
impl PyPaginatedDirEntries {
    fn __repr__(&self) -> String {
        format!("PaginatedDirEntries(page_size={}, page_number={}, total_pages={}, total_entries={})", self._entries.page_size, self._entries.page_number, self._entries.total_pages, self._entries.total_entries)
    }

    fn __str__(&self) -> String {
        let result: String = self._entries.entries
            .iter().map(|e| {
                if e.is_dir {
                    format!("{}/", e.filename)
                } else {
                    format!("{}", e.filename)
                }
            })
            .collect::<Vec<String>>()
            .join("\n");
        result
    }

    #[getter]
    pub fn page_size(&self) -> usize {
        self._entries.page_size
    }

    #[getter]
    pub fn page_number(&self) -> usize {
        self._entries.page_number
    }

    #[getter]
    pub fn total_pages(&self) -> usize {
        self._entries.total_pages
    }

    #[getter]
    pub fn total_entries(&self) -> usize {
        self._entries.total_entries
    }

    #[getter]
    pub fn entries(&self) -> Vec<PyEntry> {
        self._entries.entries
            .iter()
            .map(|e| PyEntry::from(e.to_owned()))
            .collect()
    }
}

impl From<PaginatedDirEntries> for PyPaginatedDirEntries {
    fn from(entries: PaginatedDirEntries) -> PyPaginatedDirEntries {
        PyPaginatedDirEntries { _entries: entries }
    }
}