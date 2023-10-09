use pyo3::prelude::*;

use liboxen::model::diff::generic_diff::GenericDiff;

#[pyclass]
pub struct PyDiff {
    pub diff: GenericDiff,
}

#[pymethods]
impl PyDiff {
    fn __repr__(&self) -> String {
        format!("PyDiff(type={})", self.get_type())
    }

    #[getter]
    pub fn get_type(&self) -> String {
        match &self.diff {
            GenericDiff::DirDiff(_diff) => {
                "dir".to_string()
            },
            GenericDiff::TabularDiff(_diff) => {
                "tabular".to_string()
            },
            // GenericDiff::TextDiff(_diff) => {
            //     "text".to_string()
            // },
        }
    }
}