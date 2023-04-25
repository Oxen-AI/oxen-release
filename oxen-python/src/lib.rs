use pyo3::prelude::*;

pub mod clone;
pub mod error;
pub mod repo;
pub mod branch;

/// A Python module implemented in Rust.
#[pymodule]
fn oxen(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);

    m.add_class::<repo::PyRepo>()?;

    m.add_function(wrap_pyfunction!(clone::clone, m)?)?;
    Ok(())
}

// fn register_repos_module(py: Python<'_>, parent_module: &PyModule) -> PyResult<()> {
//     let child_module = PyModule::new(py, "repos")?;
//     child_module.add_function(wrap_pyfunction!(func, child_module)?)?;
//     parent_module.add_submodule(child_module)?;
//     Ok(())
// }