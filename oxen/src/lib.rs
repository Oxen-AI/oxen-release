use pyo3::prelude::*;

pub mod branch;
pub mod error;

pub mod py_commit;
pub mod py_remote_repo;
pub mod py_repo;
pub mod py_staged_data;

/// A Python module implemented in Rust.
#[pymodule]
fn oxen(_py: Python, m: &PyModule) -> PyResult<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);

    // Logger was causing deadlock unless we run with py.allow_threads in our
    // bridge I'd rather not do that, because it's less efficient, leaving this
    // commented out just in case you need to do further debugging and
    // enable the rust -> python logging bridge

    // You will need to add py.allow_threads(|| { ... }) around your code to
    // get it to work and ensure there is not a deadlock. In practice we
    // shouldn't be calling back to python to need to grab the GIL again,
    // and can just perform all the heavy work in rust.

    // println! also works to debug, make sure to remove it before committing

    // https://docs.rs/pyo3-log/latest/pyo3_log/#interaction-with-python-gil
    // pyo3_log::init();

    m.add_class::<py_repo::PyRepo>()?;
    m.add_class::<py_remote_repo::PyRemoteRepo>()?;
    m.add_class::<py_staged_data::PyStagedData>()?;
    m.add_class::<py_commit::PyCommit>()?;

    Ok(())
}
