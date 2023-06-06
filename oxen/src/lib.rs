use pyo3::prelude::*;

pub mod error;
pub mod py_branch;

pub mod auth;
pub mod py_commit;
pub mod py_dataset;
pub mod py_local_repo;
pub mod py_remote_repo;
pub mod py_staged_data;
pub mod util;

/// A Python module implemented in Rust.
#[pymodule]
fn oxen(py: Python, m: &PyModule) -> PyResult<()> {
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

    m.add_class::<py_local_repo::PyLocalRepo>()?;
    m.add_class::<py_branch::PyBranch>()?;
    m.add_class::<py_remote_repo::PyRemoteRepo>()?;
    m.add_class::<py_dataset::PyDataset>()?;
    m.add_class::<py_staged_data::PyStagedData>()?;
    m.add_class::<py_commit::PyCommit>()?;

    // Util Module
    let util_module = PyModule::new(py, "util")?;
    util_module.add_function(wrap_pyfunction!(util::is_tabular, util_module)?)?;
    util_module.add_function(wrap_pyfunction!(util::read_df, util_module)?)?;
    m.add_submodule(util_module)?;

    // Auth Module
    let auth_module = PyModule::new(py, "auth")?;
    auth_module.add_function(wrap_pyfunction!(auth::get_oxen_home_dir, auth_module)?)?;
    auth_module.add_function(wrap_pyfunction!(auth::add_host_auth, auth_module)?)?;
    auth_module.add_function(wrap_pyfunction!(auth::create_user_config, auth_module)?)?;
    m.add_submodule(auth_module)?;
    Ok(())
}
