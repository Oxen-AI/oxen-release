use std::path::PathBuf;

use error::PyOxenError;
use liboxen::{config::UserConfig, model::{repository::local_repository::FileNew, RepositoryNew}};
use py_remote_repo::PyRemoteRepo;
use pyo3::prelude::*;

pub mod error;
pub mod py_branch;

pub mod auth;
pub mod py_commit;
pub mod py_dataset;
pub mod py_diff;
pub mod py_entry;
pub mod py_local_repo;
pub mod py_remote_repo;
pub mod py_paginated_dir_entries;
pub mod py_staged_data;
pub mod py_user;
pub mod user;
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

    m.add_class::<py_branch::PyBranch>()?;
    m.add_class::<py_commit::PyCommit>()?;
    m.add_class::<py_dataset::PyDataset>()?;
    m.add_class::<py_diff::PyDiff>()?;
    m.add_class::<py_local_repo::PyLocalRepo>()?;
    m.add_class::<py_remote_repo::PyRemoteRepo>()?;
    m.add_class::<py_staged_data::PyStagedData>()?;
    m.add_class::<py_user::PyUser>()?;

    // Util Module
    let util_module = PyModule::new(py, "util")?;
    util_module.add_function(wrap_pyfunction!(util::is_tabular, util_module)?)?;
    util_module.add_function(wrap_pyfunction!(util::read_df, util_module)?)?;
    util_module.add_function(wrap_pyfunction!(util::get_oxen_config_dir, util_module)?)?;
    m.add_submodule(util_module)?;

    // Auth Module
    let auth_module = PyModule::new(py, "auth")?;
    auth_module.add_function(wrap_pyfunction!(auth::config_auth, auth_module)?)?;
    m.add_submodule(auth_module)?;

    // User Module
    let user_module = PyModule::new(py, "user")?;
    user_module.add_function(wrap_pyfunction!(user::config_user, user_module)?)?;
    user_module.add_function(wrap_pyfunction!(user::current_user, user_module)?)?;
    m.add_submodule(user_module)?;

    Ok(())
}

// TODO: be able to pass in file list and contents from python
#[pyfunction]
pub fn create_remote_repo(namespace: String, name: String, host: String) -> Result<PyRemoteRepo, PyOxenError> {
    let result = pyo3_asyncio::tokio::get_runtime().block_on(async {
        let config = UserConfig::get()?;
        let user = config.to_user();
        let files: Vec<FileNew> = vec![FileNew {
            path: PathBuf::from("README.md"),
            contents: format!("# {}\n", &name),
        }];
        let repo = RepositoryNew::from_files(&namespace, &name, files, user);
        liboxen::api::remote::repositories::create(repo, &host).await
    })?;
    Ok(PyRemoteRepo {
        repo: result.clone(),
        host: host.clone(),
        revision: "main".to_string(),
    })
}
