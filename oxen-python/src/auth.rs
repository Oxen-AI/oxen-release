//! Oxen Auth tools
//!

use crate::error::PyOxenError;
use liboxen::config::user_config::UserConfig;
use liboxen::model::User;
use liboxen::util::fs::oxen_home_dir;
use pyo3::prelude::*;
use std::path::{Path, PathBuf};

#[pyfunction]
pub fn create_user_config(name: String, email: String, path: String) -> Result<(), PyOxenError> {
    let final_path = Path::new(&path);
    let user = User { name, email };
    let config = UserConfig::from_user(&user);
    config.save(final_path)?;
    Ok(())
}

#[pyfunction]
pub fn add_host_auth(host: String, token: String, path: String) -> Result<(), PyOxenError> {
    let final_path = Path::new(&path);
    let mut config = UserConfig::new(final_path);
    config.add_host_auth_token(host, token);
    config.save(final_path)?;
    Ok(())
}

#[pyfunction]
pub fn get_oxen_home_dir() -> Result<PathBuf, PyOxenError> {
    let path = oxen_home_dir()?;
    Ok(path)
}
