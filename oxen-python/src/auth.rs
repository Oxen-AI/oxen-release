//! Oxen Auth tools
//!

use crate::error::PyOxenError;
use liboxen::config::user_config::UserConfig;
use pyo3::prelude::*;
use std::path::Path;

#[pyfunction]
pub fn config_auth(host: String, token: String, path: String) -> Result<(), PyOxenError> {
    let final_path = Path::new(&path);
    let mut config = UserConfig::new(final_path);
    config.add_host_auth_token(host, token);
    config.save(final_path)?;
    Ok(())
}
