//! Oxen Auth tools
//!

use crate::error::PyOxenError;
use liboxen::config::auth_config::AuthConfig;
use pyo3::prelude::*;
use std::path::Path;

#[pyfunction]
pub fn config_auth(host: String, token: String, path: String) -> Result<(), PyOxenError> {
    let final_path = Path::new(&path);
    let mut config = AuthConfig::new(final_path);
    config.add_host_auth_token(host, token);
    config.save(final_path)?;
    Ok(())
}
