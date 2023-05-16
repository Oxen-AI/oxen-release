//! Configuration for Oxen, including user configuration and remote host configuration
//!

pub mod endpoint;
pub mod user_config;

pub use crate::config::user_config::UserConfig;
pub use crate::config::user_config::USER_CONFIG_FILENAME;
