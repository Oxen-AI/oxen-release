//! liboxen
//!
//! Library to interact with Oxen Repositories
//! - `oxen init .`
//! - `oxen status`
//! - `oxen add <file>`
//! - `oxen commit -m "my first commit"`
//! - `oxen push`
//!

/// api to do stuff
pub mod api;
pub mod model;
pub mod view;
pub mod command;
pub mod config;
pub mod constants;
pub mod error;
pub mod index;
pub mod test;
pub mod util;
