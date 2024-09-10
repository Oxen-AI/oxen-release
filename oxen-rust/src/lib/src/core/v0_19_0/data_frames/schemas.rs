//! # oxen schemas
//!
//! Interact with schemas
//!

use std::collections::HashMap;
use std::path::PathBuf;

use crate::error::OxenError;
use crate::model::{LocalRepository, Schema};

use std::path::Path;

pub fn list(
    repo: &LocalRepository,
    commit_id: Option<&str>,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    todo!()
}

pub fn get_by_path(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    todo!()
}

/// Get a schema for a specific revision
pub fn get_by_path_from_revision(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    todo!()
}

pub fn get_by_hash(repo: &LocalRepository, hash: String) -> Result<Option<Schema>, OxenError> {
    todo!()
}

/// Get a staged schema
pub fn get_staged(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    todo!()
}

/// List all the staged schemas
pub fn list_staged(repo: &LocalRepository) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    todo!()
}

/// Get the current schema for a given schema ref
pub fn get_from_head(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    todo!()
}

/// Get a string representation of the schema given a schema ref
pub fn show(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    staged: bool,
    verbose: bool,
) -> Result<String, OxenError> {
    todo!()
}

/// Set the name of a schema
pub fn set_name(repo: &LocalRepository, hash: &str, val: &str) -> Result<(), OxenError> {
    todo!()
}

/// Remove a schema override from the staging area, TODO: Currently undefined behavior for non-staged schemas
pub fn rm(repo: &LocalRepository, path: impl AsRef<Path>, staged: bool) -> Result<(), OxenError> {
    todo!()
}

/// Add metadata to the schema
pub fn add_schema_metadata(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    todo!()
}

/// Add metadata to a specific column
pub fn add_column_metadata(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    todo!()
}
