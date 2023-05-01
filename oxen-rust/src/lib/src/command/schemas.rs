//! # oxen schemas
//!
//! Interact with schemas
//!

use crate::api;
use crate::error::OxenError;
use crate::core::index::Stager;
use crate::model::{LocalRepository, Schema};

/// List the saved off schemas for a commit id
pub fn list(repo: &LocalRepository, commit_id: Option<&str>) -> Result<Vec<Schema>, OxenError> {
    api::local::schemas::list(repo, commit_id)
}

/// Get a staged schema
pub fn get_staged(repo: &LocalRepository, schema_ref: &str) -> Result<Option<Schema>, OxenError> {
    let stager = Stager::new(repo)?;
    stager.get_staged_schema(schema_ref)
}

/// List all the staged schemas
pub fn list_staged(repo: &LocalRepository) -> Result<Vec<Schema>, OxenError> {
    let stager = Stager::new(repo)?;
    stager.list_staged_schemas()
}

/// Get the current schema for a given schema ref
pub fn get_from_head(
    repo: &LocalRepository,
    schema_ref: &str,
) -> Result<Option<Schema>, OxenError> {
    get(repo, None, schema_ref)
}

/// Get a schema for a commit id
pub fn get(
    repo: &LocalRepository,
    commit_id: Option<&str>,
    schema_ref: &str,
) -> Result<Option<Schema>, OxenError> {
    // The list of schemas should not be too long, so just filter right now
    let list: Vec<Schema> = list(repo, commit_id)?
        .into_iter()
        .filter(|s| s.name == Some(String::from(schema_ref)) || s.hash == *schema_ref)
        .collect();
    if !list.is_empty() {
        Ok(Some(list.first().unwrap().clone()))
    } else {
        Ok(None)
    }
}

/// Set the name of a schema
pub fn set_name(repo: &LocalRepository, hash: &str, val: &str) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    stager.update_schema_names_for_hash(hash, val)
}
