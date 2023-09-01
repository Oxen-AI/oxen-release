//! # oxen schemas
//!
//! Interact with schemas
//!

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::api;
use crate::core::df::tabular;
use crate::core::index::Stager;
use crate::error::OxenError;
use crate::model::schema::Field;
use crate::model::{EntryDataType, LocalRepository, Schema};
use crate::util;

/// List the saved off schemas for a commit id
pub fn list(
    repo: &LocalRepository,
    commit_id: Option<&str>,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    api::local::schemas::list(repo, commit_id)
}

/// Get a staged schema
pub fn get_staged(
    repo: &LocalRepository,
    schema_ref: &str,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let stager = Stager::new(repo)?;
    stager.get_staged_schema(schema_ref)
}

/// List all the staged schemas
pub fn list_staged(repo: &LocalRepository) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let stager = Stager::new(repo)?;
    stager.list_staged_schemas()
}

/// Get the current schema for a given schema ref
pub fn get_from_head(
    repo: &LocalRepository,
    schema_ref: &str,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let commit = api::local::commits::head_commit(repo)?;
    api::local::schemas::list_from_ref(
        repo, commit.id, schema_ref,
    )
}

/// Get a string representation of the schema given a schema ref
pub fn show(
    repo: &LocalRepository,
    schema_ref: &str,
    staged: bool,
    verbose: bool,
) -> Result<String, OxenError> {
    let schemas = if staged {
        get_staged(repo, schema_ref)?
    } else {
        let commit = api::local::commits::head_commit(repo)?;
        api::local::schemas::list_from_ref(repo, &commit.id, schema_ref)?
    };

    if schemas.is_empty() {
        return Err(OxenError::schema_does_not_exist(schema_ref));
    }

    let mut results = String::new();
    for (path, schema) in schemas {
        // if schema.name.is_none() {
        //     eprintln!(
        //         "Schema has no name, to name run:\n\n  oxen schemas name {} \"my_schema\"\n\n",
        //         schema.hash
        //     );
        // }

        if verbose {
            let mut table = comfy_table::Table::new();
            table.set_header(vec!["name", "dtype", "dtype_override", "metadata"]);

            for field in schema.fields.iter() {
                let mut row = vec![field.name.to_string(), field.dtype.to_string()];
                if let Some(val) = &field.dtype_override {
                    row.push(val.to_owned())
                } else {
                    row.push(String::from(""))
                }

                if let Some(val) = &field.metadata {
                    row.push(val.to_owned())
                } else {
                    row.push(String::from(""))
                }
                table.add_row(row);
            }
            results.push_str(&format!(
                "{}\n{}\n",
                path.to_string_lossy(),
                table
            ));
        } else {
            results.push_str(&format!(
                "{}\t{}\t{}",
                path.to_string_lossy(),
                schema.hash,
                schema
            ))
        }
    }
    Ok(results)
}

/// Set the name of a schema
pub fn set_name(repo: &LocalRepository, hash: &str, val: &str) -> Result<(), OxenError> {
    let stager = Stager::new(repo)?;
    stager.update_schema_names_for_hash(hash, val)
}

/// Add a schema override to the staging area
pub fn add(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    fields_str: impl AsRef<str>,
) -> Result<(), OxenError> {
    let path = path.as_ref();

    // Can only add schemas to paths that exist
    if !path.exists() {
        return Err(OxenError::path_does_not_exist(path.to_str().unwrap()));
    }

    // Make sure the path is tabular
    let data_type = util::fs::file_data_type(path);
    if data_type != EntryDataType::Tabular {
        let err = format!(
            "Only tabular data is supported for schemas, found: {:?} for file {:?}",
            data_type, path
        );
        return Err(OxenError::basic_str(err));
    }

    // Read the schema of the file
    let mut schema = tabular::get_schema(path)?;

    // Add overrides to the specified fields
    let fields = Field::fields_from_string(fields_str.as_ref());
    schema.set_field_dtype_overrides(fields);

    // Add the schema to the staging area
    let stager = Stager::new(repo)?;
    stager.update_schema_field_dtype_overrides(path, schema.fields.clone())
}

/// Remove a schema override from the staging area, TODO: Currently undefined behavior for non-staged schemas
pub fn rm(repo: &LocalRepository, path: impl AsRef<Path>, staged: bool) -> Result<(), OxenError> {
    if !staged {
        panic!("Undefined behavior for non-staged schemas")
    }

    let stager = Stager::new(repo)?;
    stager.rm_schema(path)
}
