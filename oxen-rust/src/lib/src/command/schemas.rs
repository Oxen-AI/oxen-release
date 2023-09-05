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
    api::local::schemas::list_from_ref(repo, commit.id, schema_ref)
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
            let verbose_str = schema.verbose_str();
            results.push_str(&format!("{}\n{}\n", path.to_string_lossy(), verbose_str));
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
) -> Result<Schema, OxenError> {
    log::debug!("Adding schema override for {:?}", path.as_ref());
    log::debug!("fields_str: {:?}", fields_str.as_ref());
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
    stager.update_schema_for_path(path, &schema)?;

    // Fetch schema from db
    let schemas = stager.get_staged_schema(&path.to_string_lossy())?;
    Ok(schemas.values().next().unwrap().clone())
}

/// Remove a schema override from the staging area, TODO: Currently undefined behavior for non-staged schemas
pub fn rm(
    repo: &LocalRepository,
    schema_ref: impl AsRef<str>,
    staged: bool,
) -> Result<(), OxenError> {
    if !staged {
        panic!("Undefined behavior for non-staged schemas")
    }

    let stager = Stager::new(repo)?;
    stager.rm_schema(schema_ref)
}

// unit tests
#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::test;

    #[tokio::test]
    async fn test_schema_add() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Add the schema
            command::schemas::add(&repo, &bbox_path, "min_x:i32, min_y:i32")?;

            // Make sure it is staged
            let schema_ref = bbox_path.to_string_lossy();
            let schemas = command::schemas::get_staged(&repo, &schema_ref)?;
            assert_eq!(schemas.len(), 1);
            assert_eq!(schema_ref, schemas.keys().next().unwrap().to_string_lossy());
            let schema = schemas.values().next().unwrap();
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "label");
            assert_eq!(schema.fields[1].dtype, "str");

            assert_eq!(schema.fields[2].name, "min_x");
            assert_eq!(schema.fields[2].dtype, "f64");
            assert_eq!(schema.fields[2].dtype_override, Some("i32".to_string()));

            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[3].dtype_override, Some("i32".to_string()));

            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");

            // Update the schema
            let updated_schema = command::schemas::add(&repo, &bbox_path, "min_x:f32, height:f64")?;
            let schemas = command::schemas::get_staged(&repo, &schema_ref)?;
            assert_eq!(schemas.len(), 1);
            assert_eq!(schema_ref, schemas.keys().next().unwrap().to_string_lossy());
            let schema = schemas.values().next().unwrap();
            assert!(updated_schema == *schema);
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "label");
            assert_eq!(schema.fields[1].dtype, "str");

            assert_eq!(schema.fields[2].name, "min_x");
            assert_eq!(schema.fields[2].dtype, "f64");
            assert_eq!(schema.fields[2].dtype_override, Some("f32".to_string()));

            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[3].dtype_override, Some("i32".to_string()));

            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");
            assert_eq!(schema.fields[5].dtype_override, Some("f64".to_string()));

            Ok(())
        })
        .await
    }
}
