//! # oxen schemas
//!
//! Interact with schemas
//!

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::core::df::tabular;
use crate::core::index::Stager;
use crate::error::OxenError;
use crate::model::schema::Field;
use crate::model::{EntryDataType, LocalRepository, Schema};
use crate::util;
use crate::{api, command};

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
            results.push_str(&format!(
                "{} {}\n{}\n",
                path.to_string_lossy(),
                schema.hash,
                verbose_str
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
pub fn add_column_overrides(
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

    // Make sure the file is added
    command::add(repo, path)?;

    // Add the schema to the staging area
    let stager = Stager::new(repo)?;
    let relative_path = util::fs::path_relative_to_dir(path, &repo.path)?;
    stager.update_schema_for_path(&relative_path, &schema)?;

    // Fetch schema from db
    let schemas = stager.get_staged_schema(relative_path.to_string_lossy())?;
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

/// Add metadata to the schema
pub fn add_schema_metadata(
    repo: &LocalRepository,
    schema_ref: impl AsRef<str>,
    metadata: impl AsRef<str>,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let schema_ref = schema_ref.as_ref();
    let head_commit = api::local::commits::head_commit(repo)?;
    log::debug!("add_column_metadata head_commit: {}", head_commit);

    let stager = Stager::new(repo)?;
    let committed_schemas = api::local::schemas::list_from_ref(repo, head_commit.id, schema_ref)?;
    log::debug!(
        "add_column_metadata committed_schemas.len(): {:?}",
        committed_schemas.len()
    );
    let committed_schemas_is_empty = committed_schemas.is_empty();
    for (path, mut schema) in committed_schemas {
        log::debug!("committed_schemas[{:?}] -> {:?}", path, schema);
        schema.metadata = Some(metadata.as_ref().to_string());
        stager.update_schema_for_path(&path, &schema)?;
    }

    let staged_schemas = stager.get_staged_schema(schema_ref)?;
    if committed_schemas_is_empty && staged_schemas.is_empty() {
        return Err(OxenError::schema_does_not_exist(schema_ref));
    }

    let mut results = HashMap::new();
    for (path, mut schema) in staged_schemas {
        schema.metadata = Some(metadata.as_ref().to_string());
        let schema = stager.update_schema_for_path(&path, &schema)?;
        results.insert(path, schema);
    }
    Ok(results)
}

/// Add metadata to a specific column
pub fn add_column_metadata(
    repo: &LocalRepository,
    schema_ref: impl AsRef<str>,
    column: impl AsRef<str>,
    metadata: impl AsRef<str>,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let schema_ref = schema_ref.as_ref();
    let column = column.as_ref();
    let head_commit = api::local::commits::head_commit(repo)?;
    log::debug!("add_column_metadata head_commit: {}", head_commit);

    let committed_schemas = api::local::schemas::list_from_ref(repo, head_commit.id, schema_ref)?;
    log::debug!(
        "add_column_metadata committed_schemas.len(): {:?}",
        committed_schemas.len()
    );
    for (path, schema) in committed_schemas.iter() {
        log::debug!("committed_schemas[{:?}] -> {:?}", path, schema);
        if schema.matches_ref(schema_ref) || path == Path::new(schema_ref) {
            if let Some(field) = schema.get_field(column) {
                add_column_overrides(
                    repo,
                    &repo.path.join(path),
                    format!("{}:{}", field.name, field.dtype),
                )?;
            }
        }
    }
    let stager = Stager::new(repo)?;
    let staged_schemas = stager.get_staged_schema(schema_ref)?;

    if committed_schemas.is_empty() && staged_schemas.is_empty() {
        return Err(OxenError::schema_does_not_exist(schema_ref));
    }

    let mut results = HashMap::new();
    for (path, mut schema) in staged_schemas {
        schema.add_column_metadata(column, metadata.as_ref());
        let schema = stager.update_schema_for_path(&path, &schema)?;
        results.insert(path, schema);
    }
    Ok(results)
}

// unit tests
#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::test;
    use crate::util;
    use crate::{api, command};

    #[tokio::test]
    async fn test_cmd_schemas_add_staged() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Add the schema
            command::schemas::add_column_overrides(&repo, &bbox_path, "min_x:i32, min_y:i32")?;

            // Make sure it is staged
            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;
            let schema_ref = bbox_file.to_string_lossy();
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
            let updated_schema =
                command::schemas::add_column_overrides(&repo, &bbox_path, "min_x:f32, height:f64")?;
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

    #[tokio::test]
    async fn test_cmd_schemas_schema_rm_staged() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;
            let schema_ref = bbox_file.to_string_lossy();

            // Add the schema
            command::schemas::add_column_overrides(&repo, &bbox_path, "min_x:i32, min_y:i32")?;

            let schemas = command::schemas::get_staged(&repo, &schema_ref)?;
            assert_eq!(schemas.len(), 1);

            // Remove the schema
            command::schemas::rm(&repo, &schema_ref, true)?;

            // Make sure none are left
            let schemas = command::schemas::get_staged(&repo, &schema_ref)?;
            assert_eq!(schemas.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cmd_schemas_add_schema_metadata() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;
            let schema_ref = bbox_file.to_string_lossy();

            // Add and commit the schema
            command::add(&repo, &bbox_path)?;
            command::commit(&repo, "Adding bounding box file")?;

            // Add the schema
            let metadata = "{\"task\": \"bounding_box\"}".to_string();
            command::schemas::add_schema_metadata(&repo, &schema_ref, &metadata)?;

            let schemas = command::schemas::get_staged(&repo, &schema_ref)?;
            assert_eq!(schemas.len(), 1);
            assert_eq!(schema_ref, schemas.keys().next().unwrap().to_string_lossy());
            let schema = schemas.values().next().unwrap();
            assert_eq!(schema.metadata, Some(metadata));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cmd_schemas_add_custom_data_type() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;
            let schema_ref = bbox_file.to_string_lossy();

            // Add the schema
            command::schemas::add_column_overrides(&repo, &bbox_path, "file:path")?;

            let schemas = command::schemas::get_staged(&repo, &schema_ref)?;
            assert_eq!(schemas.len(), 1);
            assert_eq!(schema_ref, schemas.keys().next().unwrap().to_string_lossy());
            let schema = schemas.values().next().unwrap();
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].dtype_override, Some("path".to_string()));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cmd_schemas_add_column_metadata() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Add the schema
            let metadata = "{\"root\": \"images\"}";
            command::schemas::add_column_overrides(&repo, &bbox_path, "file:path")?;

            let bbox_path = util::fs::path_relative_to_dir(bbox_path, &repo.path)?;
            let schema_ref = bbox_path.to_string_lossy();
            command::schemas::add_column_metadata(&repo, &schema_ref, "file", metadata)?;

            let schemas = command::schemas::get_staged(&repo, &schema_ref)?;
            assert_eq!(schemas.len(), 1);
            assert_eq!(schema_ref, schemas.keys().next().unwrap().to_string_lossy());
            let schema = schemas.values().next().unwrap();
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].dtype_override, Some("path".to_string()));
            assert_eq!(schema.fields[0].metadata, Some(metadata.to_string()));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cmd_schemas_add_column_to_committed_schema() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Add the schema
            command::add(&repo, &bbox_path)?;
            let commit = command::commit(&repo, "Adding bounding box file")?;

            let schemas = api::local::schemas::list(&repo, Some(&commit.id))?;
            for (path, schema) in schemas.iter() {
                println!("GOT SCHEMA {path:?} -> {schema:?}");
            }

            let path = util::fs::path_relative_to_dir(bbox_path, &repo.path)?;
            let schema_ref = path.to_string_lossy();

            // Add the schema
            let metadata = "{\"root\": \"images\"}";
            command::schemas::add_column_metadata(&repo, &schema_ref, "file", metadata)?;

            let schemas = command::schemas::get_staged(&repo, &schema_ref)?;
            assert_eq!(schemas.len(), 1);
            assert_eq!(schema_ref, schemas.keys().next().unwrap().to_string_lossy());
            let schema = schemas.values().next().unwrap();
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].dtype_override, Some("str".to_string()));
            assert_eq!(schema.fields[0].metadata, Some(metadata.to_string()));

            // Commit the schema
            let commit = command::commit(&repo, "Adding metadata to file column")?;

            // List the committed schemas
            let schemas = api::local::schemas::list(&repo, Some(&commit.id))?;
            assert_eq!(schemas.len(), 1);
            assert_eq!(schema_ref, schemas.keys().next().unwrap().to_string_lossy());
            let schema = schemas.values().next().unwrap();
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].dtype_override, Some("str".to_string()));
            assert_eq!(schema.fields[0].metadata, Some(metadata.to_string()));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cmd_schemas_add_column_to_committed_schema_after_changing_data(
    ) -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Add the schema
            command::add(&repo, &bbox_path)?;
            let commit = command::commit(&repo, "Adding bounding box file")?;

            let schemas = api::local::schemas::list(&repo, Some(&commit.id))?;
            for (path, schema) in schemas.iter() {
                println!("GOT SCHEMA {path:?} -> {schema:?}");
            }

            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;
            let schema_ref = bbox_file.to_string_lossy();

            // Add the schema metadata
            let metadata = "{\"root\": \"images\"}";
            command::schemas::add_column_metadata(&repo, &schema_ref, "file", metadata)?;

            // Commit the schema
            command::commit(&repo, "Adding metadata to file column")?;

            // Add a new column to the data frame
            command::df::add_column(&bbox_path, "new_column:0:i32")?;

            // Stage the file
            command::add(&repo, &bbox_path)?;

            // Make sure the metadata persisted
            let schemas = command::schemas::get_staged(&repo, &schema_ref)?;
            assert_eq!(schemas.len(), 1);
            assert_eq!(schema_ref, schemas.keys().next().unwrap().to_string_lossy());
            let schema = schemas.values().next().unwrap();
            assert_eq!(schema.fields.len(), 7);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].dtype_override, Some("str".to_string()));
            assert_eq!(schema.fields[0].metadata, Some(metadata.to_string()));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cmd_schemas_persist_schema_types_across_commits() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Add the schema
            command::schemas::add_column_overrides(&repo, &bbox_path, "min_x:i32, min_y:i32")?;

            // Make sure it is staged
            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;
            let schema_ref = bbox_file.to_string_lossy();
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

            // Commit the schema
            command::commit(&repo, "Adding bounding box schema")?;

            // Update the schema
            let updated_schema =
                command::schemas::add_column_overrides(&repo, &bbox_path, "min_x:f32, height:f64")?;
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

            // this was added in the previous commit, so it should still be there
            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[3].dtype_override, Some("i32".to_string()));

            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");
            assert_eq!(schema.fields[5].dtype_override, Some("f64".to_string()));

            // Commit the schema again
            command::commit(&repo, "Updating the bounding box schema")?;

            // Update the schema
            let updated_schema =
                command::schemas::add_column_overrides(&repo, &bbox_path, "width:f64")?;
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

            // this was added in the previous commit, so it should still be there
            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[3].dtype_override, Some("i32".to_string()));

            // this was added in the previous commit, so it should still be there
            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[4].dtype_override, Some("f64".to_string()));

            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");
            assert_eq!(schema.fields[5].dtype_override, Some("f64".to_string()));

            Ok(())
        })
        .await
    }
}
