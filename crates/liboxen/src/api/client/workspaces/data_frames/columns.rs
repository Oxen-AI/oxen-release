use std::path::Path;

use polars::frame::DataFrame;
use serde_json::json;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::view::json_data_frame_view::JsonDataFrameColumnResponse;

use crate::model::RemoteRepository;

pub async fn create(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    data: String,
) -> Result<DataFrame, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri = format!("/workspaces/{workspace_id}/data_frames/columns/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("modify_df {url}\n{data}");

    let client = client::new_for_url(&url)?;
    match client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await
    {
        Ok(res) => {
            let body: String = client::parse_json_body(&url, res).await?;
            let response: Result<JsonDataFrameColumnResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.data_frame.view.to_df().await),
                Err(err) => {
                    let err = format!(
                        "api::staging::modify_df error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::modify_df Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn delete(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    column_name: &str,
) -> Result<DataFrame, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri = format!(
        "/workspaces/{workspace_id}/data_frames/columns/{column_name}/resource/{file_path_str}"
    );

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.delete(&url).send().await {
        Ok(res) => {
            let body: String = client::parse_json_body(&url, res).await?;
            log::debug!("rm_df_mod got body: {body}");
            let response: Result<JsonDataFrameColumnResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.data_frame.view.to_df().await),
                Err(err) => {
                    let err = format!(
                        "api::staging::rm_df_mod error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("rm_df_mod Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn update(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    column_name: &str,
    data: String,
) -> Result<JsonDataFrameColumnResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri = format!(
        "/workspaces/{workspace_id}/data_frames/columns/{column_name}/resource/{file_path_str}"
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("update_column {url}\n{data}");

    let client = client::new_for_url(&url)?;
    match client
        .put(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await
    {
        Ok(res) => {
            let body: String = client::parse_json_body(&url, res).await?;
            let response: Result<JsonDataFrameColumnResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => {
                    let err = format!(
                        "api::staging::update_row error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::update_row Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn add_column_metadata(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    column_name: &str,
    metadata: serde_json::Value,
) -> Result<(), OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri =
        format!("/workspaces/{workspace_id}/data_frames/columns/schema/metadata/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let body = json!({
        "column_name": column_name,
        "metadata": metadata
    });

    match client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            let err = format!("add_column_metadata Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::client::branches::create_from_branch;
    use crate::api::client::workspaces::{self, data_frames};

    use serde_json::{Value, json};

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::{NewCommitBody, RemoteRepository, Schema};
    use crate::opts::DFOpts;
    use crate::test;

    use std::path::{Path, PathBuf};

    use super::{add_column_metadata, create, delete, update};

    fn bbox_path() -> PathBuf {
        Path::new("annotations")
            .join("train")
            .join("bounding_box.csv")
    }

    async fn workspace_schema(
        remote_repo: &RemoteRepository,
        workspace_id: &str,
    ) -> Result<Schema, OxenError> {
        let df = data_frames::get(remote_repo, workspace_id, bbox_path(), &DFOpts::empty()).await?;
        Ok(df.data_frame.expect("an indexed data frame").view.schema)
    }

    /// Opens a workspace on `branch_name`, indexes the bounding box csv, and returns its schema.
    async fn indexed_workspace(
        remote_repo: &RemoteRepository,
        branch_name: &str,
        workspace_id: &str,
    ) -> Result<Schema, OxenError> {
        workspaces::create(remote_repo, branch_name, workspace_id).await?;
        data_frames::index(remote_repo, workspace_id, bbox_path()).await?;
        workspace_schema(remote_repo, workspace_id).await
    }

    fn render_image_metadata(column_name: &str) -> Value {
        json!({
            "column_name": column_name,
            "metadata": { "_oxen": { "render": { "func": "image" } } }
        })
    }

    fn field_metadata(schema: &Schema, column_name: &str) -> Option<Value> {
        let field = schema.fields.iter().find(|field| field.name == column_name);
        field
            .unwrap_or_else(|| panic!("column {column_name} is in {schema:?}"))
            .metadata
            .clone()
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_edit_columns_and_their_metadata_in_workspaces() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            let path = bbox_path();
            let new_column = r#"{"name":"new_column", "data_type": "str"}"#;
            let renamed = r#"{"new_name":"renamed_column"}"#;

            // Create a column
            indexed_workspace(&remote_repo, branch_name, "create").await?;
            create(&remote_repo, "create", &path, new_column.to_string()).await?;
            field_metadata(
                &workspace_schema(&remote_repo, "create").await?,
                "new_column",
            );

            // Delete a column outright, with no tombstone left in the schema
            let schema = indexed_workspace(&remote_repo, branch_name, "delete").await?;
            let column = schema.fields[4].name.clone();
            delete(&remote_repo, "delete", &path, &column).await?;
            let schema = workspace_schema(&remote_repo, "delete").await?;
            assert!(
                !schema.fields.iter().any(|field| field.name == column),
                "{column} is gone"
            );

            // Rename a column
            let schema = indexed_workspace(&remote_repo, branch_name, "rename").await?;
            update(
                &remote_repo,
                "rename",
                &path,
                &schema.fields[4].name,
                renamed.to_string(),
            )
            .await?;
            field_metadata(
                &workspace_schema(&remote_repo, "rename").await?,
                "renamed_column",
            );

            // Metadata added after a rename lands on the new name
            let schema = indexed_workspace(&remote_repo, branch_name, "rename-then-tag").await?;
            update(
                &remote_repo,
                "rename-then-tag",
                &path,
                &schema.fields[4].name,
                renamed.to_string(),
            )
            .await?;
            let metadata = render_image_metadata("renamed_column");
            add_column_metadata(
                &remote_repo,
                "rename-then-tag",
                &path,
                "renamed_column",
                metadata.clone(),
            )
            .await?;
            let schema = workspace_schema(&remote_repo, "rename-then-tag").await?;
            assert_eq!(field_metadata(&schema, "renamed_column"), Some(metadata));

            // Metadata can tag a column created in the same workspace
            indexed_workspace(&remote_repo, branch_name, "create-then-tag").await?;
            create(
                &remote_repo,
                "create-then-tag",
                &path,
                new_column.to_string(),
            )
            .await?;
            let metadata = render_image_metadata("new_column");
            add_column_metadata(
                &remote_repo,
                "create-then-tag",
                &path,
                "new_column",
                metadata.clone(),
            )
            .await?;
            let schema = workspace_schema(&remote_repo, "create-then-tag").await?;
            assert_eq!(field_metadata(&schema, "new_column"), Some(metadata));

            // Metadata follows its column through a rename, staged or committed in between
            let schema = indexed_workspace(&remote_repo, branch_name, "tag-then-rename").await?;
            let column = schema.fields[4].name.clone();
            let metadata = render_image_metadata(&column);
            add_column_metadata(
                &remote_repo,
                "tag-then-rename",
                &path,
                &column,
                metadata.clone(),
            )
            .await?;
            update(
                &remote_repo,
                "tag-then-rename",
                &path,
                &column,
                renamed.to_string(),
            )
            .await?;
            let schema = workspace_schema(&remote_repo, "tag-then-rename").await?;
            assert_eq!(
                field_metadata(&schema, "renamed_column"),
                Some(metadata.clone())
            );

            let commit_branch = "commit-metadata";
            create_from_branch(&remote_repo, commit_branch, DEFAULT_BRANCH_NAME).await?;
            indexed_workspace(&remote_repo, commit_branch, "tag-commit-rename").await?;
            add_column_metadata(
                &remote_repo,
                "tag-commit-rename",
                &path,
                &column,
                metadata.clone(),
            )
            .await?;
            let commit_body = NewCommitBody {
                message: "Add metadata to column".to_string(),
                author: "test@test.com".to_string(),
                email: "test@test.com".to_string(),
            };
            workspaces::commit(
                &remote_repo,
                commit_branch,
                "tag-commit-rename",
                &commit_body,
            )
            .await?;
            indexed_workspace(&remote_repo, commit_branch, "tag-commit-rename").await?;
            update(
                &remote_repo,
                "tag-commit-rename",
                &path,
                &column,
                renamed.to_string(),
            )
            .await?;
            let schema = workspace_schema(&remote_repo, "tag-commit-rename").await?;
            assert_eq!(field_metadata(&schema, "renamed_column"), Some(metadata));

            Ok(remote_repo)
        })
        .await
    }
}
