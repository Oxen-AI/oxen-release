use std::path::Path;

use polars::frame::DataFrame;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::view::json_data_frame_view::{JsonDataFrameRowResponse, VecBatchUpdateResponse};

use crate::model::RemoteRepository;

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    row_id: &str,
) -> Result<JsonDataFrameRowResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("get_row {url}\n{row_id}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<JsonDataFrameRowResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val),
        Err(err) => {
            let err = format!(
                "api::staging::get_row error parsing response from {url}\n\nErr {err:?} \n\n{body}"
            );
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn update(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    row_id: &str,
    data: String,
) -> Result<JsonDataFrameRowResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("update_row {url}\n{data}");

    let client = client::new_for_url(&url)?;
    let res = client
        .put(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<JsonDataFrameRowResponse, serde_json::Error> = serde_json::from_str(&body);
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

pub async fn delete(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{file_path_str}");

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("rm_df_mod got body: {body}");
    let response: Result<JsonDataFrameRowResponse, serde_json::Error> = serde_json::from_str(&body);
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

pub async fn add(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    data: String,
) -> Result<(DataFrame, Option<String>), OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri = format!("/workspaces/{workspace_id}/data_frames/rows/resource/{file_path_str}");
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
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<JsonDataFrameRowResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok((val.data_frame.view.to_df().await, val.row_id)),
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

pub async fn batch_update(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    data: String,
) -> Result<VecBatchUpdateResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri = format!("/workspaces/{workspace_id}/data_frames/rows/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client
        .put(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await
    {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<VecBatchUpdateResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => {
                    let err = format!(
                        "api::staging::batch_update error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::batch_update Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::client::workspaces::data_frames::{self, columns};
    use crate::api::client::workspaces::{self, changes};
    use crate::repositories::workspaces::df;

    use serde_json::{Value, json};

    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE, OXEN_ID_COL};
    use crate::error::OxenError;
    use crate::model::RemoteRepository;
    use crate::opts::DFOpts;
    use crate::test;
    use polars::prelude::AnyValue;

    use std::path::{Path, PathBuf};

    use super::{add, batch_update, delete, get, update};

    fn bbox_path() -> PathBuf {
        Path::new("annotations")
            .join("train")
            .join("bounding_box.csv")
    }

    async fn indexed_workspace(
        remote_repo: &RemoteRepository,
        workspace_id: &str,
    ) -> Result<(), OxenError> {
        workspaces::create(remote_repo, DEFAULT_BRANCH_NAME, workspace_id).await?;
        data_frames::index(remote_repo, workspace_id, bbox_path()).await?;
        Ok(())
    }

    async fn staged_rows(
        remote_repo: &RemoteRepository,
        workspace_id: &str,
    ) -> Result<Vec<Value>, OxenError> {
        let df = data_frames::get(remote_repo, workspace_id, bbox_path(), &DFOpts::empty()).await?;
        let data = df.data_frame.expect("an indexed data frame").view.data;
        Ok(data.as_array().expect("rows").clone())
    }

    fn row_id(row: &Value) -> String {
        row[OXEN_ID_COL].as_str().expect("a row id").to_string()
    }

    async fn modified_file_count(
        remote_repo: &RemoteRepository,
        workspace_id: &str,
    ) -> Result<usize, OxenError> {
        let changes = changes::list(
            remote_repo,
            workspace_id,
            Path::new(""),
            DEFAULT_PAGE_NUM,
            DEFAULT_PAGE_SIZE,
        )
        .await?;
        Ok(changes.modified_files.entries.len())
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_add_update_and_delete_rows_in_workspaces() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let path = bbox_path();

            // Edit one row at a time
            indexed_workspace(&remote_repo, "edits").await?;
            let rows = staged_rows(&remote_repo, "edits").await?;
            let initial_count = rows.len();
            let first_id = row_id(&rows[0]);
            assert_eq!(get(&remote_repo, "edits", &path, &first_id).await?.data_frame.view.size.height, 1);

            let data = r#"{"file":"lebron>jordan.jpg", "label": "dog", "min_x":13, "min_y":14, "width": 100, "height": 100}"#;
            update(&remote_repo, "edits", &path, &first_id, data.to_string()).await?;
            let row = get(&remote_repo, "edits", &path, &first_id).await?;
            assert_eq!(row.data_frame.view.data[0]["file"], "lebron>jordan.jpg");

            // A deleted row leaves the staged table rather than being tombstoned
            delete(&remote_repo, "edits", &path, &first_id).await?;
            assert_eq!(get(&remote_repo, "edits", &path, &first_id).await?.data_frame.view.size.height, 0);

            let data = r#"{"file":"image1.jpg", "label": "dog", "min_x":13, "min_y":14, "width": 100, "height": 100}"#;
            let (_df, added_id) = add(&remote_repo, "edits", &path, data.to_string()).await?;
            assert!(added_id.is_some());
            let rows = staged_rows(&remote_repo, "edits").await?;
            assert!(rows.iter().any(|row| row["file"] == "image1.jpg"), "the added row is staged");

            add(&remote_repo, "edits", &path, "{}".to_string()).await?;
            assert_eq!(staged_rows(&remote_repo, "edits").await?.len(), initial_count + 1, "one deleted, two added");

            let data = r#"{"id": 1, "name": "greg"}"#;
            let err = add(&remote_repo, "edits", &path, data.to_string())
                .await
                .expect_err("a row outside the schema is refused");
            assert!(err.to_string().contains("incompatible_schemas"), "{err}");
            assert_eq!(modified_file_count(&remote_repo, "edits").await?, 1);

            // Update several rows at once, then again with a list column
            indexed_workspace(&remote_repo, "batch").await?;
            let rows = staged_rows(&remote_repo, "batch").await?;
            let ids: Vec<String> = rows.iter().take(3).map(row_id).collect();
            let updates = json!({"data": [
                {"row_id": ids[0], "value": {"file": "cfxsx"}},
                {"row_id": ids[1], "value": {"file": "yfcsx"}},
            ]});
            batch_update(&remote_repo, "batch", &path, updates.to_string()).await?;
            let rows = staged_rows(&remote_repo, "batch").await?;
            for update in updates["data"].as_array().expect("updates") {
                let row = rows.iter().find(|row| row[OXEN_ID_COL] == update["row_id"]).expect("row");
                assert_eq!(row["file"], update["value"]["file"]);
            }

            columns::create(
                &remote_repo,
                "batch",
                &path,
                r#"{"name": "embedding", "data_type": "list[f64]"}"#.to_string(),
            )
            .await?;
            let updates = json!({"data": [
                {"row_id": ids[0], "value": {"file": "cfxsx", "embedding": [0.1, 0.2, 0.3]}},
                {"row_id": ids[1], "value": {"file": "yfcsx", "embedding": [0.4, 0.5, 0.6]}},
                {"row_id": ids[2], "value": {"file": "zxcvb", "embedding": [0.7, 0.8, 0.9]}},
            ]});
            batch_update(&remote_repo, "batch", &path, updates.to_string()).await?;
            let rows = staged_rows(&remote_repo, "batch").await?;
            for update in updates["data"].as_array().expect("updates") {
                let row = rows.iter().find(|row| row[OXEN_ID_COL] == update["row_id"]).expect("row");
                assert_eq!(row["file"], update["value"]["file"]);
                assert_eq!(row["embedding"], update["value"]["embedding"]);
            }

            // Deleting a row added in the workspace leaves the data frame listed as modified,
            // since edits are applied rather than tracked
            indexed_workspace(&remote_repo, "add-then-delete").await?;
            let mut opts = DFOpts::empty();
            opts.add_row = Some(
                r#"{"file":"image2.jpg", "label": "cat", "min_x":13, "min_y":14, "width": 100, "height": 100}"#
                    .to_string(),
            );
            let added = df(&local_repo, "add-then-delete", &path, opts).await?;
            let added_id = match added.column(OXEN_ID_COL)?.get(0)? {
                AnyValue::String(s) => s.to_string(),
                AnyValue::StringOwned(s) => s.to_string(),
                other => panic!("Expected a string row id, got {other:?}"),
            };
            assert_eq!(modified_file_count(&remote_repo, "add-then-delete").await?, 1);
            let mut opts = DFOpts::empty();
            opts.delete_row = Some(added_id);
            df(&local_repo, "add-then-delete", &path, opts).await?;
            assert_eq!(modified_file_count(&remote_repo, "add-then-delete").await?, 1);

            Ok(remote_repo)
        })
        .await
    }
}
