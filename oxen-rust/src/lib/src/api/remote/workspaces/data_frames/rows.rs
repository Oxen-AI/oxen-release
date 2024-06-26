use std::path::Path;

use polars::frame::DataFrame;

use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::view::json_data_frame_view::JsonDataFrameRowResponse;

use crate::model::RemoteRepository;

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    row_id: &str,
) -> Result<JsonDataFrameRowResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("get_row {url}\n{row_id}");

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<JsonDataFrameRowResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => {
                    let err = format!("api::staging::get_row error parsing response from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::get_row Request failed: {url}\n\nErr {err:?}");
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
            "Path must be a string: {:?}",
            path
        )));
    };

    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("update_row {url}\n{data}");

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
            let response: Result<JsonDataFrameRowResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => {
                    let err = format!("api::staging::update_row error parsing response from {url}\n\nErr {err:?} \n\n{body}");
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

pub async fn delete(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };

    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{file_path_str}");

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.delete(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("rm_df_mod got body: {}", body);
            let response: Result<JsonDataFrameRowResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.data_frame.view.to_df()),
                Err(err) => {
                    let err = format!("api::staging::rm_df_mod error parsing response from {url}\n\nErr {err:?} \n\n{body}");
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

pub async fn add(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    data: String,
) -> Result<(DataFrame, Option<String>), OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
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
                Ok(val) => Ok((val.data_frame.view.to_df(), val.row_id)),
                Err(err) => {
                    let err = format!("api::staging::modify_df error parsing response from {url}\n\nErr {err:?} \n\n{body}");
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

pub async fn restore_row(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    row_id: &str,
) -> Result<JsonDataFrameRowResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };

    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/restore/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client
        .post(&url)
        .header("Content-Type", "application/json")
        .send()
        .await
    {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<JsonDataFrameRowResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => {
                    let err = format!("api::staging::update_row error parsing response from {url}\n\nErr {err:?} \n\n{body}");
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

#[cfg(test)]
mod tests {

    use serde_json::Value;

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::opts::DFOpts;
    use crate::test;
    use crate::view::json_data_frame_view::JsonDataFrameRowResponse;

    use std::path::Path;

    #[tokio::test]
    async fn test_stage_row_on_dataframe_json() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::remote::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.workspace_id, workspace_id);

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let path = Path::new("annotations").join("train").join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            api::remote::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let result =
                api::remote::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string()
                ).await;

            assert!(result.is_ok());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_should_not_stage_invalid_schema_for_dataframe() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let data = "{\"id\": 1, \"name\": \"greg\"}";
            let result = api::remote::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string(),
            )
            .await;

            assert!(result.is_err());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_status_modified_dataframe() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace = api::remote::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.workspace_id, workspace_id);

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data: &str = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            api::remote::workspaces::data_frames::index(
                &remote_repo,
                &workspace_id,
                &path,
            ).await?;
            api::remote::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string()
            ).await?;

            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let entries = api::remote::workspaces::changes::list(
                &remote_repo,
                &workspace_id,
                &directory,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.modified_files.entries.len(), 1);
            assert_eq!(entries.modified_files.total_entries, 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_restore_row() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let workspace = api::remote::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.workspace_id, workspace_id);

            // Path to the CSV file
            let path = Path::new("annotations").join("train").join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::remote::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            // Create a new row
            let result = api::remote::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string()
            ).await;

            assert!(result.is_ok());

            let row_id: &String = result.as_ref().unwrap().1.as_ref().unwrap();

            // Get the newly created row
            let row = api::remote::workspaces::data_frames::rows::get(&remote_repo, &workspace_id, &path, row_id).await?;

            // Check the "_oxen_diff_status" field
            let data: Value = serde_json::from_value(row.data_frame.view.data[0].clone()).unwrap();
            assert_eq!(data.get("_oxen_diff_status").unwrap(), "added");

            // Restore the row
            let _restore_resp = api::remote::workspaces::data_frames::rows::restore_row(&remote_repo, &workspace_id, &path, row_id).await?;

            // Get the restored row
            let restored_row: JsonDataFrameRowResponse = api::remote::workspaces::data_frames::rows::get(&remote_repo, &workspace_id, &path, row_id).await?;

            // Check that the restored data is null
            let restore_data: Value = serde_json::from_value(restored_row.data_frame.view.data[0].clone()).unwrap();
            assert!(restore_data.is_null(), "Restored data is not null");

            Ok(remote_repo)
        }).await
    }

    #[tokio::test]
    async fn test_delete_row() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;

            // Path to the CSV file
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            api::remote::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            api::remote::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let df = api::remote::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                DFOpts::empty(),
            )
            .await?;

            // Extract the first _oxen_row_id from the data frame
            let binding = df.data_frame.unwrap();
            let row_id_value = binding
                .view
                .data
                .get(0)
                .and_then(|row| row.get("_oxen_id"))
                .unwrap();

            let row_id = row_id_value.as_str().unwrap();

            let row = api::remote::workspaces::data_frames::rows::get(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
            )
            .await?;

            let data: Value = serde_json::from_value(row.data_frame.view.data[0].clone()).unwrap();

            assert_eq!(data.get("_oxen_diff_status").unwrap(), "unchanged");

            api::remote::workspaces::data_frames::rows::delete(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
            )
            .await?;

            let row = api::remote::workspaces::data_frames::rows::get(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
            )
            .await?;

            let data: Value = serde_json::from_value(row.data_frame.view.data[0].clone()).unwrap();

            assert_eq!(data.get("_oxen_diff_status").unwrap(), "removed");
            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_update_row() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::remote::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.workspace_id, workspace_id);

            // Path to the CSV file
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            api::remote::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let df = api::remote::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                DFOpts::empty(),
            )
            .await?;

            // Extract the first _oxen_row_id from the data frame
            let binding = df
                .data_frame
                .unwrap();
            let row_id_value = binding
                .view
                .data
                .get(0)
                .and_then(|row| row.get("_oxen_id"))
                .unwrap();

            let row_id = row_id_value.as_str().unwrap();

            let row = api::remote::workspaces::data_frames::rows::get(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
            )
            .await?;

            let data: Value = serde_json::from_value(row.data_frame.view.data[0].clone()).unwrap();

            assert_eq!(data.get("_oxen_diff_status").unwrap(), "unchanged");

            let data: &str = "{\"file\":\"lebron>jordan.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::remote::workspaces::data_frames::rows::update(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
                data.to_string()
            )
            .await?;

            let row = api::remote::workspaces::data_frames::rows::get(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
            )
            .await?;

            let data: Value = serde_json::from_value(row.data_frame.view.data[0].clone()).unwrap();
            assert_eq!(data.get("file").unwrap() ,"lebron>jordan.jpg");

            assert_eq!(data.get("_oxen_diff_status").unwrap(), "modified");
            Ok(remote_repo)
        })
        .await
    }
}
