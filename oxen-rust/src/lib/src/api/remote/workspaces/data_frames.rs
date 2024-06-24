use serde::{Deserialize, Serialize};

use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::opts::DFOpts;
use crate::view::entry::PaginatedMetadataEntriesResponse;
use std::path::Path;

use crate::model::RemoteRepository;
use crate::view::{JsonDataFrameViewResponse, StatusMessage};

pub mod rows;

#[derive(Serialize, Deserialize)]
struct PutParam {
    is_indexed: bool,
}

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    opts: DFOpts,
) -> Result<JsonDataFrameViewResponse, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };
    let query_str = opts.to_http_query_params();
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/resource/{file_path_str}?{query_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<JsonDataFrameViewResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(response) => Ok(response),
                Err(err) => {
                    let err = format!("workspaces::data_frames::get error parsing from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("workspaces::data_frames::get Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}
pub async fn list(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    workspace_id: &str,
) -> Result<PaginatedMetadataEntriesResponse, OxenError> {
    let uri = format!("/workspaces/{workspace_id}/data_frames/branch/{branch_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<PaginatedMetadataEntriesResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(response) => Ok(response),
                Err(err) => {
                    let err = format!("api::workspaces::get_by_branch error parsing from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err =
                format!("api::workspaces::get_by_branch Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn index(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<StatusMessage, OxenError> {
    put(
        remote_repo,
        workspace_id,
        path,
        &serde_json::json!({"is_indexed": true}),
    )
    .await
}

pub async fn unindex(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<StatusMessage, OxenError> {
    put(
        remote_repo,
        workspace_id,
        path,
        &serde_json::json!({"is_indexed": false}),
    )
    .await
}

pub async fn put(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<StatusMessage, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };

    let uri = format!("/workspaces/{workspace_id}/data_frames/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let params = serde_json::to_string(data)?;

    let client = client::new_for_url(&url)?;
    match client.put(&url).body(params).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(response) => Ok(response),
                Err(err) => {
                    let err = format!(
                        "api::workspaces::put error parsing from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::workspaces::put Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn restore(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let file_name = path.as_ref().to_string_lossy();
    let uri = format!("/workspaces/{workspace_id}/modifications/{file_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("workspaces::data_frames::restore {}", url);
    let client = client::new_for_url(&url)?;
    match client.delete(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("workspaces::data_frames::restore got body: {}", body);
            Ok(())
        }
        Err(err) => {
            let err =
                format!("workspaces::data_frames::restore Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn diff(
    remote_repo: &RemoteRepository,
    identifier: &str,
    path: &Path,
) -> Result<StatusMessage, OxenError> {
    let file_path_str = path.to_str().unwrap();

    let uri = format!("/workspaces/{identifier}/data_frames/diff/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(response) => Ok(response),
                Err(err) => {
                    let err = format!(
                        "api::workspaces::diff error parsing from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::workspaces::diff Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};
    use crate::error::OxenError;
    use crate::model::diff::DiffResult;
    use crate::opts::DFOpts;
    use crate::test;

    #[tokio::test]
    async fn test_get_by_resource() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let path = Path::new("annotations/train/bounding_box.csv");

            api::remote::workspaces::data_frames::put(
                &remote_repo,
                "some_workspace",
                path,
                &serde_json::json!({"is_indexed": true}),
            )
            .await?;

            let res = api::remote::workspaces::data_frames::get(
                &remote_repo,
                "some_workspace",
                path,
                DFOpts::empty(),
            )
            .await?;

            assert_eq!(res.status.status_message, "resource_found");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_data_frames() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let path = Path::new("annotations/train/bounding_box.csv");

            api::remote::workspaces::data_frames::index(&remote_repo, "some_workspace", path)
                .await?;

            let res = api::remote::workspaces::data_frames::list(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "some_workspace",
            )
            .await?;

            assert_eq!(res.entries.entries.len(), 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_index_workspace_data_frames() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let path = Path::new("annotations/train/bounding_box.csv");

            let res =
                api::remote::workspaces::data_frames::index(&remote_repo, "some_workspace", path)
                    .await?;

            assert_eq!(res.status, "success");

            let res =
                api::remote::workspaces::data_frames::unindex(&remote_repo, "some_workspace", path)
                    .await?;

            assert_eq!(res.status, "success");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_data_frame_diff() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let workspace_id = "some_workspace";
            let path = Path::new("annotations/train/bounding_box.csv");

            let res = api::remote::workspaces::create(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                workspace_id,
                path,
            )
            .await;
            assert!(res.is_ok());

            let res = api::remote::workspaces::data_frames::index(&remote_repo, workspace_id, path)
                .await?;

            assert_eq!(res.status, "success");

            let res = api::remote::workspaces::data_frames::diff(&remote_repo, workspace_id, path)
                .await?;

            assert_eq!(res.status, "success");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_restore_modified_dataframe() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::remote::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let result_1 = api::remote::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string()
                ).await;
            assert!(result_1.is_ok());

            let data = "{\"file\":\"image2.jpg\", \"label\": \"cat\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            let result_2 = api::remote::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string(),
                ).await;
            assert!(result_2.is_ok());


            // Make sure both got staged
            let diff = api::remote::workspaces::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            log::debug!("Got this diff {:?}", diff);

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                }
                _ => panic!("Expected tabular diff result"),
            }
            // Delete result_2
            let result_delete = api::remote::workspaces::data_frames::restore(
                &remote_repo,
                &workspace_id,
                &path,
            ).await;
            assert!(result_delete.is_ok());

            // Should be cleared
            let diff = api::remote::workspaces::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 0);
                }
                _ => panic!("Expected tabular diff result."),
            }

            Ok(remote_repo)
        })
        .await
    }
}
