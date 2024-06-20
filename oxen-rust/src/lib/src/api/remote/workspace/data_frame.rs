use serde::{Deserialize, Serialize};

use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::opts::DFOpts;
use crate::view::entry::PaginatedMetadataEntriesResponse;
use std::path::Path;

use crate::model::RemoteRepository;
use crate::view::{JsonDataFrameViewResponse, StatusMessage};

#[derive(Serialize, Deserialize)]
struct PutParam {
    is_indexed: bool,
}

pub async fn get_by_resource(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    identifier: impl AsRef<str>,
    path: impl AsRef<Path>,
    opts: DFOpts,
) -> Result<JsonDataFrameViewResponse, OxenError> {
    let branch_name = branch_name.as_ref();
    let identifier = identifier.as_ref();
    let path = path.as_ref();
    let file_path_str = path.to_str().unwrap();
    let query_str = opts.to_http_query_params();
    let uri = format!(
        "/workspace/{identifier}/data_frame/resource/{branch_name}/{file_path_str}?{query_str}"
    );
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
                    let err = format!("api::workspace::get_by_resource error parsing from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err =
                format!("api::workspace::get_by_resource Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}
pub async fn get_by_branch(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
) -> Result<PaginatedMetadataEntriesResponse, OxenError> {
    let uri = format!("/workspace/{identifier}/data_frame/branch/{branch_name}");
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
                    let err = format!("api::workspace::get_by_branch error parsing from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::workspace::get_by_branch Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn put(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    identifier: impl AsRef<str>,
    path: impl AsRef<Path>,
    is_indexed: bool,
) -> Result<StatusMessage, OxenError> {
    let branch_name = branch_name.as_ref();
    let identifier = identifier.as_ref();
    let path = path.as_ref();
    let file_path_str = path.to_str().unwrap();

    let uri = format!("/workspace/{identifier}/data_frame/resource/{branch_name}/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let params = serde_json::to_string(&PutParam { is_indexed })?;

    let client = client::new_for_url(&url)?;
    match client.put(&url).body(params).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(response) => Ok(response),
                Err(err) => {
                    let err = format!(
                        "api::workspace::put error parsing from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::workspace::put Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn diff(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    path: &Path,
) -> Result<StatusMessage, OxenError> {
    let file_path_str = path.to_str().unwrap();

    let uri = format!("/workspace/{identifier}/data_frame/diff/{branch_name}/{file_path_str}");
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
                        "api::workspace::diff error parsing from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::workspace::diff Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;
    use std::path::PathBuf;

    use serde_json::json;

    use crate::api;
    use crate::command;
    use crate::config::UserConfig;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::error::OxenError;
    use crate::opts::DFOpts;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_get_by_resource() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let name = "main";
            let path = Path::new("annotations/train/bounding_box.csv");

            api::remote::workspace::data_frame::put(
                &remote_repo,
                name,
                "some_workspace",
                path,
                true,
            )
            .await?;

            let res = api::remote::workspace::data_frame::get_by_resource(
                &remote_repo,
                name,
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
    async fn test_get_by_branch() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let name = "main";
            let path = Path::new("annotations/train/bounding_box.csv");

            api::remote::workspace::data_frame::put(
                &remote_repo,
                name,
                "some_workspace",
                path,
                true,
            )
            .await?;

            let res = api::remote::workspace::data_frame::get_by_branch(
                &remote_repo,
                name,
                "some_workspace",
            )
            .await?;

            assert_eq!(res.entries.entries.len(), 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_put() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let name = "main";
            let path = Path::new("annotations/train/bounding_box.csv");

            let res = api::remote::workspace::data_frame::put(
                &remote_repo,
                name,
                "some_workspace",
                path,
                true,
            )
            .await?;

            assert_eq!(res.status, "success");

            let res = api::remote::workspace::data_frame::put(
                &remote_repo,
                name,
                "some_workspace",
                path,
                false,
            )
            .await?;

            assert_eq!(res.status, "success");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_data_frame_diff() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let name = "main";
            let path = Path::new("annotations/train/bounding_box.csv");

            let res = api::remote::workspace::data_frame::put(
                &remote_repo,
                name,
                "some_workspace",
                path,
                true,
            )
            .await?;

            assert_eq!(res.status, "success");

            let res = api::remote::workspace::data_frame::diff(
                &remote_repo,
                name,
                "some_workspace",
                path,
            )
            .await?;

            assert_eq!(res.status, "success");

            Ok(remote_repo)
        })
        .await
    }
}
