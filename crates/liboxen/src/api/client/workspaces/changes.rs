use crate::api;

use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::{RemoteStagedStatus, RemoteStagedStatusResponse};

use std::path::Path;

pub async fn list(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    page: usize,
    page_size: usize,
) -> Result<RemoteStagedStatus, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let path_str = path.to_str().unwrap();
    let uri =
        format!("/workspaces/{workspace_id}/changes/{path_str}?page={page}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("status url: {url}");

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("status got body: {body}");
            let response: Result<RemoteStagedStatusResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.staged),
                Err(err) => Err(OxenError::basic_str(format!(
                    "api::staging::status error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("api::staging::status Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

// TODO: Consolidate this with api::client::workspaces::files to have only one workspace rm API
pub async fn rm(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let file_name = path.as_ref().to_string_lossy();
    let uri = format!("/workspaces/{workspace_id}/changes/{file_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("rm_file {url}");
    let client = client::new_for_url(&url)?;
    match client.delete(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("rm_file got body: {body}");
            Ok(())
        }
        Err(err) => {
            let err = format!("rm_file Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::client::branches::create_from_branch;
    use crate::api::client::workspaces;
    use crate::repositories::branches::create_checkout;

    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;

    use std::path::Path;

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_list_empty_changes_in_a_new_workspace() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            // One branch pushed from the local repo, one created through the API
            let pushed_branch = "pushed-branch";
            create_checkout(&local_repo, pushed_branch)?;
            repositories::push(&local_repo).await?;
            let api_branch = "api-branch";
            create_from_branch(&remote_repo, api_branch, DEFAULT_BRANCH_NAME).await?;

            for branch_name in [pushed_branch, api_branch] {
                let workspace_id = format!("{branch_name}-workspace");
                workspaces::create(&remote_repo, branch_name, &workspace_id).await?;
                let entries = super::list(
                    &remote_repo,
                    &workspace_id,
                    Path::new("images"),
                    DEFAULT_PAGE_NUM,
                    DEFAULT_PAGE_SIZE,
                )
                .await?;
                assert_eq!(entries.added_files.entries.len(), 0, "{branch_name}");
                assert_eq!(entries.added_files.total_entries, 0, "{branch_name}");
            }

            Ok(remote_repo)
        })
        .await
    }
}
