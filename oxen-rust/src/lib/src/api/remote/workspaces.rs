pub mod changes;
pub mod commits;
pub mod data_frames;
pub mod files;

pub use commits::commit;

use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::workspaces::{NewWorkspace, WorkspaceResponse};
use crate::view::WorkspaceResponseView;

pub async fn create(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    workspace_id: impl AsRef<str>,
) -> Result<WorkspaceResponse, OxenError> {
    let branch_name = branch_name.as_ref();
    let workspace_id = workspace_id.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, "/workspaces")?;
    log::debug!("create workspace {}\n", url);

    let body = serde_json::to_string(&NewWorkspace {
        branch_name: branch_name.to_string(),
        workspace_id: workspace_id.to_string(),
    })?;

    let client = client::new_for_url(&url)?;
    let res = client
        .put(&url)
        .body(reqwest::Body::from(body))
        .send()
        .await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("create workspace got body: {}", body);
    let response: Result<WorkspaceResponseView, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.workspace),
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn delete(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
) -> Result<WorkspaceResponse, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, &format!("/workspaces/{workspace_id}"))?;
    log::debug!("delete workspace {}\n", url);

    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("delete workspace got body: {}", body);
    let response: Result<WorkspaceResponseView, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.workspace),
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::error::OxenError;
    use crate::test;

    #[tokio::test]
    async fn test_create_workspace() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = "main";
            let workspace_id = "test_workspace_id";
            let workspace = create(&remote_repo, branch_name, workspace_id).await?;

            assert_eq!(workspace.workspace_id, workspace_id);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_workspace() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = "main";
            let workspace_id = "test_workspace_id";
            let workspace = create(&remote_repo, branch_name, workspace_id).await?;

            assert_eq!(workspace.workspace_id, workspace_id);

            let res = delete(&remote_repo, workspace_id).await;
            assert!(res.is_ok());

            Ok(remote_repo)
        })
        .await
    }
}
