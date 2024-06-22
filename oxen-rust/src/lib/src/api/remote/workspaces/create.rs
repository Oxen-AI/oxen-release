use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::{WorkspaceResponseView, WorkspaceView};

// TODO: Implement this in the same fashion as the hub, but so it can all work end to end open source
pub async fn create(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    identifier: impl AsRef<str>,
    resource_path: impl AsRef<str>,
) -> Result<WorkspaceView, OxenError> {
    let branch_name = branch_name.as_ref();
    let identifier = identifier.as_ref();
    let resource_path = resource_path.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, "/workspaces")?;
    log::debug!("create workspace {}\n", url);

    let body = serde_json::to_string(&WorkspaceView {
        branch_name: branch_name.to_string(),
        identifier: identifier.to_string(),
        resource_path: resource_path.to_string(),
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test;

    #[tokio::test]
    async fn test_create_workspace() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "main";
            let identifier = "test_identifier";
            let resource_path = "test_resource_path";
            let workspace = create(&remote_repo, branch_name, identifier, resource_path).await?;

            assert_eq!(workspace.branch_name, branch_name);
            assert_eq!(workspace.identifier, identifier);
            assert_eq!(workspace.resource_path, resource_path);

            Ok(remote_repo)
        })
        .await
    }
}
