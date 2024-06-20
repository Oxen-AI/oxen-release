use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::{WorkspaceResponseView, WorkspaceView};

pub async fn create(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    identifier: impl AsRef<str>,
) -> Result<WorkspaceView, OxenError> {
    let branch_name = branch_name.as_ref();
    let identifier = identifier.as_ref();
    let uri = format!("/workspace");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("create workspace {}\n", url);

    let body = serde_json::to_string(&serde_json::json!({
        "branch_name": branch_name,
        "identifier": identifier
    }))?;

    let client = client::new_for_url(&url)?;
    let res = client
        .post(&url)
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
    async fn test_commit_staged_multiple_files() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            todo!();
            Ok(remote_repo)
        })
        .await
    }
}
