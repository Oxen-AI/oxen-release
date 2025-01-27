use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::commit::NewCommitBody;
use crate::model::RemoteRepository;
use crate::view::CommitResponse;

use reqwest::multipart::{Form, Part};
use std::path::Path;

pub async fn create_or_update(
    remote_repo: &RemoteRepository,
    branch: &str,
    directory: &str,
    file_path: &Path,
    commit_body: Option<NewCommitBody>,
) -> Result<CommitResponse, OxenError> {
    let uri = format!("/file/{branch}/{directory}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let file_part = Part::file(file_path).await?;
    let form = Form::new().part("file", file_part);

    let mut req = client.put(&url).multipart(form);

    if let Some(body) = commit_body {
        req = req
            .header("oxen-commit-author", body.author)
            .header("oxen-commit-email", body.email)
            .header("oxen-commit-message", body.message);
    }

    let res = req.send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: CommitResponse = serde_json::from_str(&body)?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::{api, repositories, test};

    #[tokio::test]
    async fn test_update_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let branch_name = "main";
            let directory_name = "test_data";
            let file_path = test::test_img_file();
            let commit_body = NewCommitBody {
                author: "Test Author".to_string(),
                email: "test@example.com".to_string(),
                message: "Update file test".to_string(),
            };

            let response = api::client::file::create_or_update(
                &remote_repo,
                branch_name,
                directory_name,
                &file_path,
                Some(commit_body),
            )
            .await?;

            assert_eq!(response.status.status_message, "resource_created");

            // Pull changes from remote to local repo
            repositories::pull(&local_repo).await?;

            // Check that the file exists in the local repo after pulling
            let file_path_in_repo = local_repo
                .path
                .join(directory_name)
                .join(file_path.file_name().unwrap());
            assert!(file_path_in_repo.exists());

            Ok(remote_repo)
        })
        .await
    }
}
