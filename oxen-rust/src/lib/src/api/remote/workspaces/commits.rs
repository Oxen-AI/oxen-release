use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::{Branch, Commit, NewCommitBody, RemoteRepository};
use crate::view::CommitResponse;

pub async fn commit_file(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    commit: &NewCommitBody,
    file_path: &str,
) -> Result<Commit, OxenError> {
    let uri = format!("/workspaces/{identifier}/commit/{branch_name}/{file_path}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let body = serde_json::to_string(&commit).unwrap();
    log::debug!("commit_staged {}\n{}", url, body);

    let client = client::new_for_url(&url)?;
    let res = client
        .post(&url)
        .body(reqwest::Body::from(body))
        .send()
        .await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("commit_staged got body: {}", body);
    let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => {
            let commit = val.commit;
            // make sure to call our /complete call to kick off the post-push hooks
            let branch = Branch {
                name: branch_name.to_string(),
                commit_id: commit.id.clone(),
                is_head: false
            };
            api::remote::commits::post_push_complete(remote_repo, &branch, &commit.id).await?;
            api::remote::repositories::post_push(remote_repo, &branch, &commit.id).await?;
            Ok(commit)
        },
        Err(err) => Err(OxenError::basic_str(format!(
            "api::staging::commit_staged error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn commit(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    commit: &NewCommitBody,
) -> Result<Commit, OxenError> {
    let uri = format!("/workspaces/{identifier}/commit/{branch_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let body = serde_json::to_string(&commit).unwrap();
    log::debug!("commit_staged {}\n{}", url, body);

    let client = client::new_for_url(&url)?;
    let res = client
        .post(&url)
        .body(reqwest::Body::from(body))
        .send()
        .await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("commit_staged got body: {}", body);
    let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => {
            let commit = val.commit;
            // make sure to call our /complete call to kick off the post-push hooks
            let branch = Branch {
                name: branch_name.to_string(),
                commit_id: commit.id.clone(),
                is_head: false
            };
            api::remote::commits::post_push_complete(remote_repo, &branch, &commit.id).await?;
            api::remote::repositories::post_push(remote_repo, &branch, &commit.id).await?;
            Ok(commit)
        },
        Err(err) => Err(OxenError::basic_str(format!(
            "api::staging::commit_staged error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::test;

    #[tokio::test]
    async fn test_commit_staged_multiple_files() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-data";
            let branch = api::remote::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let directory_name = "data";
            let paths = vec![
                test::test_img_file(),
                test::test_img_file_with_name("cole_anthony.jpeg"),
            ];
            let result = api::remote::workspaces::files::add_many(
                &remote_repo,
                &workspace_id,
                directory_name,
                paths,
            )
            .await;
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add staged data".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            let commit =
                api::remote::workspaces::commit(&remote_repo, branch_name, &workspace_id, &body)
                    .await?;

            let remote_commit = api::remote::commits::get_by_id(&remote_repo, &commit.id).await?;
            assert!(remote_commit.is_some());
            assert_eq!(commit.id, remote_commit.unwrap().id);

            println!("DONE!!");

            Ok(remote_repo)
        })
        .await
    }
}
