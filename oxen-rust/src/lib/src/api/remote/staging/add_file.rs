use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::FilePathsResponse;

use std::path::PathBuf;

pub async fn add_file(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    directory_name: &str,
    path: PathBuf,
) -> Result<PathBuf, OxenError> {
    let uri = format!("/staging/{identifier}/file/{branch_name}/{directory_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let file_name = path
        .file_name()
        .unwrap()
        .to_os_string()
        .into_string()
        .ok()
        .unwrap();
    let file = std::fs::read(&path).unwrap();
    let file_part = reqwest::multipart::Part::bytes(file).file_name(file_name);
    let form = reqwest::multipart::Form::new().part("file", file_part);
    let client = client::new_for_url(&url)?;
    match client.post(&url).multipart(form).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<FilePathsResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    let path = val.paths[0].clone();
                    Ok(path)
                }
                Err(err) => {
                    let err = format!("api::staging::add_file error parsing response from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::add_file Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn add_files(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    directory_name: &str,
    paths: Vec<PathBuf>,
) -> Result<Vec<PathBuf>, OxenError> {
    let uri = format!("/staging/{identifier}/file/{branch_name}/{directory_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let mut form = reqwest::multipart::Form::new();
    for path in paths {
        let file_name = path
            .file_name()
            .unwrap()
            .to_os_string()
            .into_string()
            .ok()
            .unwrap();
        let file = std::fs::read(&path).unwrap();
        let file_part = reqwest::multipart::Part::bytes(file).file_name(file_name);
        form = form.part("file", file_part);
    }

    let client = client::new_for_url(&url)?;
    match client.post(&url).multipart(form).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<FilePathsResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.paths),
                Err(err) => {
                    let err = format!("api::staging::add_files error parsing response from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::add_files Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::opts::CloneOpts;
    use crate::{api, constants};
    use crate::{command, test};

    use std::path::Path;

    #[tokio::test]
    async fn test_stage_single_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let directory_name = "images";
            let identifier = UserConfig::identifier()?;
            let path = test::test_img_file();
            let result = api::remote::staging::add_file(
                &remote_repo,
                branch_name,
                &identifier,
                directory_name,
                path,
            )
            .await;
            assert!(result.is_ok());

            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new(directory_name);
            let entries = api::remote::staging::status(
                &remote_repo,
                branch_name,
                &identifier,
                path,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.added_files.entries.len(), 1);
            assert_eq!(entries.added_files.total_entries, 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_stage_multiple_files() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-data";
            let branch = api::remote::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let identifier = UserConfig::identifier()?;
            let directory_name = "data";
            let paths = vec![
                test::test_img_file(),
                test::test_img_file_with_name("cole_anthony.jpeg"),
            ];
            let result = api::remote::staging::add_files(
                &remote_repo,
                branch_name,
                &identifier,
                directory_name,
                paths,
            )
            .await;
            assert!(result.is_ok());

            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new(directory_name);
            let entries = api::remote::staging::status(
                &remote_repo,
                branch_name,
                &identifier,
                path,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.added_files.entries.len(), 2);
            assert_eq!(entries.added_files.total_entries, 2);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_staged_single_file_and_pull() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-data";
            let branch = api::remote::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let identifier = UserConfig::identifier()?;
            let file_to_post = test::test_img_file();
            let directory_name = "data";
            let result = api::remote::staging::add_file(
                &remote_repo,
                branch_name,
                &identifier,
                directory_name,
                file_to_post,
            )
            .await;
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add one image".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            let commit =
                api::remote::staging::commit(&remote_repo, branch_name, &identifier, &body).await?;

            let remote_commit = api::remote::commits::get_by_id(&remote_repo, &commit.id).await?;
            assert!(remote_commit.is_some());
            assert_eq!(commit.id, remote_commit.unwrap().id);

            let remote_repo_cloned = remote_repo.clone();
            test::run_empty_dir_test_async(|cloned_repo_dir| async move {
                // Clone repo
                let opts = CloneOpts::new(remote_repo.remote.url, cloned_repo_dir.join("new_repo"));
                let cloned_repo = command::clone(&opts).await?;

                // Make sure that image is not on main branch
                let path = cloned_repo
                    .path
                    .join(directory_name)
                    .join(test::test_img_file().file_name().unwrap());
                assert!(!path.exists());

                // Pull the branch with new data
                command::pull_remote_branch(&cloned_repo, DEFAULT_REMOTE_NAME, "add-data", true)
                    .await?;

                // We should have the commit locally
                let local_commit = api::local::commits::head_commit(&cloned_repo)?;
                assert_eq!(local_commit.id, commit.id);

                // The file should exist locally
                println!("Looking for file at path: {:?}", path);
                assert!(path.exists());

                Ok(cloned_repo_dir)
            })
            .await?;

            Ok(remote_repo_cloned)
        })
        .await
    }
}
