use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::{DirEntry, RemoteRepository};
use crate::view::{RemoteStagedStatus, RemoteStagedStatusResponse};

use std::path::Path;

pub async fn list_staging_dir(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    path: &Path,
    page: usize,
    page_size: usize,
) -> Result<RemoteStagedStatus, OxenError> {
    let path_str = path.to_str().unwrap();
    let uri = format!("/staging/dir/{branch_name}/{path_str}?page={page}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        log::debug!("list_staging_dir got body: {}", body);
        let response: Result<RemoteStagedStatusResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val.staged),
            Err(err) => Err(OxenError::basic_str(format!(
                "api::dir::list_staging_dir error parsing response from {url}\n\nErr {err:?} \n\n{body}"
            ))),
        }
    } else {
        let err = format!("api::dir::list_staging_dir Err request failed: {url}");
        Err(OxenError::basic_str(err))
    }
}

pub fn stage_file(_path: &Path) -> Result<Vec<DirEntry>, OxenError> {
    Ok(vec![])
}

#[cfg(test)]
mod tests {

    use crate::{api, command, constants};
    use crate::error::OxenError;
    use crate::test;

    use std::path::Path;

    #[tokio::test]
    async fn test_list_empty_staging_dir_empty_remote() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            let branch_name = "add-images";
            command::create_checkout_branch(&local_repo, branch_name)?;
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::add_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;
            command::push_remote_branch(&local_repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            let branch = api::remote::branches::create_or_get(&remote_repo, branch_name).await?;
            assert_eq!(branch.name, branch_name);

            let page_num = 0;
            let page_size = 10;
            let path = Path::new("images");
            let entries = api::remote::staging::list_staging_dir(
                &remote_repo,
                branch_name,
                path,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.added_files.entries.len(), 0);
            assert_eq!(entries.added_files.total_entries, 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_empty_staging_dir_all_data_pushed() -> Result<(), OxenError> {
        test::run_remote_repo_test_all_data_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_or_get(&remote_repo, branch_name).await?;
            assert_eq!(branch.name, branch_name);

            let page_num = 0;
            let page_size = 10;
            let path = Path::new("images");
            let entries = api::remote::staging::list_staging_dir(
                &remote_repo,
                branch_name,
                path,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.added_files.entries.len(), 0);
            assert_eq!(entries.added_files.total_entries, 0);

            Ok(remote_repo)
        })
        .await
    }
}
