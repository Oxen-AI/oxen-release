use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;

use std::path::Path;

pub async fn rm_file(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let file_name = path.as_ref().to_string_lossy();
    let uri = format!("/staging/{identifier}/file/{branch_name}/{file_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("rm_file {}", url);
    let client = client::new_for_url(&url)?;
    match client.delete(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("rm_file got body: {}", body);
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

    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::test;
    use crate::{api, constants};

    use std::path::Path;

    #[tokio::test]
    async fn test_rm_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let identifier = UserConfig::identifier()?;
            let directory_name = "images";
            let path = test::test_img_file();
            let result = api::remote::workspace::add_file(
                &remote_repo,
                branch_name,
                &identifier,
                directory_name,
                path,
            )
            .await;
            assert!(result.is_ok());

            // Remove the file
            let result = api::remote::workspace::rm_file(
                &remote_repo,
                branch_name,
                &identifier,
                result.unwrap(),
            )
            .await;
            assert!(result.is_ok());

            // Make sure we have 0 files staged
            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new(directory_name);
            let entries = api::remote::workspace::status(
                &remote_repo,
                branch_name,
                &identifier,
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
