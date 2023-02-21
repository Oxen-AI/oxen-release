use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::{DirEntry, RemoteRepository};
use crate::view::PaginatedDirEntries;

use std::path::Path;

pub async fn list_staging_dir(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    path: &Path,
    page: usize,
    page_size: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    let path_str = path.to_str().unwrap();
    let uri = format!("/staging/dir/{branch_name}/{path_str}?page={page}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        // log::debug!("list_page got body: {}", body);
        let response: Result<PaginatedDirEntries, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val),
            Err(_) => Err(OxenError::basic_str(format!(
                "api::dir::list_dir {url} Err \n\n{body}"
            ))),
        }
    } else {
        let err = format!("api::dir::list_dir Err request failed: {url}");
        Err(OxenError::basic_str(err))
    }
}

pub fn stage_file(_path: &Path) -> Result<Vec<DirEntry>, OxenError> {
    Ok(vec![])
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::error::OxenError;
    use crate::test;

    use std::path::Path;

    #[tokio::test]
    async fn test_list_empty_staging_dir_empty_remote() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|remote_repo| async move {
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
            assert_eq!(entries.entries.len(), 0);
            assert_eq!(entries.total_entries, 0);

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
            assert_eq!(entries.entries.len(), 0);
            assert_eq!(entries.total_entries, 0);

            Ok(remote_repo)
        })
        .await
    }
}
