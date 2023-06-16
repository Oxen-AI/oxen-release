//! Fetch metadata about a resource from the remote.
//!

use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::EntryMetaDataResponse;

use std::path::Path;

/// Get the metadata about a resource from the remote.
pub async fn get(
    remote_repo: &RemoteRepository,
    commit_or_branch: &str,
    path: impl AsRef<Path>,
) -> Result<EntryMetaDataResponse, OxenError> {
    let path = path.as_ref().to_string_lossy();
    let uri = format!("/meta/{commit_or_branch}/{path}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("list_page got body: {}", body);
            let response: Result<EntryMetaDataResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => Err(OxenError::basic_str(format!(
                    "api::dir::list_dir error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("api::dir::list_dir Err {err:?} request failed: {url}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::EntryDataType;
    use crate::test;
    use crate::view::EntryMetaDataResponse;

    use std::path::Path;

    #[tokio::test]
    async fn test_get_remote_metadata() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let branch = DEFAULT_BRANCH_NAME;
            let directory = Path::new("train");

            let meta: EntryMetaDataResponse =
                api::remote::stats::get(&remote_repo, branch, directory).await?;
            println!("meta: {:?}", meta);

            assert_eq!(meta.entry.mime_type, "inode/directory");
            assert_eq!(meta.entry.data_type, EntryDataType::Dir);

            assert!(meta.status.status == "ok");

            Ok(remote_repo)
        })
        .await
    }
}
