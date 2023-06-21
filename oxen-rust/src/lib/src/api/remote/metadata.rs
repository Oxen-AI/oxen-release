//! Fetch metadata about a resource from the remote.
//!

use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::{MetadataEntryResponse, JsonDataFrameSliceResponse};

use std::path::Path;

/// Get the metadata about a resource from the remote.
pub async fn get(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<MetadataEntryResponse, OxenError> {
    let path = path.as_ref().to_string_lossy();
    let revision = revision.as_ref();
    let uri = format!("/meta/{}/{}", revision, path);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let response = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    Ok(serde_json::from_str(&body)?)
}

/// Get the metadata about a resource from the remote.
pub async fn get_dir(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<JsonDataFrameSliceResponse, OxenError> {
    let path = path.as_ref().to_string_lossy();
    let revision = revision.as_ref();
    let uri = format!("/meta/dir/{}/{}", revision, path);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let response = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    Ok(serde_json::from_str(&body)?)
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::EntryDataType;
    use crate::test;
    use crate::view::{MetadataEntryResponse, JsonDataFrameSliceResponse};

    use std::path::Path;

    #[tokio::test]
    async fn test_get_file_entry() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let path = Path::new("annotations").join("README.md");
            let revision = DEFAULT_BRANCH_NAME;
            let entry = api::remote::metadata::get(&remote_repo, revision, path).await?.entry;

            assert_eq!(entry.filename, "README.md");
            assert!(!entry.is_dir);
            assert_eq!(entry.data_type, EntryDataType::Text);
            assert_eq!(entry.mime_type, "text/markdown");
            assert_eq!(entry.resource.unwrap().path, "annotations/README.md");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_dir_entry() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let path = "train";
            let revision = DEFAULT_BRANCH_NAME;
            let entry = api::remote::metadata::get(&remote_repo, revision, path).await?.entry;

            assert_eq!(entry.filename, path);
            assert!(entry.is_dir);
            assert_eq!(entry.data_type, EntryDataType::Dir);
            assert!(entry.size > 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_remote_metadata() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let branch = DEFAULT_BRANCH_NAME;
            let directory = Path::new("train");

            let meta: MetadataEntryResponse =
                api::remote::metadata::get(&remote_repo, branch, directory).await?;
            println!("meta: {:?}", meta);

            assert_eq!(meta.entry.mime_type, "inode/directory");
            assert_eq!(meta.entry.data_type, EntryDataType::Dir);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_remote_metadata_table() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let branch = DEFAULT_BRANCH_NAME;
            let directory = Path::new("train");

            let meta: JsonDataFrameSliceResponse =
                api::remote::metadata::get_dir(&remote_repo, branch, directory).await?;
            println!("meta: {:?}", meta);

            let df = meta.df.to_df();
            println!("df: {:?}", df);

            assert_eq!(meta.full_size.width, 3);
            assert_eq!(meta.full_size.height, 5);
            

            Ok(remote_repo)
        })
        .await
    }
}
