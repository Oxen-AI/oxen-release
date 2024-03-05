//! Fetch metadata about a resource from the remote.
//!

use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::{JsonDataFrameViewResponse, MetadataEntryResponse};

use std::path::Path;

/// Get the metadata about a resource from the remote.
pub async fn get_file(
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
pub async fn list_dir(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<JsonDataFrameViewResponse, OxenError> {
    let path = path.as_ref().to_string_lossy();
    let revision = revision.as_ref();
    let uri = format!("/meta/dir/{}/{}", revision, path);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let response = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    Ok(serde_json::from_str(&body)?)
}

/// Aggregate metadata about a resource from the remote.
pub async fn agg_dir(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
) -> Result<JsonDataFrameViewResponse, OxenError> {
    let path = path.as_ref().to_string_lossy();
    let revision = revision.as_ref();
    let column = column.as_ref();
    let uri = format!("/meta/agg/dir/{revision}/{path}?column={column}");
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
    use crate::core::index::CommitEntryReader;
    use crate::error::OxenError;
    use crate::model::EntryDataType;
    use crate::test;
    use crate::view::{JsonDataFrameViewResponse, MetadataEntryResponse};

    use std::path::Path;

    #[tokio::test]
    async fn test_get_file_entry() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let path = Path::new("annotations").join("README.md");
            let revision = DEFAULT_BRANCH_NAME;

            let head = api::local::commits::head_commit(&local_repo)?;

            let commit_entry_reader = CommitEntryReader::new(&local_repo, &head)?;

            // Try to get the entry from the local repo
            let entry = commit_entry_reader.get_entry(&path)?;
            assert!(entry.is_some());

            let entry = api::remote::metadata::get_file(&remote_repo, revision, path)
                .await?
                .entry;

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
            let entry = api::remote::metadata::get_file(&remote_repo, revision, path)
                .await?
                .entry;

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
                api::remote::metadata::get_file(&remote_repo, branch, directory).await?;
            println!("meta: {:?}", meta);

            assert_eq!(meta.entry.mime_type, "inode/directory");
            assert_eq!(meta.entry.data_type, EntryDataType::Dir);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_metadata_table_list_dir() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let branch = DEFAULT_BRANCH_NAME;
            let directory = Path::new("train");

            let meta: JsonDataFrameViewResponse =
                api::remote::metadata::list_dir(&remote_repo, branch, directory).await?;

            let _df = meta.data_frame.view.to_df();

            assert_eq!(meta.data_frame.source.size.width, 11);
            assert_eq!(meta.data_frame.source.size.height, 5);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_metadata_table_agg_dir() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let branch = DEFAULT_BRANCH_NAME;
            let directory = Path::new("");

            let meta: JsonDataFrameViewResponse =
                api::remote::metadata::agg_dir(&remote_repo, branch, directory, "data_type")
                    .await?;
            println!("meta: {:?}", meta);

            let df = meta.data_frame.view.to_df();
            println!("df: {:?}", df);

            // df: shape: (4, 2)
            // ┌───────────┬───────┐
            // │ data_type ┆ count │
            // │ ---       ┆ ---   │
            // │ str       ┆ i64   │
            // ╞═══════════╪═══════╡
            // │ directory ┆ 8     │
            // │ image     ┆ 5     │
            // │ tabular   ┆ 7     │
            // │ text      ┆ 4     │
            // └───────────┴───────┘

            assert_eq!(meta.data_frame.source.size.width, 2);
            assert_eq!(meta.data_frame.source.size.height, 4);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_metadata_table_agg_train_dir() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let branch = DEFAULT_BRANCH_NAME;
            let directory = Path::new("train");

            let meta: JsonDataFrameViewResponse =
                api::remote::metadata::agg_dir(&remote_repo, branch, directory, "data_type")
                    .await?;
            println!("meta: {:?}", meta);

            let df = meta.data_frame.view.to_df();
            println!("df: {:?}", df);

            /*
            df: shape: (1, 2)
            ┌───────────┬───────┐
            │ data_type ┆ count │
            │ ---       ┆ ---   │
            │ str       ┆ i64   │
            ╞═══════════╪═══════╡
            │ Image     ┆ 5     │
            └───────────┴───────┘
            */

            assert_eq!(meta.data_frame.source.size.width, 2);
            assert_eq!(meta.data_frame.source.size.height, 1);

            // make sure that there are 5 images in the polars dataframe
            let df_str = format!("{:?}", df);
            assert_eq!(
                df_str,
                r"shape: (1, 2)
┌───────────┬───────┐
│ data_type ┆ count │
│ ---       ┆ ---   │
│ str       ┆ i64   │
╞═══════════╪═══════╡
│ image     ┆ 5     │
└───────────┴───────┘"
            );

            Ok(remote_repo)
        })
        .await
    }
}
