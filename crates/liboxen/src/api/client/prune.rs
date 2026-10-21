//! # Remote Prune
//!
//! Trigger prune operations on a remote repository.
//!

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::oxen_response::ErrorResponse;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct PruneRequest {
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PruneStats {
    pub nodes_scanned: usize,
    pub nodes_kept: usize,
    pub nodes_removed: usize,
    pub versions_scanned: usize,
    pub versions_kept: usize,
    pub versions_removed: usize,
    pub bytes_freed: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PruneResponse {
    pub status: String,
    pub status_message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorResponse>,
    pub stats: PruneStats,
}

/// Trigger a prune operation on a remote repository
///
/// # Arguments
/// * `remote_repo` - The remote repository to prune
/// * `dry_run` - If true, only report what would be removed without actually removing it
///
/// # Returns
/// Statistics about the prune operation
pub async fn prune(remote_repo: &RemoteRepository, dry_run: bool) -> Result<PruneStats, OxenError> {
    let uri = "/prune".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let request_body = PruneRequest { dry_run };

    match client.post(&url).json(&request_body).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {body}");
            let response: Result<PruneResponse, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got PruneResponse: {val:?}");
                    Ok(val.stats)
                }
                Err(err) => {
                    log::error!("Failed to parse PruneResponse from body: {body}");
                    Err(OxenError::basic_str(format!(
                        "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                    )))
                }
            }
        }
        Err(err) => {
            let err = format!("Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::prune;
    use crate::api::client::branches;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_prune() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let train_dir = local_repo.path.join("train");
            util::fs::create_dir_all(&train_dir)?;
            let file1 = train_dir.join("file1.txt");
            test::write_txt_file_to_path(&file1, "file1")?;
            repositories::add(&local_repo, &file1).await?;
            let initial_commit = repositories::commit(&local_repo, "add file1.txt")?;

            let remote_repo = test::connect_remote_repo(&mut local_repo).await?;
            repositories::push(&local_repo).await?;

            let stats = prune(&remote_repo, true).await?;
            assert_eq!(stats.nodes_removed, 0, "every node is referenced");
            assert_eq!(stats.versions_removed, 0, "every version is referenced");

            // A commit on a feature branch, pushed and then deleted on the remote, leaves data that
            // nothing references.
            let branch_name = "feature-branch";
            repositories::branches::create(&local_repo, branch_name, &initial_commit.id)?;
            repositories::checkout(&local_repo, branch_name).await?;
            let file2 = train_dir.join("file2.txt");
            test::write_txt_file_to_path(&file2, "file2")?;
            repositories::add(&local_repo, &file2).await?;
            repositories::commit(&local_repo, "add file2.txt")?;
            repositories::push(&local_repo).await?;

            repositories::checkout(&local_repo, "main").await?;
            branches::delete(&remote_repo, branch_name).await?;

            let stats = prune(&remote_repo, false).await?;
            assert!(
                stats.nodes_removed > 0 || stats.versions_removed > 0,
                "the deleted branch's data is removed"
            );

            Ok(())
        })
        .await
    }
}
