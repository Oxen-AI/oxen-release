//! Interact with the remote repository to get information about mergability of branches
//!

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::merge::{MergeSuccessResponse, Mergeable, MergeableResponse};

/// Can check the mergability of base into head
/// base or head are strings that can be branch names or commit ids
pub async fn mergability(
    remote_repo: &RemoteRepository,
    base: &str,
    head: &str,
) -> Result<Mergeable, OxenError> {
    let uri = format!("/merge/{base}..{head}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("api::client::merger::mergability url: {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: MergeableResponse = serde_json::from_str(&body)?;
    Ok(response.mergeable)
}

/// Merge the head branch into the base branch
pub async fn merge(
    remote_repo: &RemoteRepository,
    base: &str,
    head: &str,
) -> Result<(), OxenError> {
    let uri = format!("/merge/{base}..{head}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("api::client::merger::merge url: {url}");

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let _response: MergeSuccessResponse = serde_json::from_str(&body)?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::error::OxenError;
    use crate::opts::FetchOpts;
    use crate::repositories;
    use crate::test;

    #[tokio::test]
    async fn test_remote_merger_no_commits() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let base = "main";
            let head = "add-data";

            repositories::branches::create_checkout(&local_repo, head)?;
            repositories::push::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, head).await?;

            let mergability = api::client::merger::mergability(&remote_repo, base, head).await?;

            assert!(mergability.is_mergeable);
            // Only one commit in the history, the head
            assert_eq!(mergability.commits.len(), 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_merger_base_is_ahead() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let base = "main";
            let head = "add-data";

            repositories::branches::create_checkout(&local_repo, head)?;
            repositories::push::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, head).await?;

            // Checkout main and add a file to be ahead
            repositories::checkout(&local_repo, base).await?;
            let path = local_repo.path.join("file_1.txt");
            test::write_txt_file_to_path(&path, "hello")?;
            repositories::add(&local_repo, &path)?;
            repositories::commit(&local_repo, "adding file 1")?;
            repositories::push::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, base).await?;

            let mergability = api::client::merger::mergability(&remote_repo, base, head).await?;

            assert!(mergability.is_mergeable);
            assert_eq!(mergability.commits.len(), 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_merger_mergable_multiple_commits() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let base = "main";
            let head = "add-data";

            repositories::branches::create_checkout(&local_repo, head)?;
            repositories::push::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, head).await?;

            // Modify README.md
            let path = local_repo.path.join("README.md");
            test::write_txt_file_to_path(&path, "I am the README now")?;
            repositories::add(&local_repo, &path)?;

            // Commit twice
            let path = local_repo.path.join("file_1.txt");
            test::write_txt_file_to_path(&path, "hello")?;
            repositories::add(&local_repo, &path)?;
            repositories::commit(&local_repo, "adding file 1")?;

            let path = local_repo.path.join("file_2.txt");
            test::write_txt_file_to_path(&path, "world")?;
            repositories::add(&local_repo, &path)?;
            repositories::commit(&local_repo, "adding file 2")?;

            // Push commits
            repositories::push::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, head).await?;

            let mergability = api::client::merger::mergability(&remote_repo, base, head).await?;

            println!("Got {} commits", mergability.commits.len());
            for commit in &mergability.commits {
                println!("mergability commit: {:?}", commit);
            }

            assert!(mergability.is_mergeable);
            assert_eq!(mergability.commits.len(), 3);
            assert_eq!(mergability.conflicts.len(), 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_merger_multiple_commits_conflict_head_is_ahead() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let base = "main";
            let head = "add-data";

            repositories::branches::create_checkout(&local_repo, head)?;
            repositories::push::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, head).await?;

            // Modify README.md to have a conflict
            let path = local_repo.path.join("README.md");
            test::write_txt_file_to_path(&path, "I am the README now")?;
            repositories::add(&local_repo, &path)?;

            // Commit twice
            let path = local_repo.path.join("file_1.txt");
            test::write_txt_file_to_path(&path, "hello")?;
            repositories::add(&local_repo, &path)?;
            repositories::commit(&local_repo, "adding file 1")?;

            let path = local_repo.path.join("file_2.txt");
            test::write_txt_file_to_path(&path, "world")?;
            repositories::add(&local_repo, &path)?;
            repositories::commit(&local_repo, "adding file 2")?;

            // Push commits

            repositories::push::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, head).await?;

            // Checkout main and modify README.md to have a conflict
            repositories::checkout(&local_repo, base).await?;
            let path = local_repo.path.join("README.md");
            test::write_txt_file_to_path(&path, "I am on main conflicting the README")?;
            repositories::add(&local_repo, &path)?;
            repositories::commit(&local_repo, "modifying readme on main")?;

            repositories::push::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, base).await?;

            let mergability = api::client::merger::mergability(&remote_repo, base, head).await?;

            assert!(!mergability.is_mergeable);
            assert_eq!(mergability.commits.len(), 3);
            assert_eq!(mergability.conflicts.len(), 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_merger_merge() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let base = "main";
            let head = "add-data";

            repositories::branches::create_checkout(&local_repo, head)?;
            repositories::push::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, head).await?;

            // Modify a file on the head branch
            let new_file_name = "merge_file.txt";
            let path = local_repo.path.join(new_file_name);
            test::write_txt_file_to_path(&path, "hello")?;
            repositories::add(&local_repo, &path)?;
            repositories::commit(&local_repo, "adding file")?;
            repositories::push::push_remote_branch(&local_repo, DEFAULT_REMOTE_NAME, head).await?;

            // Merge the head branch into base
            api::client::merger::merge(&remote_repo, base, head).await?;

            // Verify the merge commit exists on the base branch
            repositories::checkout(&local_repo, base).await?;
            let commits_before = repositories::commits::list(&local_repo)?;
            let fetch_opts = FetchOpts::new();
            repositories::pull::pull_remote_branch(&local_repo, &fetch_opts).await?;

            let commits_after = repositories::commits::list(&local_repo)?;
            assert!(commits_after.len() > commits_before.len());

            // verify the added file is in the base branch
            let path = local_repo.path.join(new_file_name);
            assert!(path.exists());

            Ok(remote_repo)
        })
        .await
    }
}
