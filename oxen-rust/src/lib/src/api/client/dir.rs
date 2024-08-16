use std::path::Path;

use crate::api;
use crate::api::client;
use crate::constants;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::PaginatedDirEntries;

pub async fn list_root(remote_repo: &RemoteRepository) -> Result<PaginatedDirEntries, OxenError> {
    list(
        remote_repo,
        constants::DEFAULT_BRANCH_NAME,
        Path::new(""),
        1,
        1,
    )
    .await
}

pub async fn list(
    remote_repo: &RemoteRepository,
    commit_or_branch: &str,
    path: impl AsRef<Path>,
    page: usize,
    page_size: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    let path = path.as_ref().to_string_lossy();
    let uri = format!("/dir/{commit_or_branch}/{path}?page={page}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("list_page got body: {}", body);
            let response: Result<PaginatedDirEntries, serde_json::Error> =
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
    use crate::command;
    use crate::constants;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use std::path::Path;

    #[tokio::test]
    async fn test_list_dir_has_correct_commits() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let mut local_repo = local_repo;

            // Set the proper remote
            let name = local_repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push it
            command::push(&local_repo).await?;

            // Make sure we have no entries
            let root_path = Path::new("");
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 0);

            // Add a file
            let file_name = Path::new("file.txt");
            let file_path = local_repo.path.join(file_name);
            let file_content = "Hello, World!";
            util::fs::write_to_path(&file_path, file_content)?;
            repositories::add(&local_repo, file_path)?;

            // Commit it
            let first_commit = command::commit(&local_repo, "Add file.txt")?;

            // Push it
            command::push(&local_repo).await?;

            // Make sure we have one entry
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 1);

            // Add a dir
            let dir_name = Path::new("data");
            let dir_path = local_repo.path.join(dir_name);
            util::fs::create_dir_all(&dir_path)?;

            // Write some files to the dir
            let file1_path = dir_path.join("file1.txt");
            let file2_path = dir_path.join("file2.txt");
            let file1_content = "Hello, World 1!";
            let file2_content = "Hello, World 2!";
            util::fs::write_to_path(&file1_path, file1_content)?;
            util::fs::write_to_path(&file2_path, file2_content)?;
            repositories::add(&local_repo, &dir_path)?;

            // Commit it
            let second_commit = command::commit(&local_repo, "Add data dir")?;

            // Push it
            command::push(&local_repo).await?;

            // Make sure we have two entries
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 2);

            for entry in &root_entries.entries {
                println!("entry: {:?}", entry);
            }
            println!("----------------------");

            // Make sure the commit hashes are correct
            assert_eq!(
                root_entries.entries[0].latest_commit.as_ref().unwrap().id,
                second_commit.id
            );
            assert_eq!(
                root_entries.entries[1].latest_commit.as_ref().unwrap().id,
                first_commit.id
            );

            // Add a second dir
            let dir2_name = Path::new("a_data");
            let dir2_path = local_repo.path.join(dir2_name);
            util::fs::create_dir_all(&dir2_path)?;

            // Write some files to the dir
            let file3_path = dir2_path.join("file3.txt");
            let file4_path = dir2_path.join("file4.txt");
            let file3_content = "Hello, World 3!";
            let file4_content = "Hello, World 4!";
            util::fs::write_to_path(&file3_path, file3_content)?;
            util::fs::write_to_path(&file4_path, file4_content)?;
            repositories::add(&local_repo, &dir2_path)?;

            // Commit it
            let third_commit = command::commit(&local_repo, "Add a_data dir")?;

            // Push it
            command::push(&local_repo).await?;

            // Make sure we have three entries
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 3);

            for entry in &root_entries.entries {
                println!("entry: {:?}", entry);
            }

            // Make sure the commit hashes are correct
            assert_eq!(
                root_entries.entries[0].latest_commit.as_ref().unwrap().id,
                third_commit.id
            );
            assert_eq!(
                root_entries.entries[1].latest_commit.as_ref().unwrap().id,
                second_commit.id
            );
            assert_eq!(
                root_entries.entries[2].latest_commit.as_ref().unwrap().id,
                first_commit.id
            );

            // Add a sub directory to the second dir
            let dir3_name = Path::new("sub_data");
            let dir3_path = dir2_path.join(dir3_name);
            util::fs::create_dir_all(&dir3_path)?;

            // Write some files to the dir
            let file5_path = dir3_path.join("file5.txt");
            let file6_path = dir3_path.join("file6.txt");
            let file5_content = "Hello, World 5!";
            let file6_content = "Hello, World 6!";
            util::fs::write_to_path(&file5_path, file5_content)?;
            util::fs::write_to_path(&file6_path, file6_content)?;
            repositories::add(&local_repo, &dir3_path)?;

            // Commit it
            let fourth_commit = command::commit(&local_repo, "Add sub_data dir")?;

            // Push it
            command::push(&local_repo).await?;

            // Make sure the sub directory has the correct commit id
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(
                root_entries.entries[0].latest_commit.as_ref().unwrap().id,
                fourth_commit.id
            );

            Ok(())
        })
        .await
    }
}
