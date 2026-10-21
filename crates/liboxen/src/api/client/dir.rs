use std::path::Path;

use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::model::metadata::MetadataDir;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::opts::SortOpts;
use crate::view::entries::EMetadataEntry;
use crate::view::{PaginatedDirEntries, PaginatedDirEntriesResponse};
use crate::{api, constants};

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
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
    page: usize,
    page_size: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    list_with_opts(remote_repo, revision, path, page, page_size, None, None).await
}

pub async fn list_with_opts(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
    page: usize,
    page_size: usize,
    sort_opts: Option<&SortOpts>,
    depth: Option<isize>,
) -> Result<PaginatedDirEntries, OxenError> {
    let revision = revision.as_ref();
    let path = path.as_ref().to_string_lossy();
    let mut query_params = vec![format!("page={page}"), format!("page_size={page_size}")];
    if let Some(sort_opts) = sort_opts {
        query_params.push(format!("sort_by={}", sort_opts.sort_by));
        if sort_opts.reverse {
            query_params.push("reverse=true".to_string());
        }
    }
    if let Some(depth) = depth {
        query_params.push(format!("depth={depth}"));
    }
    let uri = format!("/dir/{revision}/{path}?{}", query_params.join("&"));
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<PaginatedDirEntries, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::dir::list_dir error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn file_counts(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<MetadataDir, OxenError> {
    let path_str = path.as_ref().to_string_lossy();
    let response = list(remote_repo, revision, &path, 1, 1).await?;
    match response.dir {
        Some(dir_entry) => match dir_entry {
            EMetadataEntry::MetadataEntry(metadata_entry) => match metadata_entry.metadata {
                Some(GenericMetadata::MetadataDir(metadata)) => Ok(metadata),
                _ => Err(OxenError::basic_str(format!(
                    "No metadata on directory found at {path_str}"
                ))),
            },
            EMetadataEntry::WorkspaceMetadataEntry(_) => Err(OxenError::basic_str(
                "Workspace metadata entry is not implemented",
            )),
        },
        None => Err(OxenError::basic_str(format!(
            "No directory found at {path_str}"
        ))),
    }
}

pub async fn get_dir(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<PaginatedDirEntriesResponse, OxenError> {
    let path_str = path.as_ref().to_string_lossy();
    let revision = revision.as_ref();
    let uri = format!("/dir/{revision}/{path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<PaginatedDirEntriesResponse, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::dir::get_dir error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{get_dir, list, list_with_opts};
    use crate::api;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::{MetadataEntry, StagedEntryStatus};
    use crate::opts::{SortBy, SortOpts};
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::view::entries::EMetadataEntry;

    use std::path::Path;
    use std::path::PathBuf;

    /// The listed entry called `name`, which must be a committed entry.
    fn committed_entry<'a>(entries: &'a [EMetadataEntry], name: &str) -> &'a MetadataEntry {
        match entries.iter().find(|entry| entry.filename() == name) {
            Some(EMetadataEntry::MetadataEntry(meta)) => meta,
            other => panic!("expected a committed entry called {name}, got {other:?}"),
        }
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_list_dir() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            let readme_commit = repositories::commits::head_commit(&local_repo)?;
            let root_path = Path::new("");
            let root_entries = list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 1);
            let readme = committed_entry(&root_entries.entries, "README.md");
            assert_eq!(readme.latest_commit.as_ref().unwrap().id, readme_commit.id);
            let resource = readme.resource.as_ref().unwrap();
            assert!(resource.branch.is_some(), "the resource records its branch");
            assert_eq!(resource.path, Path::new("README.md"));

            // Each entry reports the latest commit that changed it.
            let file_path = local_repo.path.join("file.txt");
            util::fs::write_to_path(&file_path, "Hello, World!")?;
            repositories::add(&local_repo, file_path).await?;
            let file_commit = repositories::commit(&local_repo, "Add file.txt")?;
            repositories::push(&local_repo).await?;
            let root_entries = list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 2);

            let data_dir = local_repo.path.join("data");
            util::fs::create_dir_all(&data_dir)?;
            util::fs::write_to_path(data_dir.join("file1.txt"), "Hello, World 1!")?;
            util::fs::write_to_path(data_dir.join("file2.txt"), "Hello, World 2!")?;
            repositories::add(&local_repo, &data_dir).await?;
            let data_commit = repositories::commit(&local_repo, "Add data dir")?;
            repositories::push(&local_repo).await?;
            let root_entries = list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 3);
            let data = committed_entry(&root_entries.entries, "data");
            assert_eq!(data.latest_commit.as_ref().unwrap().id, data_commit.id);
            let file = committed_entry(&root_entries.entries, "file.txt");
            assert_eq!(file.latest_commit.as_ref().unwrap().id, file_commit.id);

            let a_data_name = Path::new("a_data");
            let a_data_dir = local_repo.path.join(a_data_name);
            util::fs::create_dir_all(&a_data_dir)?;
            util::fs::write_to_path(a_data_dir.join("file3.txt"), "Hello, World 3!")?;
            util::fs::write_to_path(a_data_dir.join("file4.txt"), "Hello, World 4!")?;
            repositories::add(&local_repo, &a_data_dir).await?;
            let a_data_commit = repositories::commit(&local_repo, "Add a_data dir")?;
            repositories::push(&local_repo).await?;
            let root_entries = list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 4);
            let a_data = committed_entry(&root_entries.entries, "a_data");
            assert_eq!(a_data.latest_commit.as_ref().unwrap().id, a_data_commit.id);

            let sub_data_dir = a_data_dir.join("sub_data");
            util::fs::create_dir_all(&sub_data_dir)?;
            util::fs::write_to_path(sub_data_dir.join("file5.txt"), "Hello, World 5!")?;
            util::fs::write_to_path(sub_data_dir.join("file6.txt"), "Hello, World 6!")?;
            repositories::add(&local_repo, &sub_data_dir).await?;
            let sub_data_commit = repositories::commit(&local_repo, "Add sub_data dir")?;
            repositories::push(&local_repo).await?;
            let sub_entries = list(&remote_repo, DEFAULT_BRANCH_NAME, a_data_name, 1, 10).await?;
            let sub_data = committed_entry(&sub_entries.entries, "sub_data");
            assert_eq!(
                sub_data.latest_commit.as_ref().unwrap().id,
                sub_data_commit.id
            );

            // A directory and a file whose names need encoding in a URL.
            let encoded_dir = local_repo.path.join("dir=dir");
            util::fs::create_dir_all(&encoded_dir)?;
            let file_path = encoded_dir.join("file example.txt");
            util::fs::write_to_path(&file_path, "Hello World")?;
            repositories::add(&local_repo, &file_path).await?;
            repositories::commit(&local_repo, "Adding dir=dir")?;
            repositories::push(&local_repo).await?;

            let dir_response = get_dir(&remote_repo, DEFAULT_BRANCH_NAME, "dir=dir").await?;
            assert_eq!(dir_response.status.status, "success");
            match &dir_response.entries.dir {
                Some(EMetadataEntry::MetadataEntry(dir)) => {
                    assert_eq!(dir.filename, "dir=dir");
                    assert!(dir.is_dir);
                }
                other => panic!("expected the dir=dir entry, got {other:?}"),
            }
            let file = committed_entry(&dir_response.entries.entries, "file example.txt");
            assert!(!file.is_dir);

            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_get_dir_with_workspace() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let file_path = PathBuf::from("annotations")
                .join("train")
                .join("file.txt")
                .to_str()
                .unwrap()
                .to_string();
            let workspace_id = "test_workspace_id";
            let directory_name = PathBuf::from("annotations")
                .join("train")
                .to_str()
                .unwrap()
                .to_string();

            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);

            let full_path = local_repo.path.join(file_path);
            util::fs::write(&full_path, b"test content")?;

            let _result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name.clone(),
                &full_path,
            )
            .await;

            let file_path = test::test_bounding_box_csv();
            let full_path = local_repo.path.join(file_path);
            util::fs::write(&full_path, "name,age\nAlice,30\nBob,25\n")?;

            let _result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                &full_path,
            )
            .await;
            let train_path = PathBuf::from("annotations")
                .join("train")
                .to_str()
                .unwrap()
                .to_string();
            let response =
                api::client::dir::get_dir(&remote_repo, workspace_id, train_path).await?;

            for entry in response.entries.entries.iter() {
                if let EMetadataEntry::WorkspaceMetadataEntry(ws_entry) = entry {
                    match ws_entry.filename.as_str() {
                        "bounding_box.csv" => {
                            assert_eq!(
                                ws_entry.changes.as_ref().unwrap().status,
                                StagedEntryStatus::Modified,
                                "Expected bounding_box.csv to be Modified"
                            );
                        }
                        "file.txt" => {
                            assert_eq!(
                                ws_entry.changes.as_ref().unwrap().status,
                                StagedEntryStatus::Added,
                                "Expected file.txt to be Added"
                            );
                        }
                        _ => {}
                    }
                }
            }
            Ok(remote_repo)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_list_dir_sort_opts() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let remote_repo = test::connect_remote_repo(&mut local_repo).await?;

            let dir_a = local_repo.path.join("dir_a");
            let dir_a_subdir = dir_a.join("subdir");
            let dir_b = local_repo.path.join("dir_b");
            util::fs::create_dir_all(&dir_a_subdir)?;
            util::fs::create_dir_all(&dir_b)?;
            util::fs::write(local_repo.path.join("a_file.txt"), "content a")?;
            util::fs::write(dir_a.join("file_a1.txt"), "a1 content")?;
            util::fs::write(dir_a_subdir.join("file_sub.txt"), "sub content")?;
            util::fs::write(dir_b.join("file_b1.txt"), "b1 content")?;
            repositories::add(&local_repo, &local_repo.path).await?;
            repositories::commit(&local_repo, "Adding nested structure")?;
            repositories::push(&local_repo).await?;

            util::fs::write(dir_a.join("file_a2.txt"), "a2 content")?;
            util::fs::write(local_repo.path.join("b_file.txt"), "content b")?;
            repositories::add(&local_repo, &local_repo.path).await?;
            repositories::commit(&local_repo, "Adding newer files")?;
            repositories::push(&local_repo).await?;

            let newest_first = SortOpts {
                sort_by: SortBy::Date,
                reverse: true,
            };
            let root = Path::new("");
            let entries = list_with_opts(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                root,
                1,
                10,
                Some(&newest_first),
                None,
            )
            .await?;
            let names: Vec<&str> = entries.entries.iter().map(|e| e.filename()).collect();
            let position = |name: &str| names.iter().position(|n| *n == name);
            assert!(
                position("b_file.txt").is_some() && position("b_file.txt") < position("a_file.txt"),
                "the newer file sorts first: {names:?}"
            );

            let nested = list_with_opts(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                root,
                1,
                100,
                Some(&newest_first),
                Some(1),
            )
            .await?;
            let dir_a = committed_entry(&nested.entries, "dir_a");
            let children = dir_a
                .children
                .as_ref()
                .expect("dir_a children not populated");
            let child_names: Vec<&str> = children
                .iter()
                .map(|child| child.filename.as_str())
                .collect();
            assert_eq!(child_names, vec!["subdir", "file_a2.txt", "file_a1.txt"]);
            assert!(
                children[0].children.is_none(),
                "a depth of 1 lists one level of children"
            );

            Ok(())
        })
        .await
    }
}
