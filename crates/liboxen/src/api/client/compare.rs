use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{Commit, MerkleHash, RemoteRepository};
use crate::view::compare::{CompareCommitsResponse, CompareEntries, CompareTabularResponse};
use crate::view::compare::{
    TabularCompareBody, TabularCompareFieldBody, TabularCompareResourceBody,
    TabularCompareTargetBody,
};
use crate::view::diff::{DirDiffTreeSummary, DirTreeDiffResponse};
use crate::view::{CompareEntriesResponse, JsonDataFrameViewResponse};
use crate::view::{JsonDataFrameView, compare::CompareTabular};

use serde_json::json;

// TODO this should probably be cpath
#[allow(clippy::too_many_arguments)]
pub async fn create_compare(
    remote_repo: &RemoteRepository,
    compare_id: &str,
    left_path: &str,
    left_revision: &str,
    right_path: &str,
    right_revision: &str,
    keys: Vec<TabularCompareFieldBody>,
    compare: Vec<TabularCompareTargetBody>,
    display: Vec<TabularCompareTargetBody>,
) -> Result<CompareTabular, OxenError> {
    let req_body = TabularCompareBody {
        compare_id: compare_id.to_string(),
        left: TabularCompareResourceBody {
            path: left_path.to_string(),
            version: left_revision.to_string(),
        },
        right: TabularCompareResourceBody {
            path: right_path.to_string(),
            version: right_revision.to_string(),
        },
        keys,
        compare,
        display,
    };

    let uri = "/compare/data_frames".to_string();

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    let res = client.post(&url).json(&json!(req_body)).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<CompareTabularResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(tabular_compare) => Ok(tabular_compare.dfs),
        Err(err) => Err(OxenError::basic_str(format!(
            "create_compare() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn update_compare(
    remote_repo: &RemoteRepository,
    compare_id: &str,
    left_path: &str,
    left_revision: &str,
    right_path: &str,
    right_revision: &str,
    keys: Vec<TabularCompareFieldBody>,
    compare: Vec<TabularCompareTargetBody>,
    display: Vec<TabularCompareTargetBody>,
) -> Result<CompareTabular, OxenError> {
    let req_body = TabularCompareBody {
        compare_id: compare_id.to_string(),
        left: TabularCompareResourceBody {
            path: left_path.to_string(),
            version: left_revision.to_string(),
        },
        right: TabularCompareResourceBody {
            path: right_path.to_string(),
            version: right_revision.to_string(),
        },
        keys,
        compare,
        display,
    };

    let uri = format!("/compare/data_frames/{compare_id}");

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    if let Ok(res) = client.put(&url).json(&json!(req_body)).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<CompareTabularResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(tabular_compare) => Ok(tabular_compare.dfs),
            Err(err) => Err(OxenError::basic_str(format!(
                "update_compare() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str("update_compare() Request failed"))
    }
}

pub async fn get_derived_compare_df(
    remote_repo: &RemoteRepository,
    compare_id: &str,
) -> Result<JsonDataFrameView, OxenError> {
    // TODO: Factor out this basehead - not actually using it but needs to sync w/ routes on server
    let uri = format!("/compare/data_frames/{compare_id}/diff/main..main");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<JsonDataFrameViewResponse, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(df) => Ok(df.data_frame.view),
        Err(err) => Err(OxenError::basic_str(format!(
            "get_derived_compare_df() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

pub async fn commits(
    remote_repo: &RemoteRepository,
    base_commit_id: &MerkleHash,
    head_commit_id: &MerkleHash,
) -> Result<Vec<Commit>, OxenError> {
    let base_commit_id = base_commit_id.to_string();
    let head_commit_id = head_commit_id.to_string();
    let uri = format!("/compare/commits/{base_commit_id}..{head_commit_id}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<CompareCommitsResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(commits) => Ok(commits
            .compare
            .commits
            .into_iter()
            .map(|c| c.commit)
            .collect()),
        Err(err) => Err(OxenError::basic_str(format!(
            "commits() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

pub async fn dir_tree(
    remote_repo: &RemoteRepository,
    base_commit_id: &MerkleHash,
    head_commit_id: &MerkleHash,
) -> Result<Vec<DirDiffTreeSummary>, OxenError> {
    let base_commit_id = base_commit_id.to_string();
    let head_commit_id = head_commit_id.to_string();
    let uri = format!("/compare/dir_tree/{base_commit_id}..{head_commit_id}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<DirTreeDiffResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(dir_tree) => Ok(dir_tree.dirs),
        Err(err) => Err(OxenError::basic_str(format!(
            "commits() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

pub async fn entries(
    remote_repo: &RemoteRepository,
    base_commit_id: &MerkleHash,
    head_commit_id: &MerkleHash,
) -> Result<CompareEntries, OxenError> {
    let base_commit_id = base_commit_id.to_string();
    let head_commit_id = head_commit_id.to_string();
    let uri = format!("/compare/entries/{base_commit_id}..{head_commit_id}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<CompareEntriesResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(entries) => Ok(entries.compare),
        Err(err) => Err(OxenError::basic_str(format!(
            "commits() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        commits, create_compare, dir_tree, entries, get_derived_compare_df, update_compare,
    };
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::diff::AddRemoveModifyCounts;
    use crate::model::diff::diff_entry_status::DiffEntryStatus;
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::view::compare::{TabularCompareFieldBody, TabularCompareTargetBody};

    use std::path::PathBuf;

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_compare() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            // Five commits, each adding a file in a new directory.
            let mut commit_ids = Vec::new();
            for i in 0..5 {
                let full_path = local_repo
                    .path
                    .join(format!("dir_{i}"))
                    .join(format!("file_{i}.txt"));
                util::fs::create_dir_all(full_path.parent().unwrap())?;
                test::write_txt_file_to_path(full_path, format!("File content {i}"))?;
                repositories::add(&local_repo, &local_repo.path).await?;
                let commit = repositories::commit(&local_repo, &format!("Commit {i}"))?;
                commit_ids.push(commit.id);
            }

            // A file added at the root.
            let txt_path = local_repo.path.join("test_me_out.txt");
            test::write_txt_file_to_path(&txt_path, "i am the contents of test_me_out.txt")?;
            repositories::add(&local_repo, &local_repo.path).await?;
            let txt_commit = repositories::commit(&local_repo, "add test_me_out.txt")?;

            // A tabular file at the root, then a row added to it.
            let csv_path = local_repo.path.join("test_me_out.csv");
            test::write_txt_file_to_path(&csv_path, "image,label\n1,2\n3,4\n5,6")?;
            repositories::add(&local_repo, &local_repo.path).await?;
            let csv_commit = repositories::commit(&local_repo, "add test_me_out.csv")?;
            test::write_txt_file_to_path(&csv_path, "image,label\n1,2\n3,4\n5,6\n7,8")?;
            repositories::add(&local_repo, &local_repo.path).await?;
            let row_commit = repositories::commit(&local_repo, "add a row to test_me_out.csv")?;

            test::attach_remote_repo(&mut local_repo, &remote_repo)?;
            repositories::push(&local_repo).await?;

            // base..head is the commits on head that aren't already on base.
            let base = commit_ids[1].parse()?;
            let head = commit_ids[3].parse()?;
            let ids: Vec<String> = commits(&remote_repo, &base, &head)
                .await?
                .iter()
                .map(|c| c.id.clone())
                .collect();
            assert_eq!(ids.len(), 2);
            assert!(ids.contains(&commit_ids[2]));
            assert!(ids.contains(&commit_ids[3]));
            assert!(!ids.contains(&commit_ids[1]), "base itself is excluded");

            let results = dir_tree(&remote_repo, &base, &head).await?;
            assert_eq!(results.len(), 1);
            let first = results.first().unwrap();
            assert_eq!(first.name, PathBuf::from(""));
            assert_eq!(first.status, DiffEntryStatus::Modified);
            assert_eq!(first.children.len(), 2, "dir_2 and dir_3 were added");

            let reversed = dir_tree(&remote_repo, &head, &base).await?;
            let root = reversed.first().unwrap();
            assert_eq!(root.status, DiffEntryStatus::Modified);
            assert!(
                root.children
                    .iter()
                    .all(|dir| dir.status == DiffEntryStatus::Removed),
                "dir_2 and dir_3 exist only in the base: {reversed:?}"
            );
            assert_eq!(root.children.len(), 2);

            // A change at the root modifies the root and no directory under it.
            let base = commit_ids[4].parse()?;
            let head = txt_commit.id.parse()?;
            let results = dir_tree(&remote_repo, &base, &head).await?;
            assert_eq!(results.len(), 1);
            let first = results.first().unwrap();
            assert_eq!(first.name, PathBuf::from(""));
            assert_eq!(first.status, DiffEntryStatus::Modified);
            assert_eq!(first.children.len(), 0);
            let results = entries(&remote_repo, &base, &head).await?;
            let first = results.entries.first().unwrap();
            assert_eq!(first.filename, "test_me_out.txt");
            assert_eq!(first.status, "added");
            assert!(first.base_resource.is_none());
            assert!(first.head_resource.is_some());

            // A modified tabular file reports a different version on each side.
            let base = csv_commit.id.parse()?;
            let head = row_commit.id.parse()?;
            let results = dir_tree(&remote_repo, &base, &head).await?;
            assert_eq!(results.len(), 1);
            let first = results.first().unwrap();
            assert_eq!(first.name, PathBuf::from(""));
            assert_eq!(first.status, DiffEntryStatus::Modified);
            assert_eq!(first.children.len(), 0);
            let results = entries(&remote_repo, &base, &head).await?;
            assert_eq!(results.entries.len(), 1);
            let first = results.entries.first().unwrap();
            assert_eq!(first.filename, "test_me_out.csv");
            assert_eq!(first.status, "modified");
            let (base_resource, head_resource) = (
                first.base_resource.as_ref().unwrap(),
                first.head_resource.as_ref().unwrap(),
            );
            assert_ne!(base_resource.version, head_resource.version);
            let (base_entry, head_entry) = (
                first.base_entry.as_ref().unwrap(),
                first.head_entry.as_ref().unwrap(),
            );
            assert_ne!(
                base_entry.resource.as_ref().unwrap().version,
                head_entry.resource.as_ref().unwrap().version,
                "the entries' resources carry each side's version"
            );

            // Keying on a, b and c and targeting d: 1 modified, 1 added, 1 removed.
            let left_path = "left.csv";
            let right_path = "right.csv";
            test::write_txt_file_to_path(
                local_repo.path.join(left_path),
                "a,b,c,d\n1,2,3,4\n4,5,6,7\n9,0,1,2",
            )?;
            test::write_txt_file_to_path(
                local_repo.path.join(right_path),
                "a,b,c,d\n1,2,3,4\n4,5,6,8\n0,1,9,2",
            )?;
            repositories::add(&local_repo, &local_repo.path).await?;
            let first_commit = repositories::commit(&local_repo, "committing files")?;
            repositories::push(&local_repo).await?;

            let keys = || {
                ["a", "b", "c"]
                    .map(|key| TabularCompareFieldBody {
                        left: key.to_string(),
                        right: key.to_string(),
                        alias_as: None,
                        compare_method: None,
                    })
                    .to_vec()
            };
            let targets = || {
                vec![TabularCompareTargetBody {
                    left: Some("d".to_string()),
                    right: Some("d".to_string()),
                    compare_method: None,
                }]
            };
            let compare_id = "abcdefgh";
            create_compare(
                &remote_repo,
                compare_id,
                left_path,
                DEFAULT_BRANCH_NAME,
                right_path,
                DEFAULT_BRANCH_NAME,
                keys(),
                targets(),
                vec![],
            )
            .await?;

            let df = get_derived_compare_df(&remote_repo, compare_id)
                .await?
                .to_df()
                .await;
            assert_eq!(df.height(), 3);
            let counts = AddRemoveModifyCounts::from_diff_df(&df)?;
            assert_eq!(counts.added, 1);
            assert_eq!(counts.modified, 1);
            assert_eq!(counts.removed, 1);

            // Advancing the data leaves the compare as it was until the compare is updated.
            test::write_txt_file_to_path(
                local_repo.path.join(right_path),
                "a,b,c,d\n1,2,3,4\n4,5,6,8",
            )?;
            repositories::add(&local_repo, &local_repo.path).await?;
            repositories::commit(&local_repo, "committing files")?;
            repositories::push(&local_repo).await?;

            let new_df = get_derived_compare_df(&remote_repo, compare_id)
                .await?
                .to_df()
                .await;
            assert_eq!(new_df, df, "the compare does not update on its own");

            // The same body again, with the left side on the first commit and the right on main.
            update_compare(
                &remote_repo,
                compare_id,
                left_path,
                &first_commit.id,
                right_path,
                DEFAULT_BRANCH_NAME,
                keys(),
                targets(),
                vec![],
            )
            .await?;

            let new_df = get_derived_compare_df(&remote_repo, compare_id)
                .await?
                .to_df()
                .await;
            assert_ne!(
                new_df, df,
                "the update reads right.csv at the right-hand revision"
            );
            assert_eq!(new_df.height(), 2);

            Ok(remote_repo)
        })
        .await
    }
}
