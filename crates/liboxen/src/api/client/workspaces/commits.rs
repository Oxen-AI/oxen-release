use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{Branch, Commit, NewCommitBody, RemoteRepository};
use crate::view::CommitResponse;
use crate::view::merge::{Mergeable, MergeableResponse};

pub async fn mergeability(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    workspace_id: &str,
) -> Result<Mergeable, OxenError> {
    let uri = format!("/workspaces/{workspace_id}/merge/{branch_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<MergeableResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.mergeable),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::workspaces::commits::merge error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn commit(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    workspace_id: &str,
    commit: &NewCommitBody,
) -> Result<Commit, OxenError> {
    // Notify the server that we are starting a push
    api::client::repositories::pre_push_workspace(remote_repo).await?;

    let uri = format!("/workspaces/{workspace_id}/commit/{branch_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("commit_staged {url}\n{commit:?}");

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).json(&commit).send().await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("commit_staged got body: {body}");
    let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => {
            let commit = val.commit;
            // make sure to call our /complete call to kick off the post-push hooks
            let branch = Branch {
                name: branch_name.to_string(),
                commit_id: commit.id.clone(),
            };
            api::client::commits::post_push_complete(remote_repo, &branch, &commit.id).await?;
            api::client::repositories::post_push_workspace(remote_repo).await?;

            println!("🐂 commit {commit} complete!");
            Ok(commit)
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "api::workspaces::commits error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::api::client::branches::create_from_branch;
    use crate::api::client::commits::get_by_id;
    use crate::api::client::workspaces::data_frames::{columns, index};
    use crate::api::client::workspaces::{self, files};
    use crate::api::client::{data_frames, dir};
    use crate::util::fs;

    use std::path::{Path, PathBuf};

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::{NewCommitBody, RemoteRepository};
    use crate::opts::DFOpts;
    use crate::test;

    fn commit_body(message: &str) -> NewCommitBody {
        NewCommitBody {
            message: message.to_string(),
            author: "Test User".to_string(),
            email: "test@oxen.ai".to_string(),
        }
    }

    /// Creates `branch_name` off main and opens a workspace on it for each id in `workspace_ids`.
    async fn branch_with_workspaces(
        remote_repo: &RemoteRepository,
        branch_name: &str,
        workspace_ids: &[&str],
    ) -> Result<(), OxenError> {
        create_from_branch(remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
        for workspace_id in workspace_ids {
            workspaces::create(remote_repo, branch_name, workspace_id).await?;
        }
        Ok(())
    }

    async fn root_entry_count(
        remote_repo: &RemoteRepository,
        branch_name: &str,
    ) -> Result<usize, OxenError> {
        Ok(dir::list(remote_repo, branch_name, "", 1, 100)
            .await?
            .total_entries)
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_commit_workspaces_and_check_mergeability() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let bbox_dir = Path::new("annotations").join("train");
            let bbox_path = local_repo.path.join(&bbox_dir).join("bounding_box.csv");
            let bbox_dir = bbox_dir.to_str().expect("utf-8 path");

            // Commit two staged files
            branch_with_workspaces(&remote_repo, "add-data", &["add-data"]).await?;
            let paths = vec![
                test::test_img_file(),
                test::test_img_file_with_name("cole_anthony.jpeg"),
            ];
            files::add(&remote_repo, "add-data", "data", paths, &None).await?;
            let commit = workspaces::commit(
                &remote_repo,
                "add-data",
                "add-data",
                &commit_body("Add staged data"),
            )
            .await?;
            let remote_commit = get_by_id(&remote_repo, &commit.id).await?;
            assert_eq!(remote_commit.map(|c| c.id), Some(commit.id));

            // A workspace even with its branch merges with no conflicts
            branch_with_workspaces(&remote_repo, "even", &["even"]).await?;
            files::add(
                &remote_repo,
                "even",
                "data",
                vec![test::test_img_file()],
                &None,
            )
            .await?;
            let mergeable = super::mergeability(&remote_repo, "even", "even").await?;
            assert!(mergeable.is_mergeable);
            assert_eq!(mergeable.conflicts.len(), 0);
            assert_eq!(mergeable.commits.len(), 1);

            // Two workspaces on one commit: the second merges when the first changed another file,
            // and conflicts when the first changed the same file
            for (branch_name, first_change, expect_conflict) in [
                ("different-files", None, false),
                (
                    "same-file",
                    Some(
                        "file,label,min_x,min_y,width,height\ntest/test.jpg,dog,13.5,32.0,385,330",
                    ),
                    true,
                ),
            ] {
                let (first, second) = (format!("{branch_name}-1"), format!("{branch_name}-2"));
                branch_with_workspaces(&remote_repo, branch_name, &[&first, &second]).await?;
                let first_paths = match first_change {
                    Some(data) => {
                        fs::write_to_path(&bbox_path, data)?;
                        vec![bbox_path.clone()]
                    }
                    None => vec![test::test_img_file()],
                };
                files::add(&remote_repo, &first, bbox_dir, first_paths, &None).await?;
                workspaces::commit(&remote_repo, branch_name, &first, &commit_body(&first)).await?;

                fs::write_to_path(&bbox_path, "file,label\ntest/test.jpg,dog")?;
                files::add(
                    &remote_repo,
                    &second,
                    bbox_dir,
                    vec![bbox_path.clone()],
                    &None,
                )
                .await?;
                let mergeable = super::mergeability(&remote_repo, branch_name, &second).await?;
                assert_eq!(mergeable.is_mergeable, !expect_conflict, "{branch_name}");
                assert_eq!(
                    mergeable.conflicts.len(),
                    usize::from(expect_conflict),
                    "{branch_name}"
                );
                assert_eq!(mergeable.commits.len(), 2, "{branch_name}");
                let result =
                    workspaces::commit(&remote_repo, branch_name, &second, &commit_body(&second))
                        .await;
                assert_eq!(
                    result.is_err(),
                    expect_conflict,
                    "{branch_name}: {result:?}"
                );
            }

            // A column added in a workspace lands in the committed schema
            branch_with_workspaces(&remote_repo, "add-column", &["add-column"]).await?;
            let df_path = PathBuf::from(bbox_dir).join("bounding_box.csv");
            let column_name = "my_new_column";
            index(&remote_repo, "add-column", &df_path).await?;
            columns::create(
                &remote_repo,
                "add-column",
                &df_path,
                format!(r#"{{"name":"{column_name}", "data_type": "str"}}"#),
            )
            .await?;
            let commit = workspaces::commit(
                &remote_repo,
                "add-column",
                "add-column",
                &commit_body("Add column"),
            )
            .await?;
            let remote_commit = get_by_id(&remote_repo, &commit.id).await?;
            assert_eq!(remote_commit.map(|c| c.id), Some(commit.id));
            let df =
                data_frames::get(&remote_repo, "add-column", &df_path, DFOpts::empty()).await?;
            assert_eq!(
                df.data_frame.source.schema.fields.len(),
                df.data_frame.view.schema.fields.len()
            );
            assert!(
                df.data_frame
                    .view
                    .schema
                    .fields
                    .iter()
                    .any(|field| field.name == column_name),
                "column `{column_name}` is in the committed data frame"
            );

            // Committing a file a second time, unchanged, makes no commit
            for (branch_name, directory, file) in [
                ("same-parquet", "", test::test_100_parquet()),
                ("same-image", "images", test::test_img_file()),
            ] {
                let (first, second) = (format!("{branch_name}-1"), format!("{branch_name}-2"));
                branch_with_workspaces(&remote_repo, branch_name, &[&first]).await?;
                let entries_before = root_entry_count(&remote_repo, branch_name).await?;
                files::add(&remote_repo, &first, directory, vec![file.clone()], &None).await?;
                workspaces::commit(&remote_repo, branch_name, &first, &commit_body(&first)).await?;
                assert_eq!(
                    root_entry_count(&remote_repo, branch_name).await?,
                    entries_before + 1
                );

                workspaces::create(&remote_repo, branch_name, &second).await?;
                files::add(&remote_repo, &second, directory, vec![file], &None).await?;
                let err =
                    workspaces::commit(&remote_repo, branch_name, &second, &commit_body(&second))
                        .await
                        .expect_err("an unchanged file makes no commit");
                assert!(
                    err.to_string().contains("NoChanges"),
                    "{branch_name}: {err}"
                );
                assert_eq!(
                    root_entry_count(&remote_repo, branch_name).await?,
                    entries_before + 1
                );
            }

            Ok(remote_repo)
        })
        .await
    }
}
