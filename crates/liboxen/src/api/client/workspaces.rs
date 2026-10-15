pub mod changes;
pub mod commits;
pub mod data_frames;
pub mod files;

use std::path::Path;

pub use commits::commit;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::workspaces::{ListWorkspaceResponseView, WorkspaceResponseWithStatus};
use crate::view::workspaces::{NewWorkspace, WorkspaceResponse};
use crate::view::{StatusMessage, WorkspaceResponseView};

pub async fn list(remote_repo: &RemoteRepository) -> Result<Vec<WorkspaceResponse>, OxenError> {
    let url = api::endpoint::url_from_repo(remote_repo, "/workspaces")?;
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<ListWorkspaceResponseView, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.workspaces),
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
) -> Result<Option<WorkspaceResponse>, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, &format!("/workspaces/{workspace_id}"))?;
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body_result = client::parse_json_body(&url, res).await;

    let workspace = body_result
        .ok()
        .and_then(|body| serde_json::from_str::<WorkspaceResponseView>(&body).ok())
        .map(|val| val.workspace);

    Ok(workspace)
}

pub async fn get_by_name(
    remote_repo: &RemoteRepository,
    name: impl AsRef<str>,
) -> Result<Option<WorkspaceResponse>, OxenError> {
    let name = name.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, &format!("/workspaces?name={name}"))?;
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<ListWorkspaceResponseView, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(val) => {
            if val.workspaces.len() == 1 {
                Ok(Some(val.workspaces[0].clone()))
            } else if val.workspaces.is_empty() {
                Ok(None)
            } else {
                Err(OxenError::basic_str(format!(
                    "expected 1 workspace, got {}",
                    val.workspaces.len()
                )))
            }
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn create(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    workspace_id: impl AsRef<str>,
) -> Result<WorkspaceResponseWithStatus, OxenError> {
    create_with_path(remote_repo, branch_name, workspace_id, Path::new("/"), None).await
}

pub async fn create_with_name(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    workspace_id: impl AsRef<str>,
    workspace_name: impl AsRef<str>,
) -> Result<WorkspaceResponseWithStatus, OxenError> {
    let workspace_name = workspace_name.as_ref().to_string();
    create_with_path(
        remote_repo,
        branch_name,
        workspace_id,
        Path::new("/"),
        Some(workspace_name),
    )
    .await
}

pub async fn create_with_path(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    workspace_name: Option<String>,
) -> Result<WorkspaceResponseWithStatus, OxenError> {
    let branch_name = branch_name.as_ref();
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, "/workspaces")?;
    log::debug!("create workspace {url}\n");

    let body = NewWorkspace {
        branch_name: branch_name.to_string(),
        workspace_id: workspace_id.to_string(),
        // These two are needed for the oxen hub right now, ignored by the server
        resource_path: Some(path.to_str().unwrap().to_string()),
        entity_type: Some("user".to_string()),
        name: workspace_name,
        force: Some(true),
    };

    let client = client::new_for_url(&url)?;
    let res = client.put(&url).json(&body).send().await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("create workspace got body: {body}");
    let response: Result<WorkspaceResponseView, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(WorkspaceResponseWithStatus {
            id: val.workspace.id,
            name: val.workspace.name,
            commit: val.workspace.commit,
            status: val.status.status_message,
            created_at: val.workspace.created_at,
        }),
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn delete(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
) -> Result<WorkspaceResponse, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, &format!("/workspaces/{workspace_id}"))?;
    log::debug!("delete workspace {url}\n");

    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("delete workspace got body: {body}");
    let response: Result<WorkspaceResponseView, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.workspace),
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn clear(remote_repo: &RemoteRepository) -> Result<(), OxenError> {
    let url = api::endpoint::url_from_repo(remote_repo, "/workspaces")?;
    log::debug!("clear workspaces {url}\n");

    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("delete workspace got body: {body}");
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(_) => Ok(()),
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::api::client::branches::create_from_branch;
    use crate::repositories::branches::create_checkout;
    use crate::repositories::workspaces::df;
    use crate::util::fs;

    use super::*;

    use crate::command;
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::opts::DFOpts;
    use crate::repositories;
    use crate::test;

    fn commit_body(message: &str) -> NewCommitBody {
        NewCommitBody {
            message: message.to_string(),
            author: "Test User".to_string(),
            email: "test@oxen.ai".to_string(),
        }
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_create_find_commit_and_clear_workspaces() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            assert_eq!(
                list(&remote_repo).await?.len(),
                0,
                "a new remote has no workspaces"
            );
            assert!(get(&remote_repo, "id_does_not_exist").await?.is_none());
            assert!(
                get_by_name(&remote_repo, "name_does_not_exist")
                    .await?
                    .is_none()
            );

            let workspace = create(&remote_repo, DEFAULT_BRANCH_NAME, "unnamed_id").await?;
            assert_eq!(workspace.id, "unnamed_id");
            let named = [("named_id", "named"), ("named_id2", "named2")];
            for (id, name) in named {
                let workspace =
                    create_with_name(&remote_repo, DEFAULT_BRANCH_NAME, id, name).await?;
                assert_eq!(workspace.id, id);
                assert_eq!(workspace.name.as_deref(), Some(name));
            }
            for (id, name) in named {
                let by_id = get(&remote_repo, id).await?.expect("found by id");
                assert_eq!(by_id.name.as_deref(), Some(name));
                let by_name = get_by_name(&remote_repo, name)
                    .await?
                    .expect("found by name");
                assert_eq!(by_name.id, id);
            }
            assert_eq!(list(&remote_repo).await?.len(), 3);
            delete(&remote_repo, "unnamed_id").await?;
            assert!(get(&remote_repo, "unnamed_id").await?.is_none());

            // A commit closes an unnamed workspace and keeps a named one
            create(&remote_repo, DEFAULT_BRANCH_NAME, "commit_unnamed_id").await?;
            files::upload_single_file(
                &remote_repo,
                "commit_unnamed_id",
                "unnamed",
                test::test_img_file(),
            )
            .await?;
            commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "commit_unnamed_id",
                &commit_body("unnamed"),
            )
            .await?;
            assert!(get(&remote_repo, "commit_unnamed_id").await?.is_none());

            create_with_name(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "commit_named_id",
                "commit_named",
            )
            .await?;
            files::upload_single_file(&remote_repo, "commit_named", "named", test::test_img_file())
                .await?;
            commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "commit_named",
                &commit_body("named"),
            )
            .await?;
            let workspace = get(&remote_repo, "commit_named")
                .await?
                .expect("still open");
            assert_eq!(workspace.id, "commit_named_id");

            // A workspace on a branch behind main stages a file main already committed there
            let behind = "behind-main";
            create_from_branch(&remote_repo, behind, DEFAULT_BRANCH_NAME).await?;
            let folder = "images/folder";
            create(&remote_repo, DEFAULT_BRANCH_NAME, "ahead").await?;
            files::upload_single_file(&remote_repo, "ahead", folder, test::test_img_file()).await?;
            commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "ahead",
                &commit_body("Add to main"),
            )
            .await?;
            create(&remote_repo, behind, "behind").await?;
            files::upload_single_file(&remote_repo, "behind", folder, test::test_1k_parquet())
                .await?;
            commit(
                &remote_repo,
                behind,
                "behind",
                &commit_body("Add behind main"),
            )
            .await?;
            create(&remote_repo, behind, "behind").await?;
            files::upload_single_file(&remote_repo, "behind", folder, test::test_100_parquet())
                .await?;
            let status = changes::list(
                &remote_repo,
                "behind",
                "",
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE,
            )
            .await?;
            assert_eq!(status.added_files.entries.len(), 1);
            assert_eq!(status.added_files.total_entries, 1);

            // A workspace commit fails, keeping its edits, once a push changes the data frame's
            // schema on its branch
            let schema_branch = "schema-change";
            create_checkout(&local_repo, schema_branch)?;
            let path = Path::new("text.csv");
            let full_path = local_repo.path.join(path);
            fs::write_to_path(&full_path, "text,label\nI am a row,positive\n")?;
            repositories::add(&local_repo, &full_path).await?;
            repositories::commit(&local_repo, "Add text.csv")?;
            repositories::push(&local_repo).await?;

            create(&remote_repo, schema_branch, "schema_workspace").await?;
            df::index(&local_repo, "schema_workspace", path).await?;
            let mut opts = DFOpts::empty();
            opts.add_row = Some(r#"{"text": "I am a new row", "label": "neutral"}"#.to_string());
            df(&local_repo, "schema_workspace", path, opts).await?;

            let mut opts = DFOpts::empty();
            opts.add_col = Some("is_something:n/a:str".to_string());
            opts.output = Some(full_path.clone());
            command::df(&full_path, opts).await?;
            repositories::add(&local_repo, &full_path).await?;
            repositories::commit(&local_repo, "Changed the schema 😇")?;
            repositories::push(&local_repo).await?;

            let err = commit(
                &remote_repo,
                schema_branch,
                "schema_workspace",
                &commit_body("Remotely committing"),
            )
            .await
            .expect_err("the branch moved past the workspace's commit");
            assert!(err.to_string().contains("behind"), "{err}");
            let status = changes::list(
                &remote_repo,
                "schema_workspace",
                "",
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE,
            )
            .await?;
            assert_eq!(status.modified_files.entries.len(), 1);

            assert!(!list(&remote_repo).await?.is_empty());
            clear(&remote_repo).await?;
            assert_eq!(list(&remote_repo).await?.len(), 0);

            Ok(remote_repo)
        })
        .await
    }
}
