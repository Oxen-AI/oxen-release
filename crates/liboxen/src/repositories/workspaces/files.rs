use crate::core;
use crate::error::OxenError;
use crate::model::Workspace;

use std::path::PathBuf;

pub use crate::core::v_latest::workspaces::files::exists;

pub use crate::core::v_latest::workspaces::files::is_dir;

pub use crate::core::v_latest::workspaces::files::add;

pub use crate::core::v_latest::workspaces::files::rm;

pub use crate::core::v_latest::workspaces::files::unstage;

pub async fn import(
    url: &str,
    auth: &str,
    directory: PathBuf,
    filename: Option<String>,
    workspace: &Workspace,
    allow_loopback: bool,
) -> Result<(), OxenError> {
    core::v_latest::workspaces::files::import(
        url,
        auth,
        directory,
        filename,
        workspace,
        allow_loopback,
    )
    .await?;
    Ok(())
}

pub use crate::core::v_latest::workspaces::files::upload_zip;

pub use crate::core::v_latest::workspaces::files::mv;

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use uuid::Uuid;

    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::core::staged::staged_db_manager::get_staged_db_manager;
    use crate::error::OxenError;
    use crate::model::{Commit, LocalRepository, NewCommitBody, StagedEntryStatus, Workspace};
    use crate::repositories::size;
    use crate::repositories::{self, workspaces};
    use crate::test;
    use crate::util::fs;

    #[tokio::test]
    async fn test_mv_file_in_workspace() -> Result<(), OxenError> {
        // Skip workspace ops on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "test-mv";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;

            // Original file path that exists in the repo
            let original_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let new_path = Path::new("renamed").join("data").join("bbox_renamed.csv");

            // Move the file
            workspaces::files::mv(&workspace, &original_path, &new_path).await?;

            // Check status - should show the original as removed and new as added
            let status = workspaces::status::status(&workspace)?;
            println!("Status after mv: {status:?}");

            // The original path should be staged as removed in staged_files
            let removed_entry = status.staged_files.get(&original_path);
            assert!(
                removed_entry.is_some(),
                "Original path should be in staged_files"
            );
            assert_eq!(
                removed_entry.unwrap().status,
                crate::model::StagedEntryStatus::Removed,
                "Original path should have Removed status"
            );

            // The new path should be in staged_files with Added status
            let added_entry = status.staged_files.get(&new_path);
            assert!(added_entry.is_some(), "New path should be staged");
            assert_eq!(
                added_entry.unwrap().status,
                crate::model::StagedEntryStatus::Added,
                "New path should have Added status"
            );

            // The move should be detected
            assert!(
                !status.moved_files.is_empty(),
                "Move should be detected in moved_files"
            );

            assert!(
                !size::repo_size_path(&repo).exists(),
                "committing locally records no size, so there is none to start from"
            );

            // Commit the workspace and verify the file is at the new location
            let user = UserConfig::get()?.to_user();
            let new_commit = NewCommitBody {
                author: user.name.clone(),
                email: user.email.clone(),
                message: "Moved file to new location".to_string(),
            };
            let commit =
                workspaces::commit(&workspace, &new_commit, branch_name.to_string()).await?;

            // Verify the file exists at the new path in the commit
            let new_file = repositories::tree::get_file_by_path(&repo, &commit, &new_path)?;
            assert!(
                new_file.is_some(),
                "File should exist at new path after commit"
            );

            // Verify the file no longer exists at the original path
            let old_file = repositories::tree::get_file_by_path(&repo, &commit, &original_path)?;
            assert!(
                old_file.is_none(),
                "File should not exist at original path after commit"
            );

            assert_eq!(
                size::wait_for_recorded_size(&repo)?,
                repo.version_bytes()?,
                "the figure recorded is the bytes the tree references"
            );
            assert_eq!(
                fs::read_from_path(size::repo_size_path(&repo))?.trim(),
                repo.version_bytes()?.to_string(),
                "the figure is recorded on its own, with no status beside it"
            );

            // A record claiming a calculation is under way, with nothing running to finish it.
            fs::write_to_path(
                size::repo_size_path(&repo),
                r#"{"status":"pending","size":7}"#,
            )?;
            assert_eq!(
                size::wait_for_recorded_size(&repo)?,
                repo.version_bytes()?,
                "a record no pass stands behind is recalculated rather than believed"
            );

            Ok(())
        })
        .await
    }

    /// Workspace created with no files staged; exists() should return Ok(false).
    #[tokio::test]
    async fn test_exists_returns_false_when_no_files_staged() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let file = repo.path.join("hello.txt");
            crate::util::fs::write_to_path(&file, "hello")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "Add hello.txt")?;

            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-workspace", false)?;

            let result = workspaces::files::exists(&workspace, std::path::Path::new("hello.txt"))?;
            assert!(!result);

            Ok(())
        })
        .await
    }

    /// exists() returns Ok(false) for an unstaged path and Ok(true) after staging it.
    #[tokio::test]
    async fn test_exists_false_before_staging_true_after() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let file = repo.path.join("hello.txt");
            crate::util::fs::write_to_path(&file, "hello")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "Add hello.txt")?;

            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-workspace", false)?;

            // Write modified content so the file is detected as changed
            let workspace_file = workspace.workspace_repo.path.join("hello.txt");
            crate::util::fs::write_to_path(&workspace_file, "hello world")?;

            let hello = Path::new("hello.txt");
            let nonexistent = Path::new("does_not_exist.txt");

            // Before staging: both should be false
            assert!(!workspaces::files::exists(&workspace, hello)?);
            assert!(!workspaces::files::exists(&workspace, nonexistent)?);

            // Stage the file in the workspace
            workspaces::files::add(&workspace, &workspace_file).await?;

            // After staging: staged file is true, non-existent file is still false
            assert!(workspaces::files::exists(&workspace, hello)?);
            assert!(!workspaces::files::exists(&workspace, nonexistent)?);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_import_rejects_private_ip() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let file = repo.path.join("hello.txt");
            crate::util::fs::write_to_path(&file, "hello")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "Add hello.txt")?;

            let workspace = repositories::workspaces::create_temporary(&repo, &commit).await?;

            let cases = [
                ("http://127.0.0.1/secret", "loopback address"),
                ("http://10.0.0.1/internal", "private address"),
                (
                    "http://169.254.169.254/latest/meta-data/",
                    "link-local/metadata address",
                ),
                ("http://[::1]/secret", "IPv6 loopback"),
                ("file:///etc/passwd", "non-HTTP scheme"),
            ];

            for (url, label) in cases {
                // allow_loopback is false, so loopback targets are rejected like other private IPs.
                let result = workspaces::files::import(
                    url,
                    "",
                    std::path::PathBuf::from("data"),
                    None,
                    &workspace,
                    false,
                )
                .await;
                assert!(result.is_err(), "should reject {label}");
            }

            Ok(())
        })
        .await
    }

    // A chained rename: the second move's source is staged but absent from the base commit, so it
    // is unstaged outright rather than staged for removal, and only the original committed path
    // carries the `Removed` marker.
    #[tokio::test]
    async fn test_mv_twice_leaves_one_removed_marker() -> Result<(), OxenError> {
        // Skip workspace ops on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_empty_local_repo_test_async(|repo| async move {
            let committed = repo.path.join("first.txt");
            crate::util::fs::write_to_path(&committed, "hello")?;
            repositories::add(&repo, &committed).await?;
            let commit = repositories::commit(&repo, "Add first.txt")?;

            let workspace = repositories::workspaces::create(&repo, &commit, "mv-twice", true)?;

            let first = Path::new("first.txt");
            let second = Path::new("second.txt");
            let third = Path::new("third.txt");
            super::mv(&workspace, first, second).await?;
            workspaces::files::mv(&workspace, second, third).await?;

            let status = workspaces::status::status(&workspace)?;
            assert_eq!(
                status.staged_files.get(first).map(|entry| &entry.status),
                Some(&crate::model::StagedEntryStatus::Removed),
                "the committed source keeps its Removed marker across both moves"
            );
            assert_eq!(
                status.staged_files.get(third).map(|entry| &entry.status),
                Some(&crate::model::StagedEntryStatus::Added),
                "the final destination is staged as Added"
            );
            assert!(
                !status.staged_files.contains_key(second),
                "the intermediate path was never committed, so it is unstaged rather than \
                 staged for removal"
            );

            Ok(())
        })
        .await
    }
    fn body(message: &str) -> NewCommitBody {
        NewCommitBody {
            message: message.to_string(),
            author: "Bessie".to_string(),
            email: "bessie@oxen.ai".to_string(),
        }
    }

    // Commits data/a.txt, data/sub/b.txt and other.txt, and returns an editable workspace on it.
    async fn dir_workspace(repo: &LocalRepository) -> Result<Workspace, OxenError> {
        for (path, content) in [
            ("data/a.txt", "A"),
            ("data/sub/b.txt", "B"),
            ("other.txt", "Other"),
        ] {
            fs::write_to_path(repo.path.join(path), content)?;
        }
        repositories::add(repo, &repo.path).await?;
        let commit = repositories::commit(repo, "Add data")?;
        repositories::workspaces::create(repo, &commit, Uuid::new_v4().to_string(), true)
    }

    async fn stage_new_file(workspace: &Workspace, path: &str) -> Result<(), OxenError> {
        let path = workspace.workspace_repo.path.join(path);
        fs::write_to_path(&path, "new")?;
        workspaces::files::add(workspace, path).await?;
        Ok(())
    }

    fn staged_status(workspace: &Workspace, path: &str) -> Option<StagedEntryStatus> {
        workspaces::status::status(workspace)
            .unwrap()
            .staged_files
            .get(Path::new(path))
            .map(|entry| entry.status.clone())
    }

    fn assert_files(repo: &LocalRepository, commit: &Commit, present: &[&str], absent: &[&str]) {
        for path in present {
            assert!(
                repositories::tree::get_file_by_path(repo, commit, path)
                    .unwrap()
                    .is_some(),
                "{path} should be in the commit"
            );
        }
        for path in absent {
            assert!(
                repositories::tree::get_node_by_path(repo, commit, path)
                    .unwrap()
                    .is_none(),
                "{path} should not be in the commit"
            );
            assert!(
                !repositories::tree::has_dir(repo, commit, path).unwrap(),
                "{path} should not be a directory in the commit"
            );
        }
    }

    #[tokio::test]
    async fn test_mv_committed_dir_with_nested_subdir() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let workspace = dir_workspace(&repo).await?;

            workspaces::files::mv(&workspace, Path::new("data"), Path::new("moved")).await?;

            assert_eq!(
                staged_status(&workspace, "data/a.txt"),
                Some(StagedEntryStatus::Removed)
            );
            assert_eq!(
                staged_status(&workspace, "data/sub/b.txt"),
                Some(StagedEntryStatus::Removed)
            );
            assert_eq!(
                staged_status(&workspace, "moved/a.txt"),
                Some(StagedEntryStatus::Added)
            );
            assert_eq!(
                staged_status(&workspace, "moved/sub/b.txt"),
                Some(StagedEntryStatus::Added)
            );
            let retry =
                workspaces::files::mv(&workspace, Path::new("data"), Path::new("moved")).await;
            assert!(
                matches!(retry, Err(OxenError::PathStagedForRemoval(_))),
                "retrying the move finds its source staged for removal, got {retry:?}"
            );

            let commit =
                workspaces::commit(&workspace, &body("Move data"), DEFAULT_BRANCH_NAME).await?;
            assert_files(
                &repo,
                &commit,
                &["moved/a.txt", "moved/sub/b.txt", "other.txt"],
                &["data", "data/sub", "data/a.txt", "data/sub/b.txt"],
            );
            assert!(repositories::tree::has_dir(&repo, &commit, "moved/sub")?);

            Ok(())
        })
        .await
    }

    // Files added in the workspace move with the directory, and a file staged for removal stays
    // removed rather than reappearing under the new path.
    #[tokio::test]
    async fn test_mv_dir_with_committed_and_staged_only_files() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let workspace = dir_workspace(&repo).await?;
            stage_new_file(&workspace, "data/new.txt").await?;
            stage_new_file(&workspace, "data/fresh/deep.txt").await?;
            workspaces::files::rm(&workspace, &[PathBuf::from("data/a.txt")]).await?;

            workspaces::files::mv(&workspace, Path::new("data"), Path::new("moved")).await?;

            assert_eq!(staged_status(&workspace, "data/new.txt"), None);
            assert_eq!(staged_status(&workspace, "data/fresh/deep.txt"), None);
            assert_eq!(staged_status(&workspace, "moved/a.txt"), None);
            assert_eq!(
                staged_status(&workspace, "moved/new.txt"),
                Some(StagedEntryStatus::Added)
            );
            assert_eq!(
                staged_status(&workspace, "moved/fresh/deep.txt"),
                Some(StagedEntryStatus::Added)
            );

            let commit =
                workspaces::commit(&workspace, &body("Move data"), DEFAULT_BRANCH_NAME).await?;
            assert_files(
                &repo,
                &commit,
                &["moved/new.txt", "moved/fresh/deep.txt", "moved/sub/b.txt"],
                &["data", "data/fresh", "moved/a.txt"],
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_mv_staged_only_dir() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let workspace = dir_workspace(&repo).await?;
            stage_new_file(&workspace, "outer/fresh/x.txt").await?;
            stage_new_file(&workspace, "outer/fresh/deep/y.txt").await?;

            workspaces::files::mv(&workspace, Path::new("outer/fresh"), Path::new("renamed"))
                .await?;

            let commit =
                workspaces::commit(&workspace, &body("Move fresh"), DEFAULT_BRANCH_NAME).await?;
            assert_files(
                &repo,
                &commit,
                &["renamed/x.txt", "renamed/deep/y.txt", "data/a.txt"],
                &["outer", "outer/fresh"],
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_mv_dir_refuses_bad_destinations() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let workspace = dir_workspace(&repo).await?;
            stage_new_file(&workspace, "staged/z.txt").await?;

            for dest in ["other.txt", "staged"] {
                let result =
                    workspaces::files::mv(&workspace, Path::new("data"), Path::new(dest)).await;
                assert!(
                    matches!(result, Err(OxenError::DestinationAlreadyStaged(_))),
                    "moving onto {dest} should be refused, got {result:?}"
                );
            }
            for dest in ["data", "data/sub/inner"] {
                let result =
                    workspaces::files::mv(&workspace, Path::new("data"), Path::new(dest)).await;
                assert!(
                    matches!(result, Err(OxenError::MoveIntoItself(_))),
                    "moving into {dest} should be refused, got {result:?}"
                );
            }
            let result =
                workspaces::files::mv(&workspace, Path::new("nope"), Path::new("elsewhere")).await;
            assert!(
                matches!(result, Err(OxenError::PathDoesNotExist(_))),
                "a missing source should be refused, got {result:?}"
            );

            assert_eq!(staged_status(&workspace, "data/a.txt"), None);
            assert_eq!(
                staged_status(&workspace, "staged/z.txt"),
                Some(StagedEntryStatus::Added)
            );

            Ok(())
        })
        .await
    }

    // Removing a directory also unstages the files added under it in the workspace, so the commit
    // does not bring the directory back.
    #[tokio::test]
    async fn test_rm_dir_drops_staged_only_files() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let workspace = dir_workspace(&repo).await?;
            stage_new_file(&workspace, "data/new.txt").await?;
            stage_new_file(&workspace, "data/fresh/deep.txt").await?;

            let err_files = workspaces::files::rm(&workspace, &[PathBuf::from("data")]).await?;
            assert!(err_files.is_empty(), "{err_files:?}");
            assert_eq!(staged_status(&workspace, "data/new.txt"), None);
            assert_eq!(staged_status(&workspace, "data/fresh/deep.txt"), None);

            let commit =
                workspaces::commit(&workspace, &body("Remove data"), DEFAULT_BRANCH_NAME).await?;
            assert_files(
                &repo,
                &commit,
                &["other.txt"],
                &["data", "data/new.txt", "data/fresh"],
            );

            Ok(())
        })
        .await
    }

    // Moving a file the workspace removed would restore its content at the new path.
    #[tokio::test]
    async fn test_mv_refuses_file_staged_for_removal() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let workspace = dir_workspace(&repo).await?;
            workspaces::files::rm(&workspace, &[PathBuf::from("other.txt")]).await?;

            let result =
                workspaces::files::mv(&workspace, Path::new("other.txt"), Path::new("moved.txt"))
                    .await;
            assert!(
                matches!(result, Err(OxenError::PathStagedForRemoval(_))),
                "{result:?}"
            );
            assert_eq!(staged_status(&workspace, "moved.txt"), None);
            assert_eq!(
                staged_status(&workspace, "other.txt"),
                Some(StagedEntryStatus::Removed)
            );

            Ok(())
        })
        .await
    }

    // Tabular files move through the data frame rename, which must refuse a removed file too.
    #[tokio::test]
    async fn test_data_frame_rename_refuses_file_staged_for_removal() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            fs::write_to_path(repo.path.join("table.csv"), "x,y\n1,2\n")?;
            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "Add table")?;
            let workspace =
                repositories::workspaces::create(&repo, &commit, Uuid::new_v4().to_string(), true)?;
            workspaces::files::rm(&workspace, &[PathBuf::from("table.csv")]).await?;

            let result =
                repositories::workspaces::data_frames::rename(&workspace, "table.csv", "moved.csv")
                    .await;
            assert!(
                matches!(result, Err(OxenError::PathStagedForRemoval(_))),
                "{result:?}"
            );
            assert_eq!(staged_status(&workspace, "moved.csv"), None);
            assert_eq!(
                staged_status(&workspace, "table.csv"),
                Some(StagedEntryStatus::Removed)
            );

            Ok(())
        })
        .await
    }

    // A path that exists only in the workspace is unstaged; only a path with nothing at it fails.
    #[tokio::test]
    async fn test_rm_staged_only_paths() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_empty_local_repo_test_async(|repo| async move {
            let workspace = dir_workspace(&repo).await?;
            stage_new_file(&workspace, "outer/fresh/x.txt").await?;
            stage_new_file(&workspace, "outer/fresh/deep/y.txt").await?;
            stage_new_file(&workspace, "lone.txt").await?;
            stage_new_file(&workspace, "data/new.txt").await?;

            let err_files = workspaces::files::rm(
                &workspace,
                &[
                    PathBuf::from("outer/fresh"),
                    PathBuf::from("lone.txt"),
                    PathBuf::from("data/new.txt"),
                ],
            )
            .await?;
            assert!(err_files.is_empty(), "{err_files:?}");

            // Markers on committed directories may stay; nothing else should.
            let left: Vec<PathBuf> = get_staged_db_manager(&workspace.workspace_repo)?
                .read_entries_under(Path::new(""))?
                .into_iter()
                .map(|(path, _)| path)
                .filter(|path| path != Path::new("") && path != Path::new("data"))
                .collect();
            assert!(left.is_empty(), "nothing should be left staged: {left:?}");

            let err_files = workspaces::files::rm(&workspace, &[PathBuf::from("nope")]).await?;
            assert_eq!(err_files.len(), 1);

            Ok(())
        })
        .await
    }
}
