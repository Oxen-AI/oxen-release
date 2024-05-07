//! # oxen remote commit
//!
//! Commit remote staged data on a branch
//!

use crate::api;
use crate::config::UserConfig;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, NewCommitBody};

/// Commit changes that are staged on the remote repository on the current
/// checked out local branch
pub async fn commit(repo: &LocalRepository, message: &str) -> Result<Option<Commit>, OxenError> {
    let branch = api::local::branches::current_branch(repo)?;
    if branch.is_none() {
        return Err(OxenError::must_be_on_valid_branch());
    }
    let branch = branch.unwrap();

    let remote_repo = api::remote::repositories::get_default_remote(repo).await?;
    let cfg = UserConfig::get()?;
    let body = NewCommitBody {
        message: message.to_string(),
        author: cfg.name,
        email: cfg.email,
    };
    let user_id = UserConfig::identifier()?;
    let commit = api::remote::staging::commit(&remote_repo, &branch.name, &user_id, &body).await?;
    Ok(Some(commit))
}

#[cfg(test)]
mod tests {
    // use std::path::Path;

    // use crate::api;
    use crate::command;
    // use crate::config::UserConfig;
    // use crate::constants;
    use crate::error::OxenError;
    use crate::model::ContentType;
    // use crate::model::NewCommitBody;
    use crate::opts::DFOpts;
    use crate::test;

    #[tokio::test]
    async fn test_remote_commit_fails_if_schema_changed() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|repo_dir| async move {
                let cloned_repo =
                    command::clone_url(&remote_repo.remote.url, &repo_dir.join("new_repo")).await?;

                // Remote stage row
                let path = test::test_nlp_classification_csv();

                // Index the dataset
                command::remote::df::index_dataset(&cloned_repo, &path).await?;

                log::debug!("the path in question is {:?}", path);
                let mut opts = DFOpts::empty();

                opts.add_row =
                    Some("{\"text\": \"I am a new row\", \"label\": \"neutral\"}".to_string());
                opts.content_type = ContentType::Json;
                command::remote::df(&cloned_repo, &path, opts).await?;

                // Local add col
                let full_path = cloned_repo.path.join(path);
                let mut opts = DFOpts::empty();
                opts.add_col = Some("is_something:n/a:str".to_string());
                opts.output = Some(full_path.to_path_buf()); // write back to same path
                command::df(&full_path, opts)?;
                command::add(&cloned_repo, &full_path)?;

                // Commit and push the changed schema
                command::commit(&cloned_repo, "Changed the schema 😇")?;
                command::push(&cloned_repo).await?;

                // Try to commit the remote changes, should fail
                let result = command::remote::commit(&cloned_repo, "Remotely committing").await;
                println!("{:?}", result);
                assert!(result.is_err());

                // Now status should be empty
                // let branch = api::local::branches::current_branch(&cloned_repo)?.unwrap();
                // let directory = Path::new("");
                // let opts = StagedDataOpts {
                //     is_remote: true,
                //     ..Default::default()
                // };
                // let status = command::remote_status(&remote_repo, &branch, directory, &opts).await?;
                // assert_eq!(status.modified_files.len(), 1);

                Ok(repo_dir)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // #[tokio::test]
    // async fn test_remote_commit_staging_behind_main() -> Result<(), OxenError> {
    //     test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
    //         // Create branch behind-main off main
    //         let new_branch = "behind-main";
    //         let main_branch = "main";

    //         let main_path = "images/folder";
    //         let identifier = UserConfig::identifier()?;

    //         api::remote::branches::create_from_or_get(&remote_repo, new_branch, main_branch)
    //             .await?;
    //         // assert_eq!(branch.name, branch_name);

    //         // Advance head on main branch, leave behind-main behind
    //         let path = test::test_img_file();
    //         let result = api::remote::staging::add_file(
    //             &remote_repo,
    //             main_branch,
    //             &identifier,
    //             main_path,
    //             path,
    //         )
    //         .await;
    //         assert!(result.is_ok());

    //         let body = NewCommitBody {
    //             message: "Add to main".to_string(),
    //             author: "Test User".to_string(),
    //             email: "test@oxen.ai".to_string(),
    //         };
    //         api::remote::staging::commit(&remote_repo, main_branch, &identifier, &body).await?;

    //         // Make an EMPTY commit to behind-main
    //         let body = NewCommitBody {
    //             message: "Add behind main".to_string(),
    //             author: "Test User".to_string(),
    //             email: "test@oxen.ai".to_string(),
    //         };
    //         api::remote::staging::commit(&remote_repo, new_branch, &identifier, &body).await?;

    //         // Add file at images/folder to behind-main, committed to main
    //         let image_path = test::test_img_file();
    //         let result = api::remote::staging::add_file(
    //             &remote_repo,
    //             new_branch,
    //             &identifier,
    //             main_path,
    //             image_path,
    //         )
    //         .await;
    //         assert!(result.is_ok());

    //         // Check status: if valid, there should be an entry here for the file at images/folder
    //         let page_num = constants::DEFAULT_PAGE_NUM;
    //         let page_size = constants::DEFAULT_PAGE_SIZE;
    //         let path = Path::new("");
    //         let entries = api::remote::staging::status(
    //             &remote_repo,
    //             new_branch,
    //             &identifier,
    //             path,
    //             page_num,
    //             page_size,
    //         )
    //         .await?;

    //         assert_eq!(entries.added_files.entries.len(), 1);
    //         assert_eq!(entries.added_files.total_entries, 1);

    //         Ok(remote_repo)
    //     })
    //     .await
    // }
}
