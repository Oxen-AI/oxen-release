use std::path::Path;

use liboxen::api;
use liboxen::command;
use liboxen::constants;
use liboxen::core::df::tabular;
use liboxen::core::index::CommitEntryReader;
use liboxen::error::OxenError;
use liboxen::opts::DFOpts;
use liboxen::test;
use liboxen::util;

#[tokio::test]
async fn test_pull_multiple_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a file
        let filename = "labels.txt";
        let file_path = repo.path.join(filename);
        command::add(&repo, &file_path)?;
        command::commit(&repo, "Adding labels file")?;

        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?;

        let test_path = repo.path.join("test");
        command::add(&repo, &test_path)?;
        command::commit(&repo, "Adding test dir")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo =
                command::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
            command::pull(&cloned_repo).await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // 2 test, 5 train, 1 labels
            assert_eq!(8, cloned_num_files);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_pull_data_frame() -> Result<(), OxenError> {
    test::run_select_data_repo_test_no_commits_async("annotations", |mut repo| async move {
        // Track a file
        let filename = "annotations/train/bounding_box.csv";
        let file_path = repo.path.join(filename);
        let og_df = tabular::read_df(&file_path, DFOpts::empty())?;
        let og_contents = util::fs::read_from_path(&file_path)?;

        command::add(&repo, &file_path)?;
        command::commit(&repo, "Adding bounding box file")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo =
                command::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
            command::pull(&cloned_repo).await?;
            let file_path = cloned_repo.path.join(filename);

            let cloned_df = tabular::read_df(&file_path, DFOpts::empty())?;
            let cloned_contents = util::fs::read_from_path(&file_path)?;
            assert_eq!(og_df.height(), cloned_df.height());
            assert_eq!(og_df.width(), cloned_df.width());
            assert_eq!(cloned_contents, og_contents);

            // Status should be empty too
            let status = command::status(&cloned_repo)?;
            status.print_stdout();
            assert!(status.is_clean());

            // Make sure that the schema gets pulled
            let schemas = command::schemas::list(&repo, None)?;
            assert!(!schemas.is_empty());

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

// Test that we pull down the proper data frames
#[tokio::test]
async fn test_pull_multiple_data_frames_multiple_schemas() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
        let filename = Path::new("nlp")
            .join("classification")
            .join("annotations")
            .join("train.tsv");
        let file_path = repo.path.join(filename);
        let og_df = tabular::read_df(&file_path, DFOpts::empty())?;
        let og_sentiment_contents = util::fs::read_from_path(&file_path)?;

        let schemas = command::schemas::list(&repo, None)?;
        let num_schemas = schemas.len();

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo =
                command::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
            command::pull(&cloned_repo).await?;

            let filename = Path::new("nlp")
                .join("classification")
                .join("annotations")
                .join("train.tsv");
            let file_path = cloned_repo.path.join(&filename);
            let cloned_df = tabular::read_df(&file_path, DFOpts::empty())?;
            let cloned_contents = util::fs::read_from_path(&file_path)?;
            assert_eq!(og_df.height(), cloned_df.height());
            assert_eq!(og_df.width(), cloned_df.width());
            assert_eq!(cloned_contents, og_sentiment_contents);
            println!("Cloned {filename:?} {cloned_df}");

            // Status should be empty too
            let status = command::status(&cloned_repo)?;
            status.print_stdout();
            assert!(status.is_clean());

            // Make sure we grab the same amount of schemas
            let pulled_schemas = command::schemas::list(&repo, None)?;
            assert_eq!(pulled_schemas.len(), num_schemas);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_pull_full_commit_history() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // First commit
        let filename = "labels.txt";
        let filepath = repo.path.join(filename);
        command::add(&repo, &filepath)?;
        command::commit(&repo, "Adding labels file")?;

        // Second commit
        let new_filename = "new.txt";
        let new_filepath = repo.path.join(new_filename);
        util::fs::write_to_path(&new_filepath, "hallo")?;
        command::add(&repo, &new_filepath)?;
        command::commit(&repo, "Adding a new file")?;

        // Third commit
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?;

        // Fourth commit
        let test_path = repo.path.join("test");
        command::add(&repo, &test_path)?;
        command::commit(&repo, "Adding test dir")?;

        // Get local history
        let local_history = api::local::commits::list(&repo)?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo =
                command::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
            command::pull(&cloned_repo).await?;

            // Get cloned history, which should fall back to API if not found locally
            let cloned_history = api::local::commits::list(&cloned_repo)?;

            // Make sure the histories match
            assert_eq!(local_history.len(), cloned_history.len());

            // Make sure we have grabbed all the history dirs
            let hidden_dir = util::fs::oxen_hidden_dir(&cloned_repo.path);
            let history_dir = hidden_dir.join(Path::new(constants::HISTORY_DIR));
            for commit in cloned_history.iter() {
                let commit_history_dir = history_dir.join(&commit.id);
                assert!(commit_history_dir.exists());

                // make sure we can successfully open the db and read entries
                let reader = CommitEntryReader::new(&cloned_repo, commit)?;
                let entries = reader.list_entries();
                assert!(entries.is_ok());
            }

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_pull_shallow_local_status_is_err() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
        let remote_repo_copy = remote_repo.clone();

        test::run_empty_dir_test_async(|repo_dir| async move {
            let cloned_repo =
                command::shallow_clone_url(&remote_repo.remote.url, &repo_dir).await?;

            let result = command::status(&cloned_repo);
            assert!(result.is_err());

            Ok(repo_dir)
        })
        .await?;

        Ok(remote_repo_copy)
    })
    .await
}

#[tokio::test]
async fn test_pull_shallow_local_add_is_err() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
        let remote_repo_copy = remote_repo.clone();

        test::run_empty_dir_test_async(|repo_dir| async move {
            let cloned_repo =
                command::shallow_clone_url(&remote_repo.remote.url, &repo_dir).await?;

            let path = cloned_repo.path.join("README.md");
            util::fs::write_to_path(&path, "# Can't add this")?;

            let result = command::add(&cloned_repo, path);
            assert!(result.is_err());

            Ok(repo_dir)
        })
        .await?;

        Ok(remote_repo_copy)
    })
    .await
}
