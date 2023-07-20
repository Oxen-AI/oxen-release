use liboxen::api;
use liboxen::command;
use liboxen::constants;
use liboxen::error::OxenError;
use liboxen::test;
use liboxen::util;

use futures::future;

#[tokio::test]
async fn test_command_push_one_commit() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        let mut repo = repo;

        // Track the file
        let train_dir = repo.path.join("train");
        let num_files = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;
        // Commit the train dir
        let commit = command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        let page_num = 1;
        let page_size = num_files + 10;
        let entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, "train", page_num, page_size)
                .await?;
        assert_eq!(entries.total_entries, num_files);
        assert_eq!(entries.entries.len(), num_files);

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

#[tokio::test]
async fn test_command_push_one_commit_check_is_synced() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        let mut repo = repo;

        // Track the train and annotations dir
        let train_dir = repo.path.join("train");
        let annotations_dir = repo.path.join("annotations");

        command::add(&repo, &train_dir)?;
        command::add(&repo, &annotations_dir)?;
        // Commit the train dir
        let commit = command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Sleep so it can unpack...
        std::thread::sleep(std::time::Duration::from_secs(2));

        let is_synced = api::remote::commits::commit_is_synced(&remote_repo, &commit.id)
            .await?
            .unwrap();
        assert!(is_synced.is_valid);

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

#[tokio::test]
async fn test_command_push_multiple_commit_check_is_synced() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        let mut repo = repo;

        // Track the train and annotations dir
        let train_dir = repo.path.join("train");
        let train_bounding_box = repo
            .path
            .join("annotations")
            .join("train")
            .join("bounding_box.csv");

        command::add(&repo, &train_dir)?;
        command::add(&repo, &train_bounding_box)?;
        // Commit the train dir
        command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Sleep so it can unpack...
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Add and commit the rest of the annotations
        // The nlp annotations have duplicates which broke the system at a time
        let annotations_dir = repo.path.join("nlp");
        command::add(&repo, &annotations_dir)?;
        let commit = command::commit(&repo, "adding the rest of the annotations")?;

        // Push again
        command::push(&repo).await?;

        let is_synced = api::remote::commits::commit_is_synced(&remote_repo, &commit.id)
            .await?
            .unwrap();
        assert!(is_synced.is_valid);

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

#[tokio::test]
async fn test_command_push_inbetween_two_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        let mut repo = repo;
        // Track the train dir
        let train_dir = repo.path.join("train");
        let num_train_files = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;
        // Commit the train dur
        command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the remote repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push the files
        command::push(&repo).await?;

        // Track the test dir
        let test_dir = repo.path.join("test");
        let num_test_files = util::fs::count_files_in_dir(&test_dir);
        command::add(&repo, &test_dir)?;
        let commit = command::commit(&repo, "Adding test data")?;

        // Push the files
        command::push(&repo).await?;

        let page_num = 1;
        let page_size = num_train_files + num_test_files + 5;
        let train_entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, "/train", page_num, page_size)
                .await?;
        let test_entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, "/test", page_num, page_size)
                .await?;
        assert_eq!(
            train_entries.total_entries + test_entries.total_entries,
            num_train_files + num_test_files
        );
        assert_eq!(
            train_entries.entries.len() + test_entries.entries.len(),
            num_train_files + num_test_files
        );

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

#[tokio::test]
async fn test_command_push_after_two_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        // Make mutable copy so we can set remote
        let mut repo = repo;

        // Track the train dir
        let train_dir = repo.path.join("train");
        command::add(&repo, &train_dir)?;
        // Commit the train dur
        command::commit(&repo, "Adding training data")?;

        // Track the test dir
        let test_dir = repo.path.join("test");
        let num_test_files = util::fs::rcount_files_in_dir(&test_dir);
        command::add(&repo, &test_dir)?;
        let commit = command::commit(&repo, "Adding test data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the remote repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push the files
        command::push(&repo).await?;

        let page_num = 1;
        let entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, ".", page_num, 10).await?;
        assert_eq!(entries.total_entries, 2);
        assert_eq!(entries.entries.len(), 2);

        let page_size = num_test_files + 10;
        let entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, "test", page_num, page_size)
                .await?;
        assert_eq!(entries.total_entries, num_test_files);
        assert_eq!(entries.entries.len(), num_test_files);

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

// This broke when you tried to add the "." directory to add everything, after already committing the train directory.
#[tokio::test]
async fn test_command_push_after_two_commits_adding_dot() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async {
        // Make mutable copy so we can set remote
        let mut repo = repo;

        // Track the train dir
        let train_dir = repo.path.join("train");

        command::add(&repo, &train_dir)?;
        // Commit the train dur
        command::commit(&repo, "Adding training data")?;

        // Track the rest of the files
        let full_dir = &repo.path;
        let num_files = util::fs::count_items_in_dir(full_dir);
        command::add(&repo, full_dir)?;
        let commit = command::commit(&repo, "Adding rest of data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the remote repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push the files
        command::push(&repo).await?;

        let page_num = 1;
        let page_size = num_files + 10;
        let entries =
            api::remote::dir::list_dir(&remote_repo, &commit.id, ".", page_num, page_size).await?;
        assert_eq!(entries.total_entries, num_files);
        assert_eq!(entries.entries.len(), num_files);

        api::remote::repositories::delete(&remote_repo).await?;

        future::ok::<(), OxenError>(()).await
    })
    .await
}

#[tokio::test]
async fn test_cannot_push_if_remote_not_set() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        // Track the file
        let train_dirname = "train";
        let train_dir = repo.path.join(train_dirname);
        command::add(&repo, &train_dir)?;
        // Commit the train dir
        command::commit(&repo, "Adding training data")?;

        // Should not be able to push
        let result = command::push(&repo).await;
        assert!(result.is_err());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_push_branch_with_with_no_new_commits() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a dir
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        let new_branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, new_branch_name)?;

        // Push new branch, without any new commits, should still create the branch
        command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, new_branch_name).await?;

        let remote_branches = api::remote::branches::list(&remote_repo).await?;
        assert_eq!(2, remote_branches.len());

        api::remote::repositories::delete(&remote_repo).await?;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_cannot_push_two_separate_empty_roots() -> Result<(), OxenError> {
    test::run_no_commit_remote_repo_test(|remote_repo| async move {
        let ret_repo = remote_repo.clone();

        // Clone the first repo
        test::run_empty_dir_test_async(|first_repo_dir| async move {
            let first_cloned_repo =
                command::clone_url(&remote_repo.remote.url, &first_repo_dir).await?;

            // Clone the second repo
            test::run_empty_dir_test_async(|second_repo_dir| async move {
                let second_cloned_repo =
                    command::clone_url(&remote_repo.remote.url, &second_repo_dir).await?;

                // Add to the first repo, after we have the second repo cloned
                let new_file = "new_file.txt";
                let new_file_path = first_cloned_repo.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                command::add(&first_cloned_repo, &new_file_path)?;
                command::commit(&first_cloned_repo, "Adding first file path.")?;
                command::push(&first_cloned_repo).await?;

                // The push to the second version of the same repo should fail
                // Adding two commits to have a longer history that also should fail
                let new_file = "new_file_2.txt";
                let new_file_path = second_cloned_repo.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
                command::add(&second_cloned_repo, &new_file_path)?;
                command::commit(&second_cloned_repo, "Adding second file path.")?;

                let new_file = "new_file_3.txt";
                let new_file_path = second_cloned_repo.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
                command::add(&second_cloned_repo, &new_file_path)?;
                command::commit(&second_cloned_repo, "Adding third file path.")?;

                // Push should FAIL
                let result = command::push(&second_cloned_repo).await;
                assert!(result.is_err());

                Ok(second_repo_dir)
            })
            .await?;

            Ok(first_repo_dir)
        })
        .await?;

        Ok(ret_repo)
    })
    .await
}

// Test that we cannot push two completely separate local repos to the same history
// 1) Create repo A with data
// 2) Create repo B with data
// 3) Push Repo A
// 4) Push repo B to repo A and fail
#[tokio::test]
async fn test_cannot_push_two_separate_repos() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|mut repo_1| async move {
        test::run_training_data_repo_test_fully_committed_async(|mut repo_2| async move {
            // Add to the first repo
            let new_file = "new_file.txt";
            let new_file_path = repo_1.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
            command::add(&repo_1, &new_file_path)?;
            command::commit(&repo_1, "Adding first file path.")?;
            // Set/create the proper remote
            let remote = test::repo_remote_url_from(&repo_1.dirname());
            command::config::set_remote(&mut repo_1, constants::DEFAULT_REMOTE_NAME, &remote)?;
            test::create_remote_repo(&repo_1).await?;
            command::push(&repo_1).await?;

            // Adding two commits to have a longer history that also should fail
            let new_file = "new_file_2.txt";
            let new_file_path = repo_2.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
            command::add(&repo_2, &new_file_path)?;
            command::commit(&repo_2, "Adding second file path.")?;

            let new_file = "new_file_3.txt";
            let new_file_path = repo_2.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
            command::add(&repo_2, &new_file_path)?;
            command::commit(&repo_2, "Adding third file path.")?;

            // Set remote to the same as the first repo
            command::config::set_remote(&mut repo_2, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push should FAIL
            let result = command::push(&repo_2).await;
            assert!(result.is_err());

            Ok(())
        })
        .await?;

        Ok(())
    })
    .await
}
