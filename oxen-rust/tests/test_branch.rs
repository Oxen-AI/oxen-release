use liboxen::api;
use liboxen::command;
use liboxen::constants;
use liboxen::error::OxenError;
use liboxen::test;

#[test]
fn test_rename_current_branch() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Create and checkout branch
        let og_branch_name = "feature/world-explorer";
        api::local::branches::create_checkout(&repo, og_branch_name)?;

        // Rename branch
        let new_branch_name = "feature/brave-new-world";
        api::local::branches::rename_current_branch(&repo, new_branch_name)?;

        // Check that the branch name has changed
        let current_branch = api::local::branches::current_branch(&repo)?.unwrap();
        assert_eq!(current_branch.name, new_branch_name);

        // Check that old branch no longer exists
        api::local::branches::list(&repo)?
            .iter()
            .for_each(|branch| {
                assert_ne!(branch.name, og_branch_name);
            });

        Ok(())
    })
}

#[tokio::test]
async fn test_delete_remote_branch() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // Create new branch
        let new_branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, new_branch_name)?;

        // Push new branch
        command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, new_branch_name).await?;

        // Delete the branch
        api::remote::branches::delete(&remote_repo, new_branch_name).await?;

        let remote_branches = api::remote::branches::list(&remote_repo).await?;
        assert_eq!(1, remote_branches.len());

        api::remote::repositories::delete(&remote_repo).await?;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_should_not_push_branch_that_does_not_exist() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push main branch first
        if command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, "main")
            .await
            .is_err()
        {
            panic!("Pushing main branch should work");
        }

        // Then try to push branch that doesn't exist
        if command::push_remote_branch(
            &repo,
            constants::DEFAULT_REMOTE_NAME,
            "branch-does-not-exist",
        )
        .await
        .is_ok()
        {
            panic!("Should not be able to push branch that does not exist");
        }

        let remote_branches = api::remote::branches::list(&remote_repo).await?;
        assert_eq!(1, remote_branches.len());

        api::remote::repositories::delete(&remote_repo).await?;

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_delete_branch() -> Result<(), OxenError> {
    test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
        // Get the original branches
        let og_branches = api::local::branches::list(&repo)?;
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Must checkout main again before deleting
        command::checkout(&repo, og_branch.name).await?;

        // Now we can delete
        api::local::branches::delete(&repo, branch_name)?;

        // Should be same num as og_branches
        let leftover_branches = api::local::branches::list(&repo)?;
        assert_eq!(og_branches.len(), leftover_branches.len());

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_cannot_delete_branch_you_are_on() -> Result<(), OxenError> {
    test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
        let branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add another commit on this branch that moves us ahead of main
        if api::local::branches::delete(&repo, branch_name).is_ok() {
            panic!("Should not be able to delete the branch you are on");
        }

        Ok(())
    })
    .await
}

#[test]
fn test_cannot_force_delete_branch_you_are_on() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add another commit on this branch that moves us ahead of main
        if api::local::branches::force_delete(&repo, branch_name).is_ok() {
            panic!("Should not be able to force delete the branch you are on");
        }

        Ok(())
    })
}

#[tokio::test]
async fn test_cannot_delete_branch_that_is_ahead_of_current() -> Result<(), OxenError> {
    test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
        let og_branches = api::local::branches::list(&repo)?;
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add another commit on this branch
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        // Checkout main again
        command::checkout(&repo, og_branch.name).await?;

        // Should not be able to delete `my-branch` because it is ahead of `main`
        if api::local::branches::delete(&repo, branch_name).is_ok() {
            panic!("Should not be able to delete the branch that is ahead of the one you are on");
        }

        // Should be one less branch
        let leftover_branches = api::local::branches::list(&repo)?;
        assert_eq!(og_branches.len(), leftover_branches.len() - 1);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_force_delete_branch_that_is_ahead_of_current() -> Result<(), OxenError> {
    test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
        let og_branches = api::local::branches::list(&repo)?;
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let branch_name = "my-branch";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add another commit on this branch
        let labels_path = repo.path.join("labels.txt");
        command::add(&repo, labels_path)?;
        command::commit(&repo, "adding initial labels file")?;

        // Checkout main again
        command::checkout(&repo, og_branch.name).await?;

        // Force delete
        api::local::branches::force_delete(&repo, branch_name)?;

        // Should be one less branch
        let leftover_branches = api::local::branches::list(&repo)?;
        assert_eq!(og_branches.len(), leftover_branches.len());

        Ok(())
    })
    .await
}
