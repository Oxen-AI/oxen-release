use liboxen::api;
use liboxen::command;
use liboxen::constants;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::error::OxenError;
use liboxen::test;

#[tokio::test]
async fn test_fetch_branches() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|mut repo| async move {
        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Create a few local branches and push them
        let branches = ["test_moo", "test_moo_2"];
        for branch in branches.iter() {
            command::create_checkout(&repo, branch)?;
            let filepath = repo.path.join(format!("file_{}.txt", branch));
            test::write_txt_file_to_path(&filepath, &format!("a file on {}", branch))?;
            command::add(&repo, &filepath)?;
            command::commit(&repo, &format!("Adding file on {}", branch))?;
            command::push(&repo).await?;
        }

        // Clone the main branch, then fetch the others
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo =
                command::clone_url(&remote_repo.remote.url, &new_repo_dir.join("new_repo")).await?;
            let branches = api::local::branches::list(&cloned_repo)?;

            assert_eq!(1, branches.len());

            command::fetch(&cloned_repo).await?;

            let branches = api::local::branches::list(&cloned_repo)?;
            assert_eq!(3, branches.len());

            let current_branch = api::local::branches::current_branch(&cloned_repo)?.unwrap();
            assert_eq!(current_branch.name, DEFAULT_BRANCH_NAME);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}
