use std::path::Path;

use liboxen::api;
use liboxen::command;
use liboxen::constants;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::error::OxenError;
use liboxen::opts::CloneOpts;
use liboxen::test;
use liboxen::util;

#[tokio::test]
async fn test_command_push_clone_pull_push() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track the file
        let train_dirname = "train";
        let train_dir = repo.path.join(train_dirname);
        let og_num_files = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;
        // Commit the train dir
        command::commit(&repo, "Adding training data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create the remote repo
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Add a new file
        let party_ppl_filename = "party_ppl.txt";
        let party_ppl_contents = String::from("Wassup Party Ppl");
        let party_ppl_file_path = repo.path.join(party_ppl_filename);
        util::fs::write_to_path(&party_ppl_file_path, &party_ppl_contents)?;

        // Add and commit and push
        command::add(&repo, &party_ppl_file_path)?;
        let latest_commit = command::commit(&repo, "Adding party_ppl.txt")?;
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo =
                command::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
            let oxen_dir = cloned_repo.path.join(".oxen");
            assert!(oxen_dir.exists());
            command::pull(&cloned_repo).await?;

            // Make sure we pulled all of the train dir
            let cloned_train_dir = cloned_repo.path.join(train_dirname);
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_train_dir);
            assert_eq!(og_num_files, cloned_num_files);

            // Make sure we have the party ppl file from the next commit
            let cloned_party_ppl_path = cloned_repo.path.join(party_ppl_filename);
            assert!(cloned_party_ppl_path.exists());
            let cloned_contents = util::fs::read_from_path(&cloned_party_ppl_path)?;
            assert_eq!(cloned_contents, party_ppl_contents);

            // Make sure that pull updates local HEAD to be correct
            let head = api::local::commits::head_commit(&cloned_repo)?;
            assert_eq!(head.id, latest_commit.id);

            // Make sure we synced all the commits
            let repo_commits = api::local::commits::list(&repo)?;
            let cloned_commits = api::local::commits::list(&cloned_repo)?;
            assert_eq!(repo_commits.len(), cloned_commits.len());

            // Make sure we updated the dbs properly
            let status = command::status(&cloned_repo)?;
            assert!(status.is_clean());

            // Have this side add a file, and send it back over
            let send_it_back_filename = "send_it_back.txt";
            let send_it_back_contents = String::from("Hello from the other side");
            let send_it_back_file_path = cloned_repo.path.join(send_it_back_filename);
            util::fs::write_to_path(&send_it_back_file_path, &send_it_back_contents)?;

            // Add and commit and push
            command::add(&cloned_repo, &send_it_back_file_path)?;
            command::commit(&cloned_repo, "Adding send_it_back.txt")?;
            command::push(&cloned_repo).await?;

            // Pull back from the OG Repo
            command::pull(&repo).await?;
            let old_repo_status = command::status(&repo)?;
            old_repo_status.print_stdout();
            // Make sure we don't modify the timestamps or anything of the OG data
            assert!(!old_repo_status.has_modified_entries());

            let pulled_send_it_back_path = repo.path.join(send_it_back_filename);
            assert!(pulled_send_it_back_path.exists());
            let pulled_contents = util::fs::read_from_path(&pulled_send_it_back_path)?;
            assert_eq!(pulled_contents, send_it_back_contents);

            // Modify the party ppl contents
            let party_ppl_contents = String::from("Late to the party");
            util::fs::write_to_path(&party_ppl_file_path, &party_ppl_contents)?;
            command::add(&repo, &party_ppl_file_path)?;
            command::commit(&repo, "Modified party ppl contents")?;
            command::push(&repo).await?;

            // Pull the modifications
            command::pull(&cloned_repo).await?;
            let pulled_contents = util::fs::read_from_path(&cloned_party_ppl_path)?;
            assert_eq!(pulled_contents, party_ppl_contents);

            println!("----BEFORE-----");
            // Remove a file, add, commit, push the change
            util::fs::remove_file(&send_it_back_file_path)?;
            command::add(&cloned_repo, &send_it_back_file_path)?;
            command::commit(&cloned_repo, "Removing the send it back file")?;
            command::push(&cloned_repo).await?;
            println!("----AFTER-----");

            // Pull down the changes and make sure the file is removed
            command::pull(&repo).await?;
            let pulled_send_it_back_path = repo.path.join(send_it_back_filename);
            assert!(!pulled_send_it_back_path.exists());

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

// This specific flow broke during a demo
// * add file *
// push
// pull
// * modify file *
// push
// pull
// * remove file *
// push
#[tokio::test]
async fn test_command_add_modify_remove_push_pull() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a file
        let filename = "labels.txt";
        let filepath = repo.path.join(filename);
        command::add(&repo, &filepath)?;
        command::commit(&repo, "Adding labels file")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo =
                command::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
            command::pull(&cloned_repo).await?;

            // Modify the file in the cloned dir
            let cloned_filepath = cloned_repo.path.join(filename);
            let changed_content = "messing up the labels";
            util::fs::write_to_path(&cloned_filepath, changed_content)?;
            command::add(&cloned_repo, &cloned_filepath)?;
            command::commit(&cloned_repo, "I messed with the label file")?;

            // Push back to server
            command::push(&cloned_repo).await?;

            // Pull back to original guy
            command::pull(&repo).await?;

            // Make sure content changed
            let pulled_content = util::fs::read_from_path(&filepath)?;
            assert_eq!(pulled_content, changed_content);

            // Delete the file in the og filepath
            util::fs::remove_file(&filepath)?;

            // Stage & Commit & Push the removal
            command::add(&repo, &filepath)?;
            command::commit(&repo, "You mess with it, I remove it")?;
            command::push(&repo).await?;

            command::pull(&cloned_repo).await?;
            assert!(!cloned_filepath.exists());

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

// Make sure we can push again after pulling on the other side, then pull again
#[tokio::test]
async fn test_push_pull_push_pull_on_branch() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        // Track a dir
        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?;

        // Track larger files
        let larger_dir = repo.path.join("large_files");
        command::add(&repo, &larger_dir)?;
        command::commit(&repo, "Adding larger files")?;

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
            command::pull_all(&cloned_repo).await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            assert_eq!(6, cloned_num_files);
            let og_commits = api::local::commits::list(&repo)?;
            let cloned_commits = api::local::commits::list(&cloned_repo)?;
            assert_eq!(og_commits.len(), cloned_commits.len());

            // Create a branch to collab on
            let branch_name = "adding-training-data";
            api::local::branches::create_checkout(&cloned_repo, branch_name)?;

            // Track some more data in the cloned repo
            let hotdog_path = Path::new("data/test/images/hotdog_1.jpg");
            let new_file_path = cloned_repo.path.join("train").join("hotdog_1.jpg");
            util::fs::copy(hotdog_path, &new_file_path)?;
            command::add(&cloned_repo, &new_file_path)?;
            command::commit(&cloned_repo, "Adding one file to train dir")?;

            // Push it back
            command::push_remote_branch(&cloned_repo, constants::DEFAULT_REMOTE_NAME, branch_name)
                .await?;

            // Pull it on the OG side
            command::pull_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name, true)
                .await?;
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);
            // Now there should be 7 train files
            assert_eq!(7, og_num_files);

            // Add another file on the OG side, and push it back
            let hotdog_path = Path::new("data/test/images/hotdog_2.jpg");
            let new_file_path = train_path.join("hotdog_2.jpg");
            util::fs::copy(hotdog_path, &new_file_path)?;
            command::add(&repo, &train_path)?;
            command::commit(&repo, "Adding next file to train dir")?;
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            // Pull it on the second side again
            command::pull_remote_branch(
                &cloned_repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
                false,
            )
            .await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // Now there should be 7 train/ files and 1 in large_files/
            assert_eq!(8, cloned_num_files);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

// Make sure we can push again after pulling on the other side, then pull again
#[tokio::test]
async fn test_push_pull_push_pull_on_other_branch() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|mut repo| async move {
        // Track a dir
        let train_dir = repo.path.join("train");
        let train_paths = [
            Path::new("data/test/images/cat_1.jpg"),
            Path::new("data/test/images/cat_2.jpg"),
            Path::new("data/test/images/cat_3.jpg"),
            Path::new("data/test/images/dog_1.jpg"),
            Path::new("data/test/images/dog_2.jpg"),
        ];
        std::fs::create_dir_all(&train_dir)?;
        for path in train_paths.iter() {
            util::fs::copy(path, train_dir.join(path.file_name().unwrap()))?;
        }

        command::add(&repo, &train_dir)?;
        command::commit(&repo, "Adding train dir")?;

        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

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
            command::pull_all(&cloned_repo).await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // the original training files
            assert_eq!(train_paths.len(), cloned_num_files);

            // Create a branch to collaborate on
            let branch_name = "adding-training-data";
            api::local::branches::create_checkout(&cloned_repo, branch_name)?;

            // Track some more data in the cloned repo
            let hotdog_path = Path::new("data/test/images/hotdog_1.jpg");
            let new_file_path = cloned_repo.path.join("train").join("hotdog_1.jpg");
            util::fs::copy(hotdog_path, &new_file_path)?;
            command::add(&cloned_repo, &new_file_path)?;
            command::commit(&cloned_repo, "Adding one file to train dir")?;

            // Push it back
            command::push_remote_branch(&cloned_repo, constants::DEFAULT_REMOTE_NAME, branch_name)
                .await?;

            // Pull it on the OG side
            command::pull_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                &og_branch.name,
                true,
            )
            .await?;
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);
            // Now there should be still be the original train files, not the new file
            assert_eq!(train_paths.len(), og_num_files);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_push_pull_file_without_extension() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
        let filename = "LICENSE";
        let filepath = repo.path.join(filename);

        let og_content = "I am the License.";
        test::write_txt_file_to_path(&filepath, og_content)?;

        command::add(&repo, filepath)?;
        let commit = command::commit(&repo, "Adding file without extension");

        assert!(commit.is_ok());

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
            let filepath = cloned_repo.path.join(filename);
            let content = util::fs::read_from_path(&filepath)?;
            assert_eq!(og_content, content);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

/*
Test this workflow:

User 1: adds data and creates a branch with more data
    oxen init
    oxen add data/1.txt
    oxen add data/2.txt
    oxen commit -m "Adding initial data"
    oxen push
    oxen checkout -b feature/add-mooooore-data
    oxen add data/3.txt
    oxen add data/4.txt
    oxen add data/5.txt
    oxen push

User 2: clones just the branch with more data, then switches to main branch and pulls
    oxen clone remote.url -b feature/add-mooooore-data
    oxen fetch
    oxen checkout main
    # should only have the data on main

*/
#[tokio::test]
async fn test_push_pull_separate_branch_less_files() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|mut repo| async move {
        // create 5 text files in the repo.path
        for i in 1..6 {
            let filename = format!("{}.txt", i);
            let filepath = repo.path.join(&filename);
            test::write_txt_file_to_path(&filepath, &filename)?;
        }

        // add file 1.txt and 2.txt
        let filepath = repo.path.join("1.txt");
        command::add(&repo, &filepath)?;
        let filepath = repo.path.join("2.txt");
        command::add(&repo, &filepath)?;

        // Commit the files
        command::commit(&repo, "Adding initial data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // Create a branch to collab on
        let branch_name = "feature/add-mooooore-data";
        command::create_checkout(&repo, branch_name)?;

        // Add the rest of the files
        for i in 3..6 {
            let filename = format!("{}.txt", i);
            let filepath = repo.path.join(&filename);
            command::add(&repo, &filepath)?;
        }

        // Commit the files
        command::commit(&repo, "Adding mooooore data")?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            // Clone the branch
            let opts = CloneOpts {
                url: remote_repo.url().to_string(),
                dst: new_repo_dir.to_owned(),
                branch: branch_name.to_owned(),
                shallow: false,
                all: false,
            };
            let cloned_repo = command::clone(&opts).await?;

            // Make sure we have all the files from the branch
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            assert_eq!(cloned_num_files, 5);

            // Switch to main branch and pull
            command::fetch(&cloned_repo).await?;
            command::checkout(&cloned_repo, "main").await?;

            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            assert_eq!(cloned_num_files, 2);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}

#[tokio::test]
async fn test_push_pull_separate_branch_more_files() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|mut repo| async move {
        // create 5 text files in the repo.path
        for i in 1..6 {
            let filename = format!("{}.txt", i);
            let filepath = repo.path.join(&filename);
            test::write_txt_file_to_path(&filepath, &filename)?;
        }

        // add file 1.txt and 2.txt
        let filepath = repo.path.join("1.txt");
        command::add(&repo, &filepath)?;
        let filepath = repo.path.join("2.txt");
        command::add(&repo, &filepath)?;

        // Commit the files
        command::commit(&repo, "Adding initial data")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // Create a branch to collab on
        let branch_name = "feature/add-mooooore-data";
        command::create_checkout(&repo, branch_name)?;

        // Add the rest of the files
        for i in 3..6 {
            let filename = format!("{}.txt", i);
            let filepath = repo.path.join(&filename);
            command::add(&repo, &filepath)?;
        }

        // Commit the files
        command::commit(&repo, "Adding mooooore data")?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            // Clone the branch
            let opts = CloneOpts {
                url: remote_repo.url().to_string(),
                dst: new_repo_dir.to_owned(),
                branch: DEFAULT_BRANCH_NAME.to_string(),
                shallow: false,
                all: false,
            };
            let cloned_repo = command::clone(&opts).await?;

            // Make sure we have all the files from the branch
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            assert_eq!(cloned_num_files, 2);

            // Switch to main branch and pull
            command::fetch(&cloned_repo).await?;

            command::checkout(&cloned_repo, branch_name).await?;

            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            assert_eq!(cloned_num_files, 5);

            api::remote::repositories::delete(&remote_repo).await?;

            Ok(new_repo_dir)
        })
        .await
    })
    .await
}
