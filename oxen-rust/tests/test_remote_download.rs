use liboxen::api;
use liboxen::command;
use liboxen::constants;
use liboxen::error::OxenError;
use liboxen::test;
use liboxen::util;

use std::path::Path;

#[tokio::test]
async fn test_remote_download_directory() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|mut repo| async move {
        // write text files to dir
        let dir = repo.path.join("train");
        util::fs::create_dir_all(&dir)?;
        let num_files = 33;
        for i in 0..num_files {
            let path = dir.join(format!("file_{}.txt", i));
            util::fs::write_to_path(&path, format!("lol hi {}", i))?;
        }
        command::add(&repo, &dir)?;
        command::commit(&repo, "adding text files")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Now list the remote
        let branch = api::local::branches::current_branch(&repo)?.unwrap();
        let dir = Path::new("train");

        // Download the directory
        let output_dir = repo.path.join("output");
        command::remote::download(&remote_repo, &dir, &output_dir, &branch.name).await?;

        // Check that the files are there
        for i in 0..num_files {
            let path = output_dir.join("train").join(format!("file_{}.txt", i));
            println!("checking path: {:?}", path);
            assert!(path.exists());
        }

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_remote_download_directory_local_path() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|mut repo| async move {
        // write text files to dir
        let dir = repo.path.join("train");
        util::fs::create_dir_all(&dir)?;
        let num_files = 33;
        for i in 0..num_files {
            let path = dir.join(format!("file_{}.txt", i));
            util::fs::write_to_path(&path, format!("lol hi {}", i))?;
        }
        command::add(&repo, &dir)?;
        command::commit(&repo, "adding text files")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it real good
        command::push(&repo).await?;

        // Now list the remote
        let branch = api::local::branches::current_branch(&repo)?.unwrap();
        let dir = Path::new("train");

        // Download the directory
        let output_dir = Path::new("output");
        command::remote::download(&remote_repo, &dir, &output_dir, &branch.name).await?;

        // Check that the files are there
        for i in 0..num_files {
            let path = output_dir.join("train").join(format!("file_{}.txt", i));
            assert!(path.exists());
        }

        util::fs::remove_dir_all(output_dir)?;

        Ok(())
    })
    .await
}
