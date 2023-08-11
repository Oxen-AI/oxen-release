use std::path::Path;
use std::path::PathBuf;

use liboxen::api;
use liboxen::command;
use liboxen::core::index::CommitEntryReader;
use liboxen::error::OxenError;
use liboxen::model::StagedEntryStatus;
use liboxen::opts::RestoreOpts;
use liboxen::opts::RmOpts;
use liboxen::test;
use liboxen::util;

/// Should be able to use `oxen rm -r` then restore to get files back
///
/// $ oxen rm -r train/
/// $ oxen restore --staged train/
/// $ oxen restore train/
#[tokio::test]
async fn test_rm_directory_restore_directory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        let rm_dir = PathBuf::from("train");
        let full_path = repo.path.join(&rm_dir);
        let num_files = util::fs::rcount_files_in_dir(&full_path);

        // Remove directory
        let opts = RmOpts {
            path: rm_dir.to_owned(),
            recursive: true,
            staged: false,
            remote: false,
        };
        command::rm(&repo, &opts).await?;

        // Make sure we staged these removals
        let status = command::status(&repo)?;
        status.print_stdout();
        assert_eq!(num_files, status.staged_files.len());
        for (_path, entry) in status.staged_files.iter() {
            assert_eq!(entry.status, StagedEntryStatus::Removed);
        }
        // Make sure directory is no longer on disk
        assert!(!full_path.exists());

        // Restore the content from staging area
        let opts = RestoreOpts::from_staged_path(&rm_dir);
        command::restore(&repo, opts)?;

        // This should have removed all the staged files, but not restored from disk yet.
        let status = command::status(&repo)?;
        status.print_stdout();
        assert_eq!(0, status.staged_files.len());
        assert_eq!(num_files, status.removed_files.len());

        // This should restore all the files from the HEAD commit
        let opts = RestoreOpts::from_path(&rm_dir);
        command::restore(&repo, opts)?;

        let status = command::status(&repo)?;
        status.print_stdout();

        let num_restored = util::fs::rcount_files_in_dir(&full_path);
        assert_eq!(num_restored, num_files);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_rm_sub_directory() -> Result<(), OxenError> {
    test::run_empty_data_repo_test_no_commits_async(|repo| async move {
        // create the images directory
        let images_dir = repo.path.join("images").join("cats");
        util::fs::create_dir_all(&images_dir)?;

        // Add and commit the cats
        for i in 1..=3 {
            let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
            let repo_filepath = images_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;
        }

        command::add(&repo, &images_dir)?;
        command::commit(&repo, "Adding initial cat images")?;

        // Create branch
        let branch_name = "remove-data";
        command::create_checkout(&repo, branch_name)?;

        // Remove all the cat images
        for i in 1..=3 {
            let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
            util::fs::remove_file(&repo_filepath)?;
        }

        let mut rm_opts = RmOpts::from_path(Path::new("images"));
        rm_opts.recursive = true;
        command::rm(&repo, &rm_opts).await?;
        let commit = command::commit(&repo, "Removing cat images")?;

        for i in 1..=3 {
            let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
            assert!(!repo_filepath.exists())
        }

        let entries = api::local::entries::list_all(&repo, &commit)?;
        assert_eq!(entries.len(), 0);

        let dir_reader = CommitEntryReader::new(&repo, &commit)?;
        let dirs = dir_reader.list_dirs()?;
        for dir in dirs.iter() {
            println!("dir: {:?}", dir);
        }

        // Should just be the root dir, we removed the images and images/cat dir
        assert_eq!(dirs.len(), 1);

        Ok(())
    })
    .await
}
