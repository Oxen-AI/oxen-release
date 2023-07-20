use std::path::PathBuf;

use liboxen::command;
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
        assert_eq!(num_files, status.added_files.len());
        for (_path, entry) in status.added_files.iter() {
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
        assert_eq!(0, status.added_files.len());
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
