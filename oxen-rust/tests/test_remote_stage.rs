use std::path::Path;

use liboxen::api;
use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::model::ContentType;
use liboxen::opts::DFOpts;
use liboxen::test;
use polars::prelude::AnyValue;

#[tokio::test]
async fn test_remote_stage_add_row_commit_clears_remote_status() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
        let remote_repo_copy = remote_repo.clone();

        test::run_empty_dir_test_async(|repo_dir| async move {
            let repo_dir = repo_dir.join("new_repo");
            let cloned_repo =
                command::shallow_clone_url(&remote_repo.remote.url, &repo_dir).await?;

            // Remote add row
            let path = test::test_nlp_classification_csv();
            let mut opts = DFOpts::empty();
            opts.add_row = Some("I am a new row,neutral".to_string());
            opts.content_type = ContentType::Csv;
            command::remote::df(&cloned_repo, path, opts).await?;

            // Make sure it is listed as modified
            let branch = api::local::branches::current_branch(&cloned_repo)?.unwrap();
            let directory = Path::new("");
            let opts = StagedDataOpts {
                is_remote: true,
                ..Default::default()
            };
            let status = command::remote::status(&remote_repo, &branch, directory, &opts).await?;
            assert_eq!(status.staged_files.len(), 1);

            // Commit it
            command::remote::commit(&cloned_repo, "Remotely committing").await?;

            // Now status should be empty
            let status = command::remote::status(&remote_repo, &branch, directory, &opts).await?;
            assert_eq!(status.staged_files.len(), 0);

            Ok(repo_dir)
        })
        .await?;

        Ok(remote_repo_copy)
    })
    .await
}

#[tokio::test]
async fn test_remote_stage_delete_row_clears_remote_status() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
        let remote_repo_copy = remote_repo.clone();

        test::run_empty_dir_test_async(|repo_dir| async move {
            let repo_dir = repo_dir.join("new_repo");

            let cloned_repo =
                command::shallow_clone_url(&remote_repo.remote.url, &repo_dir).await?;

            // Remote add row
            let path = test::test_nlp_classification_csv();
            let mut opts = DFOpts::empty();
            opts.add_row = Some("I am a new row,neutral".to_string());
            opts.content_type = ContentType::Csv;
            // Grab ID from the row we just added
            let df = command::remote::df(&cloned_repo, &path, opts).await?;
            let uuid = match df.get(0).unwrap().first().unwrap() {
                AnyValue::Utf8(s) => s.to_string(),
                _ => panic!("Expected string"),
            };

            // Make sure it is listed as modified
            let branch = api::local::branches::current_branch(&cloned_repo)?.unwrap();
            let directory = Path::new("");
            let opts = StagedDataOpts {
                is_remote: true,
                ..Default::default()
            };
            let status = command::remote::status(&remote_repo, &branch, directory, &opts).await?;
            assert_eq!(status.staged_files.len(), 1);

            // Delete it
            let mut delete_opts = DFOpts::empty();
            delete_opts.delete_row = Some(uuid);
            command::remote::df(&cloned_repo, &path, delete_opts).await?;

            // Now status should be empty
            let status = command::remote::status(&remote_repo, &branch, directory, &opts).await?;
            assert_eq!(status.staged_files.len(), 0);

            Ok(repo_dir)
        })
        .await?;

        Ok(remote_repo_copy)
    })
    .await
}
