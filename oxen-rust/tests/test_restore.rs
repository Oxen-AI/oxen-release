use std::path::Path;

use liboxen::api;
use liboxen::command;
use liboxen::core::df::tabular;
use liboxen::error::OxenError;
use liboxen::model::ContentType;
use liboxen::opts::DFOpts;
use liboxen::opts::RestoreOpts;
use liboxen::test;
use liboxen::util;

#[test]
fn test_command_restore_removed_file_from_head() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write to file
        let hello_filename = "hello.txt";
        let hello_file = repo.path.join(hello_filename);
        util::fs::write_to_path(&hello_file, "Hello World")?;

        // Track the file
        command::add(&repo, &hello_file)?;
        // Commit the file
        command::commit(&repo, "My message")?;

        // Remove the file from disk
        util::fs::remove_file(&hello_file)?;

        // Check that it doesn't exist, then it does after we restore it
        assert!(!hello_file.exists());
        // Restore takes the filename not the full path to the test repo
        // ie: "hello.txt" instead of data/test/runs/repo_data/test/runs_fc1544ab-cd55-4344-aa13-5360dc91d0fe/hello.txt
        command::restore(&repo, RestoreOpts::from_path(hello_filename))?;
        assert!(hello_file.exists());

        Ok(())
    })
}

#[test]
fn test_command_restore_file_from_commit_id() -> Result<(), OxenError> {
    test::run_empty_local_repo_test(|repo| {
        // Write to file
        let hello_filename = "hello.txt";
        let hello_file = repo.path.join(hello_filename);
        util::fs::write_to_path(&hello_file, "Hello World")?;

        // Track the file
        command::add(&repo, &hello_file)?;
        // Commit the file
        command::commit(&repo, "My message")?;

        // Modify the file once
        let first_modification = "Hola Mundo";
        let hello_file = test::modify_txt_file(hello_file, first_modification)?;
        command::add(&repo, &hello_file)?;
        let first_mod_commit = command::commit(&repo, "Changing to spanish")?;

        // Modify again
        let second_modification = "Bonjour le monde";
        let hello_file = test::modify_txt_file(hello_file, second_modification)?;
        command::add(&repo, &hello_file)?;
        command::commit(&repo, "Changing to french")?;

        // Restore from the first commit
        command::restore(
            &repo,
            RestoreOpts::from_path_ref(hello_filename, first_mod_commit.id),
        )?;
        let content = util::fs::read_from_path(&hello_file)?;
        assert!(hello_file.exists());
        assert_eq!(content, first_modification);

        Ok(())
    })
}

#[tokio::test]
async fn test_command_restore_removed_file_from_branch_with_commits_between(
) -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits_async(|repo| async move {
        // (file already created in helper)
        let file_to_remove = repo.path.join("labels.txt");

        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Commit the file
        command::add(&repo, &file_to_remove)?;
        command::commit(&repo, "Adding labels file")?;

        let train_dir = repo.path.join("train");
        command::add(&repo, train_dir)?;
        command::commit(&repo, "Adding train dir")?;

        // Branch
        command::create_checkout(&repo, "remove-labels")?;

        // Delete the file
        util::fs::remove_file(&file_to_remove)?;

        // We should recognize it as missing now
        let status = command::status(&repo)?;
        assert_eq!(status.removed_files.len(), 1);

        // Commit removed file
        command::add(&repo, &file_to_remove)?;
        command::commit(&repo, "Removing labels file")?;

        // Make sure file is not there
        assert!(!file_to_remove.exists());

        // Switch back to main branch
        command::checkout(&repo, orig_branch.name).await?;
        // Make sure we restore file
        assert!(file_to_remove.exists());

        Ok(())
    })
    .await
}

#[test]
fn test_restore_directory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let history = api::local::commits::list(&repo)?;
        let last_commit = history.first().unwrap();

        let annotations_dir = Path::new("annotations");

        // Remove one file
        let bbox_file = annotations_dir.join("train").join("bounding_box.csv");
        let bbox_path = repo.path.join(bbox_file);

        let og_bbox_contents = util::fs::read_from_path(&bbox_path)?;
        util::fs::remove_file(&bbox_path)?;

        // Modify another file
        let readme_file = annotations_dir.join("README.md");
        let readme_path = repo.path.join(readme_file);
        let og_readme_contents = util::fs::read_from_path(&readme_path)?;

        let readme_path = test::append_line_txt_file(readme_path, "Adding s'more")?;

        // Restore the directory
        command::restore(
            &repo,
            RestoreOpts::from_path_ref(annotations_dir, last_commit.id.clone()),
        )?;

        // Make sure the removed file is restored
        let restored_contents = util::fs::read_from_path(&bbox_path)?;
        assert_eq!(og_bbox_contents, restored_contents);

        // Make sure the modified file is restored
        let restored_contents = util::fs::read_from_path(readme_path)?;
        assert_eq!(og_readme_contents, restored_contents);

        Ok(())
    })
}

#[test]
fn test_restore_removed_tabular_data() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let history = api::local::commits::list(&repo)?;
        let last_commit = history.first().unwrap();

        let bbox_file = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_path = repo.path.join(&bbox_file);

        let og_contents = util::fs::read_from_path(&bbox_path)?;
        util::fs::remove_file(&bbox_path)?;

        command::restore(
            &repo,
            RestoreOpts::from_path_ref(bbox_file, last_commit.id.clone()),
        )?;
        let restored_contents = util::fs::read_from_path(&bbox_path)?;
        assert_eq!(og_contents, restored_contents);

        Ok(())
    })
}

#[test]
fn test_restore_modified_tabular_data() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let history = api::local::commits::list(&repo)?;
        let last_commit = history.first().unwrap();

        let bbox_file = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_path = repo.path.join(&bbox_file);

        let og_contents = util::fs::read_from_path(&bbox_path)?;

        let mut opts = DFOpts::empty();
        opts.add_row = Some("train/dog_99.jpg,dog,101.5,32.0,385,330".to_string());
        opts.content_type = ContentType::Csv;
        let mut df = tabular::read_df(&bbox_path, opts)?;
        tabular::write_df(&mut df, &bbox_path)?;

        command::restore(
            &repo,
            RestoreOpts::from_path_ref(bbox_file, last_commit.id.clone()),
        )?;
        let restored_contents = util::fs::read_from_path(&bbox_path)?;
        assert_eq!(og_contents, restored_contents);

        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 0);
        assert!(status.is_clean());

        Ok(())
    })
}

#[test]
fn test_restore_modified_text_data() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let history = api::local::commits::list(&repo)?;
        let last_commit = history.first().unwrap();

        let bbox_file = Path::new("annotations")
            .join("train")
            .join("annotations.txt");
        let bbox_path = repo.path.join(&bbox_file);

        let og_contents = util::fs::read_from_path(&bbox_path)?;
        let new_contents = format!("{og_contents}\nnew 0");
        util::fs::write_to_path(&bbox_path, new_contents)?;

        command::restore(
            &repo,
            RestoreOpts::from_path_ref(bbox_file, last_commit.id.clone()),
        )?;
        let restored_contents = util::fs::read_from_path(&bbox_path)?;
        assert_eq!(og_contents, restored_contents);

        let status = command::status(&repo)?;
        assert_eq!(status.modified_files.len(), 0);
        assert!(status.is_clean());

        Ok(())
    })
}

#[test]
fn test_restore_staged_file() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let bbox_file = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_path = repo.path.join(&bbox_file);

        // Stage file
        command::add(&repo, bbox_path)?;

        // Make sure is staged
        let status = command::status(&repo)?;
        assert_eq!(status.staged_files.len(), 1);
        status.print_stdout();

        // Remove from staged
        command::restore(&repo, RestoreOpts::from_staged_path(bbox_file))?;

        // Make sure is unstaged
        let status = command::status(&repo)?;
        assert_eq!(status.staged_files.len(), 0);

        Ok(())
    })
}

#[test]
fn test_restore_data_frame_with_duplicates() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let ann_file = Path::new("nlp")
            .join("classification")
            .join("annotations")
            .join("train.tsv");
        let ann_path = repo.path.join(&ann_file);
        let orig_df = tabular::read_df(&ann_path, DFOpts::empty())?;
        let og_contents = util::fs::read_from_path(&ann_path)?;

        // Commit
        command::add(&repo, &ann_path)?;
        let commit = command::commit(&repo, "adding data with duplicates")?;

        // Remove
        util::fs::remove_file(&ann_path)?;

        // Restore from commit
        command::restore(&repo, RestoreOpts::from_path_ref(ann_file, commit.id))?;

        // Make sure is same size
        let restored_df = tabular::read_df(&ann_path, DFOpts::empty())?;
        assert_eq!(restored_df.height(), orig_df.height());
        assert_eq!(restored_df.width(), orig_df.width());

        let restored_contents = util::fs::read_from_path(&ann_path)?;
        assert_eq!(og_contents, restored_contents);

        Ok(())
    })
}

#[test]
fn test_restore_bounding_box_data_frame() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let ann_file = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let ann_path = repo.path.join(&ann_file);
        let orig_df = tabular::read_df(&ann_path, DFOpts::empty())?;
        let og_contents = util::fs::read_from_path(&ann_path)?;

        // Commit
        command::add(&repo, &ann_path)?;
        let commit = command::commit(&repo, "adding data with duplicates")?;

        // Remove
        util::fs::remove_file(&ann_path)?;

        // Restore from commit
        command::restore(&repo, RestoreOpts::from_path_ref(ann_file, commit.id))?;

        // Make sure is same size
        let restored_df = tabular::read_df(&ann_path, DFOpts::empty())?;
        assert_eq!(restored_df.height(), orig_df.height());
        assert_eq!(restored_df.width(), orig_df.width());

        let restored_contents = util::fs::read_from_path(&ann_path)?;
        assert_eq!(og_contents, restored_contents);

        Ok(())
    })
}

#[test]
fn test_restore_staged_directory() -> Result<(), OxenError> {
    test::run_training_data_repo_test_no_commits(|repo| {
        let relative_path = Path::new("annotations");
        let annotations_dir = repo.path.join(relative_path);

        // Stage file
        command::add(&repo, annotations_dir)?;

        // Make sure is staged
        let status = command::status(&repo)?;
        assert_eq!(status.staged_dirs.len(), 1);
        assert_eq!(status.staged_files.len(), 6);
        status.print_stdout();

        // Remove from staged
        command::restore(&repo, RestoreOpts::from_staged_path(relative_path))?;

        // Make sure is unstaged
        let status = command::status(&repo)?;
        assert_eq!(status.staged_dirs.len(), 0);
        assert_eq!(status.staged_files.len(), 0);

        Ok(())
    })
}
