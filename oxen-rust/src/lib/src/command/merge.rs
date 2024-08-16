//! # oxen merge
//!
//! Merge a branch into the current branch
//!

use crate::core::v0_10_0::index::Merger;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;

/// # Merge a branch into the current branch
/// Checks for simple fast forward merge, or if current branch has diverged from the merge branch
/// it will perform a 3 way merge
/// If there are conflicts, it will abort and show the conflicts to be resolved in the `status` command
pub fn merge<S: AsRef<str>>(
    repo: &LocalRepository,
    merge_branch_name: S,
) -> Result<Option<Commit>, OxenError> {
    let merge_branch_name = merge_branch_name.as_ref();
    if !repositories::branches::exists(repo, merge_branch_name)? {
        return Err(OxenError::local_branch_not_found(merge_branch_name));
    }

    let base_branch =
        repositories::branches::current_branch(repo)?.ok_or(OxenError::must_be_on_valid_branch())?;
    let merge_branch = repositories::branches::get_by_name(repo, merge_branch_name)?
        .ok_or(OxenError::local_branch_not_found(merge_branch_name))?;

    let merger = Merger::new(repo)?;
    if let Some(commit) = merger.merge_into_base(&merge_branch, &base_branch)? {
        println!(
            "Successfully merged `{}` into `{}`",
            merge_branch_name, base_branch.name
        );
        println!("HEAD -> {}", commit.id);
        Ok(Some(commit))
    } else {
        eprintln!("Automatic merge failed; fix conflicts and then commit the result.");
        Ok(None)
    }
}

#[cfg(test)]
mod tests {

    use crate::command;
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::opts::DFOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use std::path::Path;

    #[tokio::test]
    async fn test_command_merge_dataframe_conflict_both_added_rows_checkout_theirs(
    ) -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Add a more rows on this branch
            let branch_name = "ox-add-rows";
            repositories::branches::create_checkout(&repo, branch_name)?;

            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);
            let bbox_file =
                test::append_line_txt_file(bbox_file, "train/cat_3.jpg,cat,41.0,31.5,410,427")?;
            let their_branch_contents = util::fs::read_from_path(&bbox_file)?;
            let their_df = tabular::read_df(&bbox_file, DFOpts::empty())?;
            println!("their df {their_df}");

            repositories::add(&repo, &bbox_file)?;
            command::commit(&repo, "Adding new annotation as an Ox on a branch.")?;

            // Add a more rows on the main branch
            command::checkout(&repo, og_branch.name).await?;

            let bbox_file =
                test::append_line_txt_file(bbox_file, "train/dog_4.jpg,dog,52.0,62.5,256,429")?;

            repositories::add(&repo, &bbox_file)?;
            command::commit(&repo, "Adding new annotation on main branch")?;

            // Try to merge in the changes
            command::merge(&repo, branch_name)?;

            // We should have a conflict....
            let status = command::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            // Run command::checkout_theirs() and make sure their changes get kept
            command::checkout_theirs(&repo, &bbox_filename)?;
            let restored_df = tabular::read_df(&bbox_file, DFOpts::empty())?;
            println!("restored df {restored_df}");

            let file_contents = util::fs::read_from_path(&bbox_file)?;

            assert_eq!(file_contents, their_branch_contents);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_merge_dataframe_conflict_both_added_rows_combine_uniq(
    ) -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            // Add a more rows on this branch
            let branch_name = "ox-add-rows";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add in a line in this branch
            let row_from_branch = "train/cat_3.jpg,cat,41.0,31.5,410,427";
            let bbox_file = test::append_line_txt_file(bbox_file, row_from_branch)?;

            // Add the changes
            repositories::add(&repo, &bbox_file)?;
            command::commit(&repo, "Adding new annotation as an Ox on a branch.")?;

            // Add a more rows on the main branch
            command::checkout(&repo, og_branch.name).await?;

            let row_from_main = "train/dog_4.jpg,dog,52.0,62.5,256,429";
            let bbox_file = test::append_line_txt_file(bbox_file, row_from_main)?;

            repositories::add(&repo, &bbox_file)?;
            command::commit(&repo, "Adding new annotation on main branch")?;

            // Try to merge in the changes
            command::merge(&repo, branch_name)?;

            // We should have a conflict....
            let status = command::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            // Run command::checkout_theirs() and make sure their changes get kept
            command::checkout_combine(&repo, bbox_filename)?;
            let df = tabular::read_df(&bbox_file, DFOpts::empty())?;

            // This doesn't guarantee order, but let's make sure we have 7 annotations now
            assert_eq!(df.height(), 8);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_merge_dataframe_conflict_error_added_col() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            // Add a more columns on this branch
            let branch_name = "ox-add-column";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add in a column in this branch
            let mut opts = DFOpts::empty();
            opts.add_col = Some(String::from("random_col:unknown:str"));
            let mut df = tabular::read_df(&bbox_file, opts)?;
            println!("WRITE DF IN BRANCH {df:?}");
            tabular::write_df(&mut df, &bbox_file)?;

            // Add the changes
            repositories::add(&repo, &bbox_file)?;
            command::commit(&repo, "Adding new column as an Ox on a branch.")?;

            // Add a more rows on the main branch
            command::checkout(&repo, og_branch.name).await?;

            let row_from_main = "train/dog_4.jpg,dog,52.0,62.5,256,429";
            let bbox_file = test::append_line_txt_file(bbox_file, row_from_main)?;

            repositories::add(&repo, bbox_file)?;
            command::commit(&repo, "Adding new row on main branch")?;

            // Try to merge in the changes
            command::merge(&repo, branch_name)?;

            // We should have a conflict....
            let status = command::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            // Run command::checkout_theirs() and make sure we cannot
            let result = command::checkout_combine(&repo, bbox_filename);
            println!("{result:?}");
            assert!(result.is_err());

            Ok(())
        })
        .await
    }
}
