use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::compare::CompareEntries;
use crate::view::CompareEntriesResponse;

pub async fn list_diff_entries(
    remote_repo: &RemoteRepository,
    base: impl AsRef<str>,
    head: impl AsRef<str>,
    page: usize,
    page_size: usize,
) -> Result<CompareEntries, OxenError> {
    let base = base.as_ref();
    let head = head.as_ref();
    let uri = format!("/compare/entries/{base}..{head}?page={page}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("list_page got body: {}", body);
            let response: Result<CompareEntriesResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.compare),
                Err(err) => Err(OxenError::basic_str(format!(
                    "api::dir::list_dir error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("api::dir::list_dir Err {err:?} request failed: {url}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::error::OxenError;
    use crate::model::EntryDataType;
    use crate::model::diff::generic_diff_summary::GenericDiffSummary;
    use crate::opts::RmOpts;
    use crate::test;
    use crate::util;
    use image::imageops;

    #[tokio::test]
    async fn test_diff_entries_cifar_csvs() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // Get the current branch
            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

            // Track test.csv file
            let test_file = test::test_csv_file_with_name("test_cifar_2x9999.csv");
            let repo_filename = "test.csv";
            let repo_filepath = repo.path.join(repo_filename);
            util::fs::copy(&test_file, &repo_filepath)?;

            command::add(&repo, &repo_filepath)?;
            command::commit(&repo, "Adding test csv with two columns and 9999 rows")?;

            // Track train.csv file
            let test_file = test::test_csv_file_with_name("train_cifar_2x50000.csv");
            let repo_filename = "train.csv";
            let repo_filepath = repo.path.join(repo_filename);
            util::fs::copy(&test_file, &repo_filepath)?;

            command::add(&repo, &repo_filepath)?;
            command::commit(&repo, "Adding train csv with two columns and 50k rows")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            command::push(&repo).await?;

            // Create branch
            let branch_name = "modify-data";
            command::create_checkout(&repo, branch_name)?;

            // Modify test.csv file
            let test_file = test::test_csv_file_with_name("test_cifar_2x10000.csv");
            let repo_filename = "test.csv";
            let repo_filepath = repo.path.join(repo_filename);
            util::fs::copy(&test_file, &repo_filepath)?;

            command::add(&repo, &repo_filepath)?;
            command::commit(&repo, "Adding title row to csv")?;

            // Track train.csv file
            let test_file = test::test_csv_file_with_name("train_cifar_5x50000.csv");
            let repo_filename = "train.csv";
            let repo_filepath = repo.path.join(repo_filename);
            util::fs::copy(&test_file, &repo_filepath)?;

            command::add(&repo, &repo_filepath)?;
            command::commit(&repo, "Adding columns to train.csv")?;

            // Push it real good
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            let compare = api::remote::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            assert_eq!(compare.entries.len(), 2);

            let test_csv = compare.entries.get(0).unwrap();
            assert_eq!(test_csv.filename, "test.csv");
            assert_eq!(test_csv.status, "modified");

            let summary = test_csv.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::TabularDiffSummary(summary) => {
                    assert_eq!(summary.num_added_rows, 1);
                    assert_eq!(summary.num_removed_rows, 0);
                    assert_eq!(summary.num_added_cols, 2);
                    assert_eq!(summary.num_removed_cols, 2);
                    assert!(summary.schema_has_changed);
                }
                _ => panic!("Wrong summary type"),
            }

            let test_csv = compare.entries.get(1).unwrap();
            assert_eq!(test_csv.filename, "train.csv");
            assert_eq!(test_csv.status, "modified");

            let summary = test_csv.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::TabularDiffSummary(summary) => {
                    assert_eq!(summary.num_added_rows, 0);
                    assert_eq!(summary.num_removed_rows, 0);
                    assert_eq!(summary.num_added_cols, 3);
                    assert_eq!(summary.num_removed_cols, 0);
                    assert!(summary.schema_has_changed);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_changed_images_in_dir() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // Get the current branch
            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            command::add(&repo, &images_dir)?;
            command::commit(&repo, "Adding initial cat images")?;

            // Add and commit the dogs
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            command::add(&repo, &images_dir)?;
            command::commit(&repo, "Adding initial dog images")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            command::push(&repo).await?;

            // Create branch
            let branch_name = "modify-data";
            command::create_checkout(&repo, branch_name)?;

            // Resize all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));

                // Open the image file.
                let img = image::open(&repo_filepath).unwrap();

                // Resize the image to the specified dimensions.
                let dims = 96;
                let new_img = imageops::resize(&img, dims, dims, imageops::Nearest);

                // Save the resized image.
                new_img.save(repo_filepath).unwrap();
            }

            command::add(&repo, &images_dir)?;
            command::commit(&repo, "Resized all the cats")?;

            // Remove one of the dogs
            let repo_filepath = PathBuf::from("images").join(&format!("dog_1.jpg"));
            // util::fs::remove_file(&repo_filepath)?;

            let rm_opts = RmOpts::from_path(repo_filepath);
            command::rm(&repo, &rm_opts).await?;
            command::commit(&repo, "Removing dog")?;

            // Push it real good
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            let compare = api::remote::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("{:#?}", compare);

            // Removed 1 dog, modified 3 cats, modified 1 directory
            assert_eq!(compare.entries.len(), 5);

            let entry = compare.entries.get(0).unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.file_counts.modified, 3);
                    assert_eq!(summary.file_counts.added, 0);
                    assert_eq!(summary.file_counts.removed, 1);
                }
                _ => panic!("Wrong summary type"),
            }

            // let test_csv = compare.entries.get(1).unwrap();
            // assert_eq!(test_csv.filename, "train.csv");
            // assert_eq!(test_csv.status, "modified");

            // let summary = test_csv.diff_summary.as_ref().unwrap();
            // match summary {
            //     GenericDiffSummary::TabularDiffSummary(summary) => {
            //         assert_eq!(summary.num_added_rows, 0);
            //         assert_eq!(summary.num_removed_rows, 0);
            //         assert_eq!(summary.num_added_cols, 3);
            //         assert_eq!(summary.num_removed_cols, 0);
            //         assert!(summary.schema_has_changed);
            //     }
            //     _ => panic!("Wrong summary type"),
            // }

            Ok(())
        })
        .await
    }
}
