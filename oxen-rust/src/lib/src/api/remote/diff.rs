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

    use std::path::Path;
    use std::path::PathBuf;

    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::error::OxenError;
    use crate::model::diff::generic_diff_summary::GenericDiffSummary;
    use crate::model::EntryDataType;
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
                    assert_eq!(summary.tabular.num_added_rows, 1);
                    assert_eq!(summary.tabular.num_removed_rows, 0);
                    assert_eq!(summary.tabular.num_added_cols, 2);
                    assert_eq!(summary.tabular.num_removed_cols, 2);
                    assert!(summary.tabular.schema_has_changed);
                }
                _ => panic!("Wrong summary type"),
            }

            let test_csv = compare.entries.get(1).unwrap();
            assert_eq!(test_csv.filename, "train.csv");
            assert_eq!(test_csv.status, "modified");

            let summary = test_csv.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::TabularDiffSummary(summary) => {
                    assert_eq!(summary.tabular.num_added_rows, 0);
                    assert_eq!(summary.tabular.num_removed_rows, 0);
                    assert_eq!(summary.tabular.num_added_cols, 3);
                    assert_eq!(summary.tabular.num_removed_cols, 0);
                    assert!(summary.tabular.schema_has_changed);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_added_images_in_dir() -> Result<(), OxenError> {
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

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            command::push(&repo).await?;

            // Create branch
            let branch_name = "add-data";
            command::create_checkout(&repo, branch_name)?;

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

            // Push new branch real good
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            let compare = api::remote::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            // Added 4 dogs, one dir
            assert_eq!(compare.entries.len(), 5);

            let entry = compare.entries.get(0).unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 4);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_added_images_in_subdirs() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // Get the current branch
            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

            // create the images directory
            let images_dir = repo.path.join("images");
            let cats_dir = images_dir.join("cats");
            util::fs::create_dir_all(&cats_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = cats_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            command::add(&repo, &cats_dir)?;
            command::commit(&repo, "Adding initial cat images")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            command::push(&repo).await?;

            // Create branch
            let branch_name = "add-data";
            command::create_checkout(&repo, branch_name)?;

            // Add and commit the dogs
            let dogs_dir = images_dir.join("dogs");
            util::fs::create_dir_all(&dogs_dir)?;
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = dogs_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            command::add(&repo, &dogs_dir)?;
            command::commit(&repo, "Adding initial dog images")?;

            // Modify a cat
            let test_file = test::test_img_file_with_name("cat_1.jpg");
            let repo_filepath = cats_dir.join(test_file.file_name().unwrap());
            // Open the image file.
            let img = image::open(&repo_filepath).unwrap();

            // Resize the image to the specified dimensions.
            let dims = 96;
            let new_img = imageops::resize(&img, dims, dims, imageops::Nearest);

            // Save the resized image.
            new_img.save(repo_filepath).unwrap();

            // Remove a cat
            let test_file = test::test_img_file_with_name("cat_2.jpg");
            let repo_filepath = cats_dir.join(test_file.file_name().unwrap());
            util::fs::remove_file(&repo_filepath)?;

            command::add(&repo, &cats_dir)?;
            command::commit(&repo, "Add and modify some cats")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            let compare = api::remote::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("COMPARE: {:#?}", compare);

            // Added 4 dogs, three dirs
            assert_eq!(compare.entries.len(), 8);

            let entry = compare.entries.get(0).unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 1);
                    assert_eq!(summary.dir.file_counts.added, 4);
                    assert_eq!(summary.dir.file_counts.removed, 1);
                }
                _ => panic!("Wrong summary type"),
            }

            let entry = compare.entries.get(1).unwrap();
            assert_eq!(entry.filename, "images/cats");
            assert_eq!(entry.status, "added");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 1);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 1);
                }
                _ => panic!("Wrong summary type"),
            }

            let entry = compare.entries.get(2).unwrap();
            assert_eq!(entry.filename, "images/dogs");
            assert_eq!(entry.status, "added");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 4);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_removing_images_in_subdir() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // Get the current branch
            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

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

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            command::push(&repo).await?;

            // Create branch
            let branch_name = "remove-data";
            command::create_checkout(&repo, branch_name)?;

            // Remove all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                util::fs::remove_file(&repo_filepath)?;
            }

            let mut rm_opts = RmOpts::from_path(Path::new("images").join("cats"));
            rm_opts.recursive = true;
            command::rm(&repo, &rm_opts).await?;
            command::commit(&repo, "Removing cat images")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            let compare = api::remote::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("COMPARE: {:#?}", compare);

            // Removed 3 cats, two sub dirs
            assert_eq!(compare.entries.len(), 5);

            // images
            let entry = compare.entries.get(0).unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 3);
                }
                _ => panic!("Wrong summary type"),
            }

            // images/cats
            let entry = compare.entries.get(1).unwrap();
            assert_eq!(entry.filename, "images/cats");
            assert_eq!(entry.status, "removed");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 3);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_removing_images_by_rming_parent_in_subdir() -> Result<(), OxenError>
    {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // Get the current branch
            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

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

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            command::push(&repo).await?;

            // Create branch
            let branch_name = "remove-data";
            command::create_checkout(&repo, branch_name)?;

            // Remove all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                util::fs::remove_file(&repo_filepath)?;
            }

            // THIS IS THE CRUX of this test, do not remove images/cats, just remove images/
            let mut rm_opts = RmOpts::from_path(Path::new("images"));
            rm_opts.recursive = true;
            command::rm(&repo, &rm_opts).await?;
            command::commit(&repo, "Removing cat images")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            let compare = api::remote::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("COMPARE: {:#?}", compare);

            // Removed 3 cats, two parent dirs
            assert_eq!(compare.entries.len(), 5);

            let entry = compare.entries.get(0).unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "removed");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 3);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_entries_removing_images_in_dir() -> Result<(), OxenError> {
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

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            command::push(&repo).await?;

            // Create branch
            let branch_name = "remove-data";
            command::create_checkout(&repo, branch_name)?;

            // Remove all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                util::fs::remove_file(&repo_filepath)?;
            }

            let mut rm_opts = RmOpts::from_path("images");
            rm_opts.recursive = true;
            command::rm(&repo, &rm_opts).await?;
            command::commit(&repo, "Removing cat images")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            command::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            let compare = api::remote::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            // Removed 3 cats, one dir
            assert_eq!(compare.entries.len(), 4);

            let entry = compare.entries.get(0).unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "removed");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 3);
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
            let repo_filepath = PathBuf::from("images").join("dog_1.jpg");

            let rm_opts = RmOpts::from_path(repo_filepath);
            command::rm(&repo, &rm_opts).await?;
            command::commit(&repo, "Removing dog")?;

            // Add dwight howard and vince carter
            let test_file = test::test_img_file_with_name("dwight_vince.jpeg");
            let repo_filepath = images_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;
            command::add(&repo, &images_dir)?;
            command::commit(&repo, "Adding dwight and vince")?;

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

            // removed 1 dog, add 1 dog, modified 3 cats, modified 1 directory
            assert_eq!(compare.entries.len(), 6);

            let entry = compare.entries.get(0).unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 3);
                    assert_eq!(summary.dir.file_counts.added, 1);
                    assert_eq!(summary.dir.file_counts.removed, 1);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }
}
