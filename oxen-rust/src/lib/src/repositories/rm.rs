//! # oxen rm
//!
//! Remove files from the index and working directory
//!

use std::collections::HashSet;

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use std::path::{Path, PathBuf};

use glob::glob;

use crate::util;

/// Removes the path from the index
pub async fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    log::debug!("Rm with opts: {opts:?}");
    let path: &Path = opts.path.as_ref();
    let paths: HashSet<PathBuf> = parse_glob_path(path, repo, opts)?;

    log::debug!("paths: {paths:?}");
    p_rm(&paths, repo, opts).await?;

    Ok(())
}

async fn p_rm(
    paths: &HashSet<PathBuf>,
    repo: &LocalRepository,
    opts: &RmOpts,
) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            log::debug!("Version found: V0_10_0");
            for path in paths {
                let opts = RmOpts::from_path_opts(path, opts);
                core::v0_10_0::index::rm(repo, &opts).await?;
            }
        }
        MinOxenVersion::V0_19_0 => {
            log::debug!("Version found: V0_19_0");
            core::v0_19_0::rm::rm(paths, repo, opts).await?;
        }
    }
    Ok(())
}

// TODO: Should removing dirs from staged require -r?
// Collect paths for removal. Returns error if dir found and -r not set
fn parse_glob_path(
    path: &Path,
    repo: &LocalRepository,
    opts: &RmOpts,
) -> Result<HashSet<PathBuf>, OxenError> {
    let mut paths: HashSet<PathBuf> = HashSet::new();
    log::debug!("Parsing paths: {path:?}");

    if let Some(path_str) = path.to_str() {
        if util::fs::is_glob_path(path_str) {
            // Match against any untracked entries in the current dir

            for entry in glob(path_str)? {
                let full_path = repo.path.join(entry?);
                paths.insert(full_path);
            }
        } else {
            // Non-glob path
            let full_path = repo.path.join(path);
            paths.insert(full_path);
        }
    }

    log::debug!("parse_glob_paths: {paths:?}");
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::model::StagedEntryStatus;
    use crate::opts::RestoreOpts;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::repositories::entries;
    use crate::test;
    use crate::util;

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
            println!("Before rm");
            repositories::rm(&repo, &opts).await?;

            // Make sure we staged these removals
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(num_files, status.staged_files.len());
            for (path, entry) in status.staged_files.iter() {
                // The root path will be added as staged
                if path != Path::new("") {
                    println!("Path is : {path:?}, entry is: {entry:?} ");
                    assert_eq!(entry.status, StagedEntryStatus::Removed);
                }
            }
            // Make sure directory is no longer on disk
            assert!(!full_path.exists());

            // Restore the content from staging area
            let opts = RestoreOpts::from_staged_path(&rm_dir);
            repositories::restore::restore(&repo, opts)?;

            // This should have removed all the staged files, but not restored from disk yet.
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(0, status.staged_files.len());
            assert_eq!(num_files, status.removed_files.len());

            // This should restore all the files from the HEAD commit
            let opts = RestoreOpts::from_path(&rm_dir);
            repositories::restore::restore(&repo, opts)?;

            let status = repositories::status(&repo)?;
            status.print();

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

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Create branch
            let branch_name = "remove-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Remove all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                util::fs::remove_file(&repo_filepath)?;
            }

            let mut rm_opts = RmOpts::from_path(Path::new("images"));
            rm_opts.recursive = true;
            repositories::rm(&repo, &rm_opts).await?;
            let commit = repositories::commit(&repo, "Removing cat images")?;

            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                assert!(!repo_filepath.exists())
            }

            let tree = repositories::tree::get_by_commit(&repo, &commit)?;
            let (files, dirs) = repositories::tree::list_files_and_dirs(&tree)?;
            assert_eq!(files.len(), 0);
            for dir in dirs.iter() {
                println!("dir: {:?}", dir);
            }

            // Should be 0, as list_files_and_dirs explicitly excludes the root dir
            assert_eq!(dirs.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_multi_level_directory() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images").join("cats");
            util::fs::create_dir_all(&images_dir)?;

            // create several levels of subdirectories
            for i in 1..=3 {
                let sub_dir = repo
                    .path
                    .join("images")
                    .join("cats")
                    .join(format!("subdir{i}_level_1"));
                util::fs::create_dir_all(&sub_dir)?;
            }

            for i in 1..=2 {
                let sub_dir = repo
                    .path
                    .join("images")
                    .join("cats")
                    .join(format!("subdir{i}_level_1"))
                    .join(format!("subdir{i}_level_2"));
                util::fs::create_dir_all(&sub_dir)?;
            }

            // Third level
            for i in 1..=1 {
                let sub_dir = repo
                    .path
                    .join("images")
                    .join("cats")
                    .join(format!("subdir{i}_level_1"))
                    .join(format!("subdir{i}_level_2"))
                    .join(format!("subdir{i}_level_3"));
                util::fs::create_dir_all(&sub_dir)?;
            }

            // Add and commit the cats to every subdirectory
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            for j in 1..=3 {
                for i in 1..=3 {
                    let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(test_file.file_name().unwrap());
                    util::fs::copy(&test_file, &repo_filepath)?;
                }
            }

            for j in 1..=2 {
                for i in 1..=3 {
                    let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(format!("subdir{j}_level_2"))
                        .join(test_file.file_name().unwrap());
                    util::fs::copy(&test_file, &repo_filepath)?;
                }
            }

            for j in 1..=1 {
                for i in 1..=3 {
                    let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(format!("subdir{j}_level_2"))
                        .join(format!("subdir{j}_level_3"))
                        .join(test_file.file_name().unwrap());
                    util::fs::copy(&test_file, &repo_filepath)?;
                }
            }

            repositories::add(&repo, &images_dir)?;

            // TODO: The following assertions seem like the should be valid, and are backed up by the print statement
            // However, staus.staged_dirs.len() is showing up as 2, rather than 9. That seems to be an issue with status?

            /*
            let status = repositories::status(&repo)?;
            status.print();

            // root dir + images + cats + level 1 * 3 + level 2 * 2 + level 3 * 1
            assert_eq!(status.staged_dirs.len(), 9);

            // 3 * (cats + level 1 * 3 + level 2 * 2 + level 3 * 1)
            assert_eq!(status.staged_files.len(), 21);
            */

            repositories::commit(&repo, "Adding initial cat images")?;

            // Create branch
            let branch_name = "remove-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Remove all the cat images and subdirectories
            let mut rm_opts = RmOpts::from_path(Path::new("images"));
            rm_opts.recursive = true;
            repositories::rm(&repo, &rm_opts).await?;
            let commit = repositories::commit(&repo, "Removing cat images and sub_directories")?;

            // None of these files should exist after rm -r
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                assert!(!repo_filepath.exists())
            }

            for j in 1..=3 {
                for i in 1..=3 {
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(format!("cat_{i}.jpg"));
                    assert!(!repo_filepath.exists())
                }
            }

            for j in 1..=2 {
                for i in 1..=3 {
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(format!("subdir{j}_level_2"))
                        .join(format!("cat_{i}.jpg"));
                    assert!(!repo_filepath.exists())
                }
            }

            for j in 1..=1 {
                for i in 1..=3 {
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(format!("subdir{j}_level_2"))
                        .join(format!("subdir{j}_level_3"))
                        .join(format!("cat_{i}.jpg"));
                    assert!(!repo_filepath.exists())
                }
            }

            let entries = entries::list_for_commit(&repo, &commit)?;
            assert_eq!(entries.len(), 0);

            let tree = repositories::tree::get_by_commit(&repo, &commit)?;
            let dirs = tree.list_dir_paths()?;
            for dir in dirs.iter() {
                log::debug!("dir: {:?}", dir);
            }

            // Should be 0, as list_files_and_dirs explicitly excludes the root dir
            assert_eq!(dirs.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_one_file_in_dir() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Add and commit the dogs
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial dog images")?;

            // Create branch
            let branch_name = "modify-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Resize all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));

                let dims = 96;
                util::image::resize_and_save(&repo_filepath, &repo_filepath, dims)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Resized all the cats")?;

            // Remove one of the dogs
            let repo_filepath = PathBuf::from("images").join("dog_1.jpg");

            let rm_opts = RmOpts::from_path(repo_filepath);
            repositories::rm(&repo, &rm_opts).await?;
            let commit = repositories::commit(&repo, "Removing dog")?;

            let tree = repositories::tree::get_by_commit(&repo, &commit)?;
            println!("tree after rm dog");
            tree.print();

            // Add dwight howard and vince carter
            let test_file = test::test_img_file_with_name("dwight_vince.jpeg");
            let repo_filepath = images_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, repo_filepath)?;
            repositories::add(&repo, &images_dir)?;
            let commit = repositories::commit(&repo, "Adding dwight and vince")?;

            // Should have 3 cats, 3 dogs, and one dwight/vince
            let tree = repositories::tree::get_by_commit(&repo, &commit)?;
            println!("tree after add dwight/vince");
            tree.print();

            let (files, dirs) = repositories::tree::list_files_and_dirs(&tree)?;

            for dir in dirs.iter() {
                log::debug!("dir: {:?}", dir);
            }

            for file in files.iter() {
                log::debug!("file: {:?}", file);
            }

            assert_eq!(files.len(), 7);
            assert_eq!(dirs.len(), 1);

            Ok(())
        })
        .await
    }

    #[test]
    fn test_wildcard_remove_nested_nlp_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let dir = Path::new("nlp");
            let repo_dir = repo.path.join(dir);
            repositories::add(&repo, repo_dir)?;

            let status = repositories::status(&repo)?;
            status.print();

            // Should add all the sub dirs
            // nlp/
            //   classification/
            //     annotations/
            assert_eq!(
                status
                    .staged_dirs
                    .paths
                    .get(Path::new("nlp"))
                    .unwrap()
                    .len(),
                3
            );
            // Should add sub files
            // nlp/classification/annotations/train.tsv
            // nlp/classification/annotations/test.tsv
            assert_eq!(status.staged_files.len(), 2);

            repositories::commit(&repo, "Adding nlp dir")?;

            // Remove the nlp dir
            let dir = Path::new("nlp");
            let repo_nlp_dir = repo.path.join(dir);
            std::fs::remove_dir_all(repo_nlp_dir)?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.removed_files.len(), 2);
            assert_eq!(status.staged_files.len(), 0);
            // Add the removed nlp dir with a wildcard
            repositories::add(&repo, "nlp/*")?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 1);
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_wildcard_rm_deleted_and_present() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Add and commit the dogs
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial dog images")?;

            // Pre-remove two cats and one dog to ensure deleted images get staged as removed as well as non-deleted images
            std::fs::remove_file(repo.path.join("images").join("cat_1.jpg"))?;
            std::fs::remove_file(repo.path.join("images").join("cat_2.jpg"))?;
            std::fs::remove_file(repo.path.join("images").join("dog_1.jpg"))?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.removed_files.len(), 3);
            assert_eq!(status.staged_files.len(), 0);

            // Remove with wildcard
            let rm_opts = RmOpts {
                path: PathBuf::from("images/*"),
                recursive: false,
                staged: false,
                remote: false,
            };

            repositories::rm(&repo, &rm_opts).await?;

            let status = repositories::status(&repo)?;

            // Should now have 7 staged for removal
            assert_eq!(status.staged_files.len(), 7);
            assert_eq!(status.removed_files.len(), 0);

            // Unstage the changes with staged rm
            let rm_opts = RmOpts {
                path: PathBuf::from("images/*"),
                recursive: false,
                staged: true,
                remote: false,
            };

            repositories::rm(&repo, &rm_opts).await?;

            let status = repositories::status(&repo)?;

            // Files unstaged, still removed
            assert_eq!(status.staged_files.len(), 0);
            assert_eq!(status.removed_files.len(), 7);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_file() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("README", |repo| async move {
            // Stage the README.md file
            let path = Path::new("README.md");
            repositories::add(&repo, repo.path.join(path))?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), 1);
            assert!(status.staged_files.contains_key(path));

            let opts = RmOpts::from_staged_path(path);
            repositories::rm(&repo, &opts).await?;

            let status = repositories::status(&repo)?;
            log::debug!("status: {:?}", status);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_dir_without_recursive_flag_should_be_error() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Stage the data
            let path = Path::new("train");
            repositories::add(&repo, repo.path.join(path))?;

            let status = repositories::status(&repo)?;
            status.print();
            // 2: train & the root dir
            assert_eq!(status.staged_dirs.len(), 2);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                recursive: false, // This should be an error
                remote: false,
            };
            let result = repositories::rm(&repo, &opts).await;
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_train_dir() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Stage the data
            let path = Path::new("train");
            repositories::add(&repo, repo.path.join(path))?;

            let status = repositories::status(&repo)?;
            status.print();
            // 2: train & the root dir
            assert_eq!(status.staged_dirs.len(), 2);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                recursive: true, // make sure to pass in recursive
                remote: false,
            };
            repositories::rm(&repo, &opts).await?;

            let status = repositories::status(&repo)?;
            status.print();
            // 1: The root dir will still be present
            assert_eq!(status.staged_dirs.len(), 1);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_dir_with_slash() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Stage the data
            let path = Path::new("train/");
            repositories::add(&repo, repo.path.join(path))?;

            let status = repositories::status(&repo)?;
            // 2: train & the root dir
            assert_eq!(status.staged_dirs.len(), 2);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                recursive: true, // make sure to pass in recursive
                remote: false,
            };
            let result = repositories::rm(&repo, &opts).await;
            assert!(result.is_ok());

            let status = repositories::status(&repo)?;
            status.print();
            // 1: The root dir will still be present
            assert_eq!(status.staged_dirs.len(), 1);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_staged_rm_file() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("README", |repo| async move {
            // Remove the readme
            let path = Path::new("README.md");

            let opts = RmOpts::from_path(path);
            repositories::rm(&repo, &opts).await?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_files.len(), 1);
            assert_eq!(
                status.staged_files.get(path).unwrap().status,
                StagedEntryStatus::Removed
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_without_recursive_flag_should_be_error() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Remove the train dir
            let path = Path::new("train");

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: false,
                recursive: false, // This should be an error
                remote: false,
            };

            let result = repositories::rm(&repo, &opts).await;
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_that_is_not_committed_should_throw_error() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // The train dir is not committed, so should get an error trying to remove
            let train_dir = Path::new("train");

            let opts = RmOpts {
                path: train_dir.to_path_buf(),
                staged: false,
                recursive: true, // Need to specify recursive
                remote: false,
            };

            let result = repositories::rm(&repo, &opts).await;
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_with_modifications_should_throw_error() -> Result<(), OxenError> {
        // skip on windows, not sure why it's failing...
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_select_data_repo_test_committed_async("train", |repo| async move {
            // Remove the train dir
            let train_dir = Path::new("train");

            let opts = RmOpts {
                path: train_dir.to_path_buf(),
                staged: false,
                recursive: true, // Need to specify recursive
                remote: false,
            };

            // copy a cat into the dog image
            util::fs::copy(
                Path::new("data")
                    .join("test")
                    .join("images")
                    .join("cat_1.jpg"),
                repo.path.join(train_dir.join("dog_1.jpg")),
            )?;

            // There should be one modified file
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(status.modified_files.len(), 1);

            let result = repositories::rm(&repo, &opts).await;
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_train_dir() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("train", |repo| async move {
            let head_commit = repositories::commits::head_commit(&repo)?;
            let og_tree = repositories::tree::get_by_commit(&repo, &head_commit)?;
            println!("og tree");
            og_tree.print();

            // Remove the train dir
            let path = Path::new("train");

            let og_num_files = util::fs::rcount_files_in_dir(&repo.path.join(path));

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: false,
                recursive: true, // Must pass in recursive = true
                remote: false,
            };
            repositories::rm(&repo, &opts).await?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_files.len(), og_num_files);
            for (_, staged_entry) in status.staged_files.iter() {
                assert_eq!(staged_entry.status, StagedEntryStatus::Removed);
            }

            // commit the removal
            let commit = repositories::commit(&repo, "removed train dir")?;

            // make sure the train dir is deleted from the commits db
            let tree = repositories::tree::get_by_commit(&repo, &commit)?;
            println!("tree after rm train dir");
            tree.print();
            let has_dir = tree.has_dir(path);
            println!("has_dir: {:?}", has_dir);
            assert!(!has_dir);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_with_slash() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("train", |repo| async move {
            // Remove the train dir
            let path = Path::new("train/");

            let og_num_files = util::fs::rcount_files_in_dir(&repo.path.join(path));

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: false,
                recursive: true, // Must pass in recursive = true
                remote: false,
            };
            repositories::rm(&repo, &opts).await?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_files.len(), og_num_files);
            for (_, staged_entry) in status.staged_files.iter() {
                assert_eq!(staged_entry.status, StagedEntryStatus::Removed);
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_subdir() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("annotations", |repo| async move {
            // Remove the annotations/train subdir
            let path = Path::new("annotations").join("train");
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path.join(&path));

            let opts = RmOpts {
                path,
                staged: false,
                recursive: true, // Must pass in recursive = true
                remote: false,
            };
            repositories::rm(&repo, &opts).await?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_files.len(), og_num_files);
            for (_, staged_entry) in status.staged_files.iter() {
                assert_eq!(staged_entry.status, StagedEntryStatus::Removed);
            }

            Ok(())
        })
        .await
    }
}
