//! # oxen diff
//!
//! Compare two files to find changes between them.
//!
//! ## Usage
//!
//! ```shell
//! oxen diff <file_1> <file_2> [options]
//! ```

use std::path::{Path, PathBuf};

use crate::core::index::MergeConflictReader;
use crate::error::OxenError;
use crate::model::diff::DiffResult;
use crate::model::entry::commit_entry::CommitPath;
use crate::model::LocalRepository;
use crate::{api, util};

pub fn diff(
    path_1: impl AsRef<Path>,
    path_2: Option<PathBuf>,
    keys: Vec<String>,
    targets: Vec<String>,
    repo_dir: Option<PathBuf>,
    revision_1: Option<String>,
    revision_2: Option<String>,
) -> Result<DiffResult, OxenError> {
    log::debug!(
        "diff called with keys: {:?} and targets: {:?}",
        keys,
        targets,
    );

    // If the user specifies two files without revisions, we will compare the files on disk
    if revision_1.is_none() && revision_2.is_none() && path_2.is_some() {
        // If we do not have revisions set, just compare the files on disk
        let result = api::local::diff::diff_files(path_1, path_2.unwrap(), keys, targets, vec![])?;

        return Ok(result);
    }

    // Make sure we have a repository to look up the revisions
    let Some(repo_dir) = repo_dir else {
        return Err(OxenError::basic_str(
            "Specifying a revision requires a repository",
        ));
    };

    let repository = LocalRepository::new(repo_dir.as_ref())?;

    // TODONOW: might be able to clean this logic up - pull out into function so we can early return and be less confusing
    let (cpath_1, cpath_2) = if let Some(path_2) = path_2 {
        let cpath_1 = if let Some(revison) = revision_1 {
            let commit_1 = api::local::revisions::get(&repository, revison)?;
            CommitPath {
                commit: commit_1,
                path: path_1.as_ref().to_path_buf(),
            }
        } else {
            CommitPath {
                commit: None,
                path: path_1.as_ref().to_path_buf(),
            }
        };

        let cpath_2 = if let Some(revison) = revision_2 {
            let commit = api::local::revisions::get(&repository, revison)?;

            CommitPath {
                commit,
                path: path_2.clone(),
            }
        } else {
            CommitPath {
                commit: None,
                path: path_2.clone(),
            }
        };

        (cpath_1, cpath_2)
    } else {
        // If no file2, compare with file1 at head.
        let commit = Some(api::local::commits::head_commit(&repository)?);

        (
            CommitPath {
                commit,
                path: path_1.as_ref().to_path_buf(),
            },
            CommitPath {
                commit: None,
                path: path_1.as_ref().to_path_buf(),
            },
        )
    };

    let result = diff_commits(&repository, cpath_1, cpath_2, keys, targets, vec![])?;

    Ok(result)
}

pub fn diff_commits(
    repo: &LocalRepository,
    cpath_1: CommitPath,
    cpath_2: CommitPath,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<DiffResult, OxenError> {
    log::debug!(
        "Compare command called with: {:?} and {:?}",
        cpath_1,
        cpath_2
    );

    // TODONOW - anything we can clean up with this mut initialization?
    let mut path_1 = cpath_1.path.clone();
    let mut path_2 = cpath_2.path.clone();

    if let Some(commit_1) = cpath_1.commit {
        let entry_1 = api::local::entries::get_commit_entry(repo, &commit_1, &cpath_1.path)?
            .ok_or_else(|| {
                OxenError::ResourceNotFound(
                    format!("{}@{}", cpath_1.path.display(), commit_1.id).into(),
                )
            })?;

        path_1 = util::fs::version_path(repo, &entry_1);
    };

    if let Some(mut commit_2) = cpath_2.commit {
        // if there are merge conflicts, compare against the conflict commit instead
        let merger = MergeConflictReader::new(repo)?;

        if merger.has_conflicts()? {
            commit_2 = merger.get_conflict_commit()?.unwrap();
        }

        let entry_2 = api::local::entries::get_commit_entry(repo, &commit_2, &cpath_2.path)?
            .ok_or_else(|| {
                OxenError::ResourceNotFound(
                    format!("{}@{}", cpath_2.path.display(), commit_2.id).into(),
                )
            })?;

        path_2 = util::fs::version_path(repo, &entry_2);
    };

    let compare_result = api::local::diff::diff_files(path_1, path_2, keys, targets, display)?;

    log::debug!("compare result: {:?}", compare_result);

    Ok(compare_result)
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use polars::lazy::dsl::{col, lit};
    use polars::lazy::frame::IntoLazy;

    use crate::command;
    use crate::error::OxenError;
    use crate::model::diff::{ChangeType, DiffResult};
    use crate::model::entry::commit_entry::CommitPath;
    use crate::test;
    use crate::util;

    #[test]
    fn test_command_diff_txt_files() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let file1 = dir.join("file1.txt");
            let file2 = dir.join("file2.txt");

            util::fs::write_to_path(&file1, "hello\nhi\nhow are you?")?;
            util::fs::write_to_path(&file2, "hello\nhi\nhow are you doing?")?;

            let diff = command::diff(&file1, Some(file2), vec![], vec![], None, None, None)?;

            match diff {
                DiffResult::Text(result) => {
                    let lines = result.lines;

                    for line in &lines {
                        println!("{:?}", line);
                    }

                    assert_eq!(lines.len(), 4);

                    // should be 2 unchanged
                    assert_eq!(lines[0].modification, ChangeType::Unchanged);
                    assert_eq!(&lines[0].text, "hello");
                    assert_eq!(lines[1].modification, ChangeType::Unchanged);
                    assert_eq!(&lines[1].text, "hi");
                    // 1 removed
                    assert_eq!(lines[2].modification, ChangeType::Removed);
                    assert_eq!(&lines[2].text, "how are you?");
                    // 1 added
                    assert_eq!(lines[3].modification, ChangeType::Added);
                    assert_eq!(&lines[3].text, "how are you doing?");
                }
                _ => panic!("expected text result"),
            }

            Ok(())
        })
    }

    // #[tokio::test]
    // async fn test_compare_same_dataframe_no_keys_no_targets() -> Result<(), OxenError> {
    //     test::run_empty_local_repo_test_async(|repo| async move {
    //         let csv1 = "a,b,c\n1,2,3\n4,5,6\n";
    //         let csv2 = "a,b,c\n1,2,3\n4,5,6\n";

    //         let path_1 = PathBuf::from("file1.csv");
    //         let path_2 = PathBuf::from("file2.csv");

    //         // Write to file
    //         tokio::fs::write(repo.path.join(&path_1), csv1).await?;
    //         tokio::fs::write(repo.path.join(&path_2), csv2).await?;

    //         command::add(&repo, repo.path.clone())?;

    //         let commit = command::commit(&repo, "two files")?;

    //         let c1 = CommitPath {
    //             commit: Some(commit.clone()),
    //             path: path_1.clone(),
    //         };

    //         let c2 = CommitPath {
    //             commit: Some(commit.clone()),
    //             path: path_2.clone(),
    //         };

    //         let compare_result = command::diff_commits(&repo, c1, c2, vec![], vec![], vec![])?;

    //         match compare_result {
    //             DiffResult::Tabular(result) => {
    //                 let df = result.contents;
    //                 assert_eq!(df.height(), 0);
    //             }
    //             _ => panic!("expected tabular result"),
    //         }

    //         Ok(())
    //     })
    //     .await
    // }

    #[tokio::test]
    async fn test_compare_one_added_one_removed_no_keys_no_targets() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let csv1 = "a,b,c\n1,2,3\n4,5,6\n";
            let csv2 = "a,b,c\n1,2,3\n4,5,2\n";

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            command::add(&repo, repo.path.clone())?;

            let commit = command::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result = command::diff_commits(&repo, c1, c2, vec![], vec![], vec![])?;

            let diff_col = ".oxen.diff.status";
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 2);
                    assert_eq!(df.width(), 4); // 3 (inferred) key columns + diff status
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    assert_eq!(added_df.height(), 1);
                    assert_eq!(removed_df.height(), 1);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }
    #[tokio::test]
    async fn test_compare_all_types_with_keys_and_targets() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Keying on "a" and "b" with target "c"
            // Removed: -> 5,6 and 7,8
            // Added: 9,10
            // Modified: 3,4 (1 -> 1234)
            // Unchanged: 1,2 (not included)

            let csv1 = "a,b,c\n1,2,1\n3,4,1\n5,6,1\n7,8,1";
            let csv2 = "a,b,c\n1,2,1\n3,4,1234\n9,10,1";

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            command::add(&repo, repo.path.clone())?;

            let commit = command::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result = command::diff_commits(
                &repo,
                c1,
                c2,
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string()],
                vec![],
            )?;

            let diff_col = ".oxen.diff.status";
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 4);
                    assert_eq!(df.width(), 5); // 2 key columns, 1 target column * 2 views each, and diff status
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    let modified_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("modified")))
                        .collect()?;
                    assert_eq!(added_df.height(), 1);
                    assert_eq!(removed_df.height(), 2);
                    assert_eq!(modified_df.height(), 1);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_all_types_with_keys_and_same_targets() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // If all the targets are 1 - aka, using targets but none are unchanged, we want to make sure that
            // the column namings of .left and .right are consistently handled

            let csv1 = "a,b,c\n1,2,1\n3,4,1\n5,6,1\n7,8,1";
            let csv2 = "a,b,c\n1,2,1\n3,4,1\n9,10,1";

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            command::add(&repo, repo.path.clone())?;

            let commit = command::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result = command::diff_commits(
                &repo,
                c1,
                c2,
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string()],
                vec![],
            )?;

            let diff_col = ".oxen.diff.status";
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 3);
                    assert_eq!(df.width(), 5); // 2 key columns, 1 target column * 2 views each, and diff status
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    let modified_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("modified")))
                        .collect()?;
                    assert_eq!(added_df.height(), 1);
                    assert_eq!(removed_df.height(), 2);
                    assert_eq!(modified_df.height(), 0);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_same_files_with_targets() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n";
            let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n";

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            command::add(&repo, repo.path.clone())?;

            let commit = command::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result = command::diff_commits(
                &repo,
                c1,
                c2,
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string(), "d".to_string()],
                vec![],
            )?;

            // Should return empty df
            let diff_col = ".oxen.diff.status";
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 0);
                    assert_eq!(df.width(), 7); // 2 key columns, 2 targets * 2(right+left) + diff status
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    let modified_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("modified")))
                        .collect()?;
                    assert_eq!(added_df.height(), 0);
                    assert_eq!(removed_df.height(), 0);
                    assert_eq!(modified_df.height(), 0);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn compare_no_keys_no_targets_added_column() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n8,7,6,5";
            let csv2 = "a,b,c,d,e\n1,2,3,4,5\n4,5,6,7,8\n9,8,7,6,5";
            // 2 modified (added row) 1 added 1 removed

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            command::add(&repo, repo.path.clone())?;

            let commit = command::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result = command::diff_commits(&repo, c1, c2, vec![], vec![], vec![])?;

            // Should return empty df
            let diff_col = ".oxen.diff.status";
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 4);
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    let modified_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("modified")))
                        .collect()?;
                    assert_eq!(added_df.height(), 1);
                    assert_eq!(removed_df.height(), 1);
                    assert_eq!(modified_df.height(), 2);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_keys_no_targets_implies_modified() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // We'll key on a, b, and c.
            // D will then be an implicit target bc it's a shared column
            // (1 added, 1 removed, 1 modified)
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n0,0,0,0\n";
            let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,8\n1,1,1,1\n";

            let path_1 = PathBuf::from("file1.csv");
            let path_2 = PathBuf::from("file2.csv");

            // Write to file
            tokio::fs::write(repo.path.join(&path_1), csv1).await?;
            tokio::fs::write(repo.path.join(&path_2), csv2).await?;

            command::add(&repo, repo.path.clone())?;

            let commit = command::commit(&repo, "two files")?;

            let c1 = CommitPath {
                commit: Some(commit.clone()),
                path: path_1.clone(),
            };

            let c2 = CommitPath {
                commit: Some(commit.clone()),
                path: path_2.clone(),
            };

            let compare_result = command::diff_commits(
                &repo,
                c1,
                c2,
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                vec![],
                vec![],
            )?;

            // Should return empty df
            let diff_col = ".oxen.diff.status";
            match compare_result {
                DiffResult::Tabular(result) => {
                    let df = result.contents;
                    assert_eq!(df.height(), 3);
                    let added_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("added")))
                        .collect()?;
                    let removed_df = df
                        .clone()
                        .lazy()
                        .filter(col(diff_col).eq(lit("removed")))
                        .collect()?;
                    let modified_df = df
                        .lazy()
                        .filter(col(diff_col).eq(lit("modified")))
                        .collect()?;
                    assert_eq!(added_df.height(), 1);
                    assert_eq!(removed_df.height(), 1);
                    assert_eq!(modified_df.height(), 1);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }
}
