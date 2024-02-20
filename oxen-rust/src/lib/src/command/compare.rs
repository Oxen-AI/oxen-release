use crate::api;
use crate::core::index::MergeConflictReader;
use crate::error::OxenError;
use crate::model::entry::commit_entry::{CommitPath, CompareEntry};
use crate::model::LocalRepository;
use crate::view::compare::CompareResult;

pub fn compare(
    repo: &LocalRepository,
    cpath_1: CommitPath,
    cpath_2: CommitPath,
    keys: Vec<String>,
    targets: Vec<String>,
    display: Vec<String>,
) -> Result<CompareResult, OxenError> {
    // TODONOW - anything we can clean up with this mut initialization?
    let mut compare_entry_1 = CompareEntry {
        commit_entry: None,
        path: cpath_1.path.clone(),
    };

    let mut compare_entry_2 = CompareEntry {
        commit_entry: None,
        path: cpath_2.path.clone(),
    };

    if let Some(commit_1) = cpath_1.commit {
        let entry_1 = api::local::entries::get_commit_entry(repo, &commit_1, &cpath_1.path)?
            .ok_or_else(|| {
                OxenError::ResourceNotFound(
                    format!("{}@{}", cpath_1.path.display(), commit_1.id).into(),
                )
            })?;

        compare_entry_1.commit_entry = Some(entry_1);
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

        compare_entry_2.commit_entry = Some(entry_2);
    };

    let mut display_by_column: Vec<String> = vec![];

    for col in display {
        display_by_column.push(format!("{}.left", col));
        display_by_column.push(format!("{}.right", col));
    }

    let compare_result = api::local::compare::compare_files(
        repo,
        None,
        compare_entry_1,
        compare_entry_2,
        keys,
        targets,
        display_by_column,
    )?;

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
    use crate::model::entry::commit_entry::CommitPath;
    use crate::test;
    use crate::view::compare::CompareResult;
    #[tokio::test]
    async fn test_compare_same_dataframe_no_keys_no_targets() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let csv1 = "a,b,c\n1,2,3\n4,5,6\n";
            let csv2 = "a,b,c\n1,2,3\n4,5,6\n";

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

            let compare_result = command::compare(&repo, c1, c2, vec![], vec![], vec![])?;

            match compare_result {
                CompareResult::Tabular((_ct, df)) => {
                    assert_eq!(df.height(), 0);
                }
                _ => panic!("expected tabular result"),
            }

            Ok(())
        })
        .await
    }

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

            let compare_result = command::compare(&repo, c1, c2, vec![], vec![], vec![])?;

            let diff_col = ".oxen.diff.status";
            match compare_result {
                CompareResult::Tabular((_ct, df)) => {
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

            let compare_result = command::compare(
                &repo,
                c1,
                c2,
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string()],
                vec![],
            )?;

            let diff_col = ".oxen.diff.status";
            match compare_result {
                CompareResult::Tabular((_ct, df)) => {
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

            let compare_result = command::compare(
                &repo,
                c1,
                c2,
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string()],
                vec![],
            )?;

            let diff_col = ".oxen.diff.status";
            match compare_result {
                CompareResult::Tabular((_ct, df)) => {
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

            let compare_result = command::compare(
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
                CompareResult::Tabular((_ct, df)) => {
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

            let compare_result = command::compare(&repo, c1, c2, vec![], vec![], vec![])?;

            // Should return empty df
            let diff_col = ".oxen.diff.status";
            match compare_result {
                CompareResult::Tabular((_ct, df)) => {
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

            let compare_result = command::compare(
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
                CompareResult::Tabular((_ct, df)) => {
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
