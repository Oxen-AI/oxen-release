use serde::{Deserialize, Serialize};

use crate::core::df::tabular;
use crate::core::index::CommitDirEntryReader;
use crate::error::OxenError;
use crate::model::diff::diff_entry_status::DiffEntryStatus;
use crate::model::diff::generic_diff::GenericDiff;
use crate::model::{Commit, CommitEntry, DataFrameDiff, DiffEntry, LocalRepository, Schema};
use crate::opts::DFOpts;
use crate::view::compare::AddRemoveModifyCounts;
use crate::view::Pagination;
use crate::{constants, util, api};

use crate::core::index::CommitEntryReader;
use colored::Colorize;
use difference::{Changeset, Difference};
use polars::export::ahash::HashMap;
use polars::prelude::DataFrame;
use polars::prelude::IntoLazy;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub fn compare_files(
    repo: &LocalRepository,
    file_1: PathBuf,
    commit_1: Commit,
    file_2: PathBuf,
    commit_2: Commit, 
    keys: Vec<String>,
    targets: Vec<String>, // TODONOW: custom return type 
) -> Result<String, OxenError> {
    // Assert that the files exist in their respective commits and are tabular. 
    let version_file_1 = api::local::diff::get_version_file_from_commit(repo, &commit_1, &file_1)?;
    let version_file_2 = api::local::diff::get_version_file_from_commit(repo, &commit_2, &file_2)?;

    if !util::fs::is_tabular(&version_file_1) || !util::fs::is_tabular(&version_file_2) {
        return Err(OxenError::invalid_file_type(format!(
            "Compare not supported for non-tabular files, found {file_1:?} and {file_2:?}",
        )));
    }


    Ok("".to_string())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::model::diff::diff_entry_status::DiffEntryStatus;
    use crate::opts::RmOpts;
    use crate::test;
    use crate::util;

    #[test]
    fn test_compare_fails_when_not_tabular() -> Result<(), OxenError> {
        test::run_bounding_box_csv_repo_test_fully_committed(|repo| {
            let hello_file = repo.path.join("Hello.txt");
            let world_file = repo.path.join("World.txt");
            test::write_txt_file_to_path(&hello_file, "Hello")?;
            test::write_txt_file_to_path(&world_file, "World")?;

            command::add(&repo, &hello_file)?;
            command::add(&repo, &world_file)?;

            command::commit(&repo, "adding_new_files")?;

            let head_commit = api::local::commits::head_commit(&repo)?;

            let keys = vec![];
            let targets = vec![];

            let result = api::local::compare::compare_files(
                &repo,
                hello_file,
                head_commit.clone(),
                world_file,
                head_commit,
                keys,
                targets,
            );

            log::debug!("{:?}", result);
            assert!(matches!(
                result.unwrap_err(),
                OxenError::InvalidFileType(_)
            ));

            Ok(())
        })
    }
}