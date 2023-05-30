//! # oxen checkout
//!
//! Checkout a branch or commit
//!

use std::path::Path;

use crate::core::df::tabular;
use crate::core::index::MergeConflictReader;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository};
use crate::opts::{DFOpts, RestoreOpts};
use crate::{api, command, util};

/// # Checkout a branch or commit id
/// This switches HEAD to point to the branch name or commit id,
/// it also updates all the local files to be from the commit that this branch references
pub async fn checkout<S: AsRef<str>>(
    repo: &LocalRepository,
    value: S,
) -> Result<Option<Branch>, OxenError> {
    let value = value.as_ref();
    log::debug!("--- CHECKOUT START {} ----", value);
    if api::local::branches::exists(repo, value)? {
        if api::local::branches::is_checked_out(repo, value) {
            println!("Already on branch {value}");
            return api::local::branches::get_by_name(repo, value);
        }

        println!("Checkout branch: {value}");
        api::local::branches::set_working_branch(repo, value).await?;
        api::local::branches::set_head(repo, value)?;
        api::local::branches::get_by_name(repo, value)
    } else {
        // If we are already on the commit, do nothing
        if api::local::branches::is_checked_out(repo, value) {
            eprintln!("Commit already checked out {value}");
            return Ok(None);
        }

        println!("Checkout commit: {value}");
        api::local::branches::set_working_commit_id(repo, value).await?;
        api::local::branches::set_head(repo, value)?;
        Ok(None)
    }
}

/// # Checkout a file and take their changes
/// This overwrites the current file with the changes in the branch we are merging in
pub fn checkout_theirs(repo: &LocalRepository, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let merger = MergeConflictReader::new(repo)?;
    let conflicts = merger.list_conflicts()?;
    log::debug!(
        "checkout_theirs {:?} conflicts.len() {}",
        path.as_ref(),
        conflicts.len()
    );

    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts
        .iter()
        .find(|c| c.merge_entry.path == path.as_ref())
    {
        // Lookup the file for the merge commit entry and copy it over
        command::restore(
            repo,
            RestoreOpts::from_path_ref(path, conflict.merge_entry.commit_id.clone()),
        )
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
}

/// # Checkout a file and take our changes
/// This overwrites the current file with the changes we had in our current branch
pub fn checkout_ours(repo: &LocalRepository, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let merger = MergeConflictReader::new(repo)?;
    let conflicts = merger.list_conflicts()?;
    log::debug!(
        "checkout_ours {:?} conflicts.len() {}",
        path.as_ref(),
        conflicts.len()
    );

    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts
        .iter()
        .find(|c| c.merge_entry.path == path.as_ref())
    {
        // Lookup the file for the base commit entry and copy it over
        command::restore(
            repo,
            RestoreOpts::from_path_ref(path, conflict.base_entry.commit_id.clone()),
        )
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
}

/// # Combine Conflicting Tabular Data Files
/// This overwrites the current file with the changes in their file
pub fn checkout_combine<P: AsRef<Path>>(repo: &LocalRepository, path: P) -> Result<(), OxenError> {
    let merger = MergeConflictReader::new(repo)?;
    let conflicts = merger.list_conflicts()?;
    log::debug!(
        "checkout_combine checking path {:?} -> [{}] conflicts",
        path.as_ref(),
        conflicts.len()
    );
    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts
        .iter()
        .find(|c| c.merge_entry.path == path.as_ref())
    {
        if util::fs::is_tabular(&conflict.base_entry.path) {
            let df_base_path = util::fs::version_path(repo, &conflict.base_entry);
            let df_base = tabular::read_df(df_base_path, DFOpts::empty())?;
            let df_merge_path = util::fs::version_path(repo, &conflict.merge_entry);
            let df_merge = tabular::read_df(df_merge_path, DFOpts::empty())?;

            log::debug!("GOT DF HEAD {}", df_base);
            log::debug!("GOT DF MERGE {}", df_merge);

            match df_base.vstack(&df_merge) {
                Ok(result) => {
                    log::debug!("GOT DF COMBINED {}", result);
                    match result.unique(None, polars::frame::UniqueKeepStrategy::First, None) {
                        Ok(mut uniq) => {
                            log::debug!("GOT DF COMBINED UNIQUE {}", uniq);
                            let output_path = repo.path.join(&conflict.base_entry.path);
                            tabular::write_df(&mut uniq, &output_path)
                        }
                        _ => Err(OxenError::basic_str("Could not uniq data")),
                    }
                }
                _ => Err(OxenError::basic_str(
                    "Could not combine data, make sure schema's match",
                )),
            }
        } else {
            Err(OxenError::basic_str(
                "Cannot use --combine on non-tabular data file.",
            ))
        }
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
}
