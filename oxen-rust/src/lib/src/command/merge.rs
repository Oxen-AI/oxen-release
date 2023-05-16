//! # oxen merge
//!
//! Merge a branch into the current branch
//!

use crate::api;
use crate::core::index::Merger;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};

/// # Merge a branch into the current branch
/// Checks for simple fast forward merge, or if current branch has diverged from the merge branch
/// it will perform a 3 way merge
/// If there are conflicts, it will abort and show the conflicts to be resolved in the `status` command
pub fn merge<S: AsRef<str>>(
    repo: &LocalRepository,
    merge_branch_name: S,
) -> Result<Option<Commit>, OxenError> {
    let merge_branch_name = merge_branch_name.as_ref();
    if !api::local::branches::exists(repo, merge_branch_name)? {
        return Err(OxenError::local_branch_not_found(merge_branch_name));
    }

    let base_branch =
        api::local::branches::current_branch(repo)?.ok_or(OxenError::must_be_on_valid_branch())?;
    let merge_branch = api::local::branches::get_by_name(repo, merge_branch_name)?
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
