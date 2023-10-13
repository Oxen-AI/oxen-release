//! # oxen diff
//!
//! Compare files and directories between versions
//!

use std::path::Path;

use crate::api;
use crate::core::index::MergeConflictReader;
use crate::error::OxenError;
use crate::model::LocalRepository;

/// Diff a file from a commit or compared to another file
/// `resource` can be a None, commit id, branch name, or another path.
///    None: compare `path` to the last commit versioned of the file. If a merge conflict with compare to the merge conflict
///    commit id: compare `path` to the version of `path` from that commit
///    branch name: compare `path` to the version of `path` from that branch
///    another path: compare `path` to the other `path` provided
/// `path` is the path you want to compare the resource to
pub fn diff(
    repo: &LocalRepository,
    resource: Option<&str>,
    path: impl AsRef<Path>,
) -> Result<String, OxenError> {
    if let Some(resource) = resource {
        // `resource` is Some(resource)
        if let Some(compare_commit) = api::local::commits::get_by_id(repo, resource)? {
            // `resource` is a commit id
            let original_commit = api::local::commits::head_commit(repo)?;
            api::local::diff::diff_one(repo, &original_commit, &compare_commit, path)
        } else if let Some(branch) = api::local::branches::get_by_name(repo, resource)? {
            // `resource` is a branch name
            let compare_commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
            let original_commit = api::local::commits::head_commit(repo)?;

            api::local::diff::diff_one(repo, &original_commit, &compare_commit, path)
        } else if Path::new(resource).exists() {
            // `resource` is another path
            api::local::diff::diff_files(resource, path)
        } else {
            Err(OxenError::basic_str(format!(
                "Could not find resource: {resource:?}"
            )))
        }
    } else {
        // `resource` is None
        // First check if there are merge conflicts
        let merger = MergeConflictReader::new(repo)?;
        if merger.has_conflicts()? {
            match merger.get_conflict_commit() {
                Ok(Some(commit)) => {
                    let current_path = path.as_ref();
                    let version_path = api::local::diff::get_version_file_from_commit(
                        repo,
                        &commit,
                        current_path,
                    )?;
                    api::local::diff::diff_files(current_path, version_path)
                }
                err => {
                    log::error!("{err:?}");
                    Err(OxenError::basic_str(format!(
                        "Could not find merge resource: {resource:?}"
                    )))
                }
            }
        } else {
            // No merge conflicts, compare to last version committed of the file
            let current_path = path.as_ref();
            let commit = api::local::commits::head_commit(repo)?;
            let version_path =
                api::local::diff::get_version_file_from_commit(repo, &commit, current_path)?;
            api::local::diff::diff_files(version_path, current_path)
        }
    }
}
