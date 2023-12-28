use crate::api;
use crate::error::OxenError;
use crate::model::entry::commit_entry::CommitPath;
use crate::model::LocalRepository;
use std::path::PathBuf;

pub fn compare(
    repo: &LocalRepository,
    cpath_1: CommitPath,
    cpath_2: CommitPath,
    keys: Vec<String>,
    targets: Vec<String>,
    output: Option<PathBuf>,
) -> Result<(), OxenError> {
    let entry_1 = api::local::entries::get_commit_entry(repo, &cpath_1.commit, &cpath_1.path)?
        .ok_or_else(|| {
            OxenError::ResourceNotFound(
                format!("{}@{}", cpath_1.path.display(), cpath_1.commit.id).into(),
            )
        })?;
    let entry_2 = api::local::entries::get_commit_entry(repo, &cpath_2.commit, &cpath_2.path)?
        .ok_or_else(|| {
            OxenError::ResourceNotFound(
                format!("{}@{}", cpath_2.path.display(), cpath_2.commit.id).into(),
            )
        })?;

    let _compare: crate::view::compare::CompareTabular =
        api::local::compare::compare_files(repo, None, entry_1, entry_2, keys, targets, output)?;
    Ok(())
}
