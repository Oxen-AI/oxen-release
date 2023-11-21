use crate::api;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::DFOpts;
use std::path::PathBuf;

// TODONOW: Doc comments

pub fn compare(
    repo: &LocalRepository,
    file_1: PathBuf,
    revision_1: Option<&str>,
    file_2: PathBuf,
    revision_2: Option<&str>,
    keys: Vec<String>,
    targets: Vec<String>,
) -> Result<String, OxenError> {
    let current_commit = api::local::commits::head_commit(repo)?;
    // For revision_1 and revision_2, if none, set to current_commit
    let revision_1 = revision_1.unwrap_or(current_commit.id.as_str());
    let revision_2 = revision_2.unwrap_or(current_commit.id.as_str());

    let commit_1 = api::local::revisions::get(repo, revision_1)?
        .ok_or_else(|| OxenError::revision_not_found(revision_1.into()))?;
    let commit_2 = api::local::revisions::get(repo, revision_2)?
        .ok_or_else(|| OxenError::revision_not_found(revision_2.into()))?;

    let entry_1 = api::local::entries::get_commit_entry(repo, &commit_1, &file_1)?
        .ok_or_else(|| OxenError::ResourceNotFound(format!("{}@{}", file_1.display(), revision_1).into()))?;
    let entry_2 = api::local::entries::get_commit_entry(repo, &commit_2, &file_2)?
        .ok_or_else(|| OxenError::ResourceNotFound(format!("{}@{}", file_2.display(), revision_2).into()))?;


    api::local::compare::compare_files(
        repo,
        "temp_cli_id",
        entry_1,
        entry_2,
        keys,
        targets,
        false, 
        DFOpts::empty(),
    )?;
    Ok("".to_string())
}
