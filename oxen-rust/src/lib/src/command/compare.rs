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

    // TODONOW: assert and parse tabular
    api::local::compare::compare_files(
        repo,
        file_1,
        commit_1,
        file_2,
        commit_2,
        keys,
        targets,
        DFOpts::empty(), // TODONOW: is this going to affect anything?
    )?;
    Ok("".to_string())
}
