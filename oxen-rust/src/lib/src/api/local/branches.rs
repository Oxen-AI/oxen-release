use crate::command;
use crate::error::OxenError;
use crate::index::RefReader;
use crate::model::{Branch, LocalRepository};

pub fn list(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    let referencer = RefReader::new(repo)?;
    let branches = referencer.list_branches()?;
    Ok(branches)
}

pub fn get_by_name(repo: &LocalRepository, name: &str) -> Result<Option<Branch>, OxenError> {
    let referencer = RefReader::new(repo)?;
    Ok(referencer.get_branch_by_name(name)?)
}

pub fn create(repo: &LocalRepository, name: &str) -> Result<Branch, OxenError> {
    command::create_branch(repo, name)
}
