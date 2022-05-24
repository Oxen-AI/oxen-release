
use crate::error::OxenError;
use crate::index::{Referencer, Committer};
use crate::model::{LocalRepository, Branch};

pub fn list(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    let referencer = Referencer::new(repo)?;
    let branches = referencer.list_branches()?;
    Ok(branches)
}

pub fn get_by_name(repo: &LocalRepository, name: &str) -> Result<Option<Branch>, OxenError> {
    let referencer = Referencer::new(repo)?;
    Ok(referencer.get_branch_by_name(name)?)
}

pub fn create(repo: &LocalRepository, name: &str) -> Result<Branch, OxenError> {
    let committer = Committer::new(repo)?;
    match committer.get_head_commit() {
        Ok(Some(head_commit)) => {
            committer.referencer.create_branch(name, &head_commit.id)
        }
        _ => Err(OxenError::basic_str(
            "Err: No Commits. Cannot create a branch until you make your initial commit.",
        )),
    }
}
