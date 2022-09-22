use crate::command;
use crate::error::OxenError;
use crate::index::{CommitReader, RefReader, RefWriter};
use crate::model::{Branch, LocalRepository};

pub fn list(repo: &LocalRepository) -> Result<Vec<Branch>, OxenError> {
    command::list_branches(repo)
}

pub fn get_by_name(repo: &LocalRepository, name: &str) -> Result<Option<Branch>, OxenError> {
    let referencer = RefReader::new(repo)?;
    referencer.get_branch_by_name(name)
}

pub fn update(repo: &LocalRepository, name: &str, commit_id: &str) -> Result<Branch, OxenError> {
    let referencer = RefReader::new(repo)?;
    match referencer.get_branch_by_name(name)? {
        Some(branch) => {
            // Set the branch to point to the commit
            let ref_writer = RefWriter::new(&repo)?;
            match ref_writer.set_branch_commit_id(name, commit_id) {
                Ok(()) => Ok(branch),
                Err(err) => Err(err),
            }
        }
        None => command::create_branch(repo, name, commit_id),
    }
}

pub fn create(repo: &LocalRepository, name: &str) -> Result<Branch, OxenError> {
    command::create_branch_from_head(repo, name)
}

pub fn delete(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    if let Ok(Some(branch)) = command::current_branch(repo) {
        if branch.name == name {
            let err = format!("Err: Cannot delete current checked out branch '{}'", name);
            return Err(OxenError::basic_str(&err));
        }
    }

    if branch_has_been_merged(repo, name)? {
        let ref_writer = RefWriter::new(repo)?;
        ref_writer.delete_branch(name)
    } else {
        let err = format!("Err: The branch '{}' is not fully merged.\nIf you are sure you want to delete it, run 'oxen branch -D {}'.", name, name);
        Err(OxenError::basic_str(&err))
    }
}

fn branch_has_been_merged(repo: &LocalRepository, name: &str) -> Result<bool, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let commit_reader = CommitReader::new(repo)?;

    if let Some(branch_commit_id) = ref_reader.get_commit_id_for_branch(name)? {
        if let Some(commit_id) = ref_reader.head_commit_id()? {
            let history = commit_reader.history_from_commit_id(&commit_id)?;
            for commit in history.iter() {
                if commit.id == branch_commit_id {
                    return Ok(true);
                }
            }
            // We didn't find commit
            Ok(false)
        } else {
            // Cannot check if it has been merged if we are in a detached HEAD state
            Ok(false)
        }
    } else {
        let err = format!("Err: The branch '{}' does not exist.", name);
        Err(OxenError::basic_str(&err))
    }
}

pub fn force_delete(repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
    if let Ok(Some(branch)) = command::current_branch(repo) {
        if branch.name == name {
            let err = format!("Err: Cannot delete current checked out branch '{}'", name);
            return Err(OxenError::basic_str(&err));
        }
    }

    let ref_writer = RefWriter::new(repo)?;
    ref_writer.delete_branch(name)
}
