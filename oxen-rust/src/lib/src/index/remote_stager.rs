use std::path::PathBuf;

use std::path::Path;

use crate::api;
use crate::command;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::error::OxenError;
use crate::index::CommitDirReader;
use crate::index::Stager;
use crate::model::Branch;
use crate::model::LocalRepository;
use crate::model::StagedData;
use crate::model::StagedEntry;
use crate::util;

use super::stager::STAGED_DIR;

// These methods create a directory within .oxen/staging/branch-name/ that is basically a local oxen repo
// Then we can stage data right into here using the same stager

pub fn branch_staging_dir(repo: &LocalRepository, branch: &Branch) -> PathBuf {
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(&branch.name)
}

// Stages a file in a specified directory
pub fn stage_file(
    repo: &LocalRepository,
    branch: &Branch,
    filepath: &Path,
) -> Result<PathBuf, OxenError> {
    let staging_dir = branch_staging_dir(repo, branch);
    let oxen_dir = staging_dir.join(OXEN_HIDDEN_DIR);
    let branch_repo = if oxen_dir.exists() {
        log::debug!("stage_file Already have oxen repo 🐂");
        LocalRepository::new(&staging_dir)?
    } else {
        log::debug!("stage_file Initializing oxen repo! 🐂");
        let repo = command::init(&staging_dir)?;
        if !api::local::branches::branch_exists(&repo, &branch.name)? {
            command::create_checkout_branch(&repo, &branch.name)?;
        }
        repo
    };

    log::debug!("remote stager before add...");

    // Stager will be in the branch repo
    let stager = Stager::new(&branch_repo)?;
    // But we will read from the commit in the main repo
    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
    let reader = CommitDirReader::new(&repo, &commit)?;

    let full_path = staging_dir.join(filepath);
    stager.add_file(full_path.as_ref(), &reader)?;

    log::debug!("remote stager after add...");

    let relative_path = util::fs::path_relative_to_dir(&filepath, &staging_dir)?;
    Ok(relative_path)
}

// Stages a row in a DataFrame verifying the schema
pub fn stage_row(repo: &LocalRepository, branch: &Branch) -> Result<StagedEntry, OxenError> {
    Err(OxenError::basic_str("TODO"))
}

// Stages a line we want to append to a file
pub fn stage_append(repo: &LocalRepository, branch: &Branch) -> Result<StagedEntry, OxenError> {
    Err(OxenError::basic_str("TODO"))
}

pub fn list_staged_data(repo: &LocalRepository, branch: &Branch) -> Result<StagedData, OxenError> {
    let staging_dir = branch_staging_dir(repo, branch);
    let branch_repo = LocalRepository::new(&staging_dir)?;

    // Stager will be in the branch repo
    let stager = Stager::new(&branch_repo)?;
    // But we will read from the commit in the main repo
    let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
    let reader = CommitDirReader::new(&repo, &commit)?;
    let status = stager.status(&reader)?;

    Ok(status)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::command;
    use crate::error::OxenError;
    use crate::index;
    use crate::index::remote_stager::stage_file;
    use crate::test;

    #[test]
    fn test_remote_stager_stage_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Stage file contents, pass in a directory and extension and it returns a unique file that was added
            let branch = command::current_branch(&repo)?.unwrap();
            let directory = PathBuf::from("data/");
            let extension = "md"; // markdown file
            let entry_contents = "Hello World";

            panic!("TODO");
            // stage_file(&repo, &branch, &directory, &extension, &entry_contents.as_bytes())?;

            // Verify staged data
            let staged_data = index::remote_stager::list_staged_data(&repo, &branch)?;
            staged_data.print_stdout();
            assert_eq!(staged_data.added_files.len(), 1);

            Ok(())
        })
    }
}
