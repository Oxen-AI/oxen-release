use std::path::PathBuf;

use std::path::Path;

use time::OffsetDateTime;

use crate::api;
use crate::command;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::error::OxenError;
use crate::index::CommitDirReader;
use crate::index::Stager;
use crate::model::Branch;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::model::NewCommit;
use crate::model::StagedData;
use crate::model::User;
use crate::util;

use super::stager::STAGED_DIR;
use super::CommitWriter;

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
        let branch_repo = command::init(&staging_dir)?;
        if !api::local::branches::branch_exists(&branch_repo, &branch.name)? {
            command::create_checkout_branch(&branch_repo, &branch.name)?;
        }
        branch_repo
    };

    log::debug!("remote stager before add...");

    // Stager will be in the branch repo
    let stager = Stager::new(&branch_repo)?;
    // But we will read from the commit in the main repo
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
    let reader = CommitDirReader::new(repo, &commit)?;

    let full_path = staging_dir.join(filepath);
    stager.add_file(full_path.as_ref(), &reader)?;

    log::debug!("remote stager after add...");

    let relative_path = util::fs::path_relative_to_dir(filepath, &staging_dir)?;
    Ok(relative_path)
}

pub fn commit_staged(
    repo: &LocalRepository,
    branch: &Branch,
    user: &User,
    message: &str,
) -> Result<Commit, OxenError> {
    // Stager will be in the branch repo
    let staging_dir = branch_staging_dir(repo, branch);
    let branch_repo = LocalRepository::new(&staging_dir)?;
    let stager = Stager::new(&branch_repo)?;
    // But we will read from the commit in the main repo
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
    let reader = CommitDirReader::new(repo, &commit)?;
    let status = stager.status(&reader)?;

    if !status.has_added_entries() {
        return Err(OxenError::basic_str("There are no files staged"));
    }

    let commit_writer = CommitWriter::new(repo)?;
    let timestamp = OffsetDateTime::now_utc();

    let new_commit = NewCommit {
        parent_ids: vec![branch.commit_id.to_owned()],
        message: String::from(message),
        author: user.name.to_owned(),
        email: user.email.to_owned(),
        timestamp,
    };
    log::debug!("commit_staged: new_commit: {:#?}", &new_commit);

    let commit = commit_writer.commit_from_new(
        &new_commit,
        &status,
        &staging_dir,
        Some(branch.to_owned()),
    )?;
    api::local::branches::update(repo, &branch.name, &commit.id)?;

    stager.unstage()?;

    // TODO: cleanup all files in staging dir

    Ok(commit)
}

pub fn list_staged_data(repo: &LocalRepository, branch: &Branch) -> Result<StagedData, OxenError> {
    let staging_dir = branch_staging_dir(repo, branch);
    let branch_repo = LocalRepository::new(&staging_dir)?;

    // Stager will be in the branch repo
    let stager = Stager::new(&branch_repo)?;
    // But we will read from the commit in the main repo
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
    let reader = CommitDirReader::new(repo, &commit)?;
    let status = stager.status(&reader)?;

    Ok(status)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::command;
    use crate::error::OxenError;
    use crate::index;
    use crate::model::User;
    use crate::test;
    use crate::util;

    #[test]
    fn test_remote_stager_stage_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Stage file contents
            let branch = command::current_branch(&repo)?.unwrap();
            let directory = Path::new("data/");
            let filename = Path::new("Readme.md");
            let branch_dir = index::remote_stager::branch_staging_dir(&repo, &branch);
            let full_dir = branch_dir.join(directory);
            let full_path = full_dir.join(filename);
            let relative_path = directory.join(filename);
            let entry_contents = "Hello World";
            std::fs::create_dir_all(full_dir)?;
            util::fs::write_to_path(&full_path, entry_contents)?;

            index::remote_stager::stage_file(&repo, &branch, &relative_path)?;

            // Verify staged data
            let staged_data = index::remote_stager::list_staged_data(&repo, &branch)?;
            staged_data.print_stdout();
            assert_eq!(staged_data.added_files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_remote_commit_staged() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Stage file contents
            let branch = command::current_branch(&repo)?.unwrap();
            let directory = Path::new("data/");
            let filename = Path::new("Readme.md");
            let branch_dir = index::remote_stager::branch_staging_dir(&repo, &branch);
            let full_dir = branch_dir.join(directory);
            let full_path = full_dir.join(filename);
            let relative_path = directory.join(filename);
            let entry_contents = "Hello World";
            std::fs::create_dir_all(full_dir)?;
            util::fs::write_to_path(&full_path, entry_contents)?;
            index::remote_stager::stage_file(&repo, &branch, &relative_path)?;

            let og_commits = command::log(&repo)?;
            let user = User {
                name: String::from("Test User"),
                email: String::from("test@oxen.ai"),
            };
            let message: &str = "I am committing this remote staged data";
            index::remote_stager::commit_staged(&repo, &branch, &user, message)?;

            let new_commits = command::log(&repo)?;
            assert_eq!(og_commits.len() + 1, new_commits.len());

            Ok(())
        })
    }
}
