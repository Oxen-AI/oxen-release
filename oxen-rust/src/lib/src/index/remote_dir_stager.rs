use std::path::PathBuf;

use std::path::Path;

use time::OffsetDateTime;

use crate::api;
use crate::command;
use crate::constants;
use crate::constants::{OXEN_HIDDEN_DIR, STAGED_DIR};
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

use super::CommitWriter;

// These methods create a directory within .oxen/staging/branch-name/ that is basically a local oxen repo
// Then we can stage data right into here using the same stager

pub fn branch_staging_dir(repo: &LocalRepository, branch: &Branch) -> PathBuf {
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(&branch.name)
}

pub fn init_or_get(repo: &LocalRepository, branch: &Branch) -> Result<LocalRepository, OxenError> {
    let staging_dir = branch_staging_dir(repo, branch);
    let oxen_dir = staging_dir.join(OXEN_HIDDEN_DIR);
    let branch_repo = if oxen_dir.exists() {
        log::debug!("stage_file Already have oxen repo 🐂");
        LocalRepository::new(&staging_dir)?
    } else {
        log::debug!("stage_file Initializing oxen repo! 🐂");
        init_local_repo_staging_dir(repo, &staging_dir)?
    };

    if !api::local::branches::branch_exists(&branch_repo, &branch.name)? {
        command::create_checkout_branch(&branch_repo, &branch.name)?;
    }

    Ok(branch_repo)
}

pub fn init_local_repo_staging_dir(
    repo: &LocalRepository,
    staging_dir: &Path,
) -> Result<LocalRepository, OxenError> {
    let oxen_hidden_dir = repo.path.join(constants::OXEN_HIDDEN_DIR);
    let staging_oxen_dir = staging_dir.join(constants::OXEN_HIDDEN_DIR);
    log::debug!("Creating staging_oxen_dir: {staging_oxen_dir:?}");
    std::fs::create_dir_all(&staging_oxen_dir)?;

    let dirs_to_copy = vec![
        constants::COMMITS_DIR,
        constants::HISTORY_DIR,
        constants::REFS_DIR,
        constants::HEAD_FILE,
    ];

    for dir in dirs_to_copy {
        let oxen_dir = oxen_hidden_dir.join(dir);
        let staging_dir = staging_oxen_dir.join(dir);

        log::debug!("Copying {dir} dir {oxen_dir:?} -> {staging_dir:?}");
        if oxen_dir.is_dir() {
            util::fs::copy_dir_all(oxen_dir, staging_dir)?;
        } else {
            std::fs::copy(oxen_dir, staging_dir)?;
        }
    }

    LocalRepository::new(staging_dir)
}

// Stages a file in a specified directory
pub fn stage_file(
    repo: &LocalRepository,
    branch_repo: &LocalRepository,
    branch: &Branch,
    filepath: &Path,
) -> Result<PathBuf, OxenError> {
    let staging_dir = branch_staging_dir(repo, branch);
    log::debug!("remote stager before add... staging_dir {:?}", staging_dir);

    // Stager will be in the branch repo
    let stager = Stager::new(branch_repo)?;
    // But we will read from the commit in the main repo
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
    let reader = CommitDirReader::new(repo, &commit)?;
    stager.add_file(filepath.as_ref(), &reader)?;

    log::debug!("remote stager after add...");

    let relative_path = util::fs::path_relative_to_dir(filepath, &staging_dir)?;
    Ok(relative_path)
}

pub fn has_file(branch_repo: &LocalRepository, filepath: &Path) -> Result<bool, OxenError> {
    // Stager will be in the branch repo
    let stager = Stager::new(branch_repo)?;
    stager.has_staged_file(filepath)
}

pub fn delete_file(branch_repo: &LocalRepository, filepath: &Path) -> Result<(), OxenError> {
    // Stager will be in the branch repo
    let stager = Stager::new(branch_repo)?;
    stager.remove_staged_file(filepath)?;
    let full_path = branch_repo.path.join(filepath);
    match std::fs::remove_file(&full_path) {
        Ok(_) => Ok(()),
        Err(e) => {
            log::error!("Error deleting file {full_path:?} -> {e:?}");
            Err(OxenError::file_does_not_exist(full_path))
        }
    }
}

pub fn commit_staged(
    repo: &LocalRepository,
    branch_repo: &LocalRepository,
    branch: &Branch,
    user: &User,
    message: &str,
) -> Result<Commit, OxenError> {
    log::debug!("commit_staged started on branch: {}", branch.name);

    let staging_dir = branch_staging_dir(repo, branch);
    let mut status = status_for_branch(repo, branch_repo, branch)?;

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

    // This should copy all the files over from the staging dir to the versions dir
    let commit = commit_writer.commit_from_new(
        &new_commit,
        &mut status,
        &staging_dir,
        Some(branch.to_owned()),
    )?;
    api::local::branches::update(repo, &branch.name, &commit.id)?;

    match std::fs::remove_dir_all(&staging_dir) {
        Ok(_) => log::debug!("commit_staged: removed staging dir: {:?}", staging_dir),
        Err(e) => log::error!("commit_staged: error removing staging dir: {:?}", e),
    }

    Ok(commit)
}

fn status_for_branch(
    repo: &LocalRepository,
    branch_repo: &LocalRepository,
    branch: &Branch,
) -> Result<StagedData, OxenError> {
    // Stager will be in the branch repo
    let staging_dir = branch_staging_dir(repo, branch);
    log::debug!("commit_staged staging_dir: {:?}", staging_dir);

    let stager = Stager::new(branch_repo)?;
    // But we will read from the commit in the main repo
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
    let reader = CommitDirReader::new(repo, &commit)?;
    log::debug!("commit_staged before status...");

    let status = stager.status(&reader)?;
    log::debug!("commit_staged after status...");
    status.print_stdout();

    Ok(status)
}

pub fn list_staged_data(
    repo: &LocalRepository,
    branch_repo: &LocalRepository,
    branch: &Branch,
    directory: &Path,
) -> Result<StagedData, OxenError> {
    // Stager will be in the branch repo
    let stager = Stager::new(branch_repo)?;
    // But we will read from the commit in the main repo
    log::debug!(
        "list_staged_data get commit by id {} -> {} -> {:?}",
        branch.name,
        branch.commit_id,
        directory
    );
    match api::local::commits::get_by_id(repo, &branch.commit_id)? {
        Some(commit) => {
            let reader = CommitDirReader::new(repo, &commit)?;
            if Path::new(".") == directory {
                log::debug!("list_staged_data: status for root");
                let status = stager.status(&reader)?;
                Ok(status)
            } else {
                let status = stager.status_from_dir(&reader, directory)?;
                Ok(status)
            }
        }
        None => Err(OxenError::commit_id_does_not_exist(&branch.commit_id)),
    }
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
            let branch_dir = index::remote_dir_stager::branch_staging_dir(&repo, &branch);
            let full_dir = branch_dir.join(directory);
            let full_path = full_dir.join(filename);
            let entry_contents = "Hello World";
            std::fs::create_dir_all(full_dir)?;
            util::fs::write_to_path(&full_path, entry_contents)?;

            let branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch)?;
            index::remote_dir_stager::stage_file(&repo, &branch_repo, &branch, &full_path)?;

            // Verify staged data
            let staged_data = index::remote_dir_stager::list_staged_data(
                &repo,
                &branch_repo,
                &branch,
                directory,
            )?;
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
            let branch_dir = index::remote_dir_stager::branch_staging_dir(&repo, &branch);
            let full_dir = branch_dir.join(directory);
            let full_path = full_dir.join(filename);
            let entry_contents = "Hello World";
            std::fs::create_dir_all(full_dir)?;
            util::fs::write_to_path(&full_path, entry_contents)?;
            let branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch)?;
            index::remote_dir_stager::stage_file(&repo, &branch_repo, &branch, &full_path)?;

            let og_commits = command::log(&repo)?;
            let user = User {
                name: String::from("Test User"),
                email: String::from("test@oxen.ai"),
            };
            let message: &str = "I am committing this remote staged data";
            index::remote_dir_stager::commit_staged(&repo, &branch_repo, &branch, &user, message)?;

            for commit in og_commits.iter() {
                println!("OG commit: {commit:#?}");
            }

            let new_commits = command::log(&repo)?;
            assert_eq!(og_commits.len() + 1, new_commits.len());

            Ok(())
        })
    }
}
