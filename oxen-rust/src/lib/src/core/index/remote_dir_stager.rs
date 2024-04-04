use std::path::PathBuf;

use std::path::Path;

use time::OffsetDateTime;

use crate::api;
use crate::constants;
use crate::constants::{OXEN_HIDDEN_DIR, STAGED_DIR};
use crate::core::index;
use crate::core::index::CommitEntryReader;
use crate::core::index::SchemaReader;
use crate::core::index::Stager;
use crate::error::OxenError;
use crate::model::Branch;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::model::NewCommit;
use crate::model::NewCommitBody;
use crate::model::StagedData;
use crate::util;

use super::CommitWriter;

// These methods create a directory within .oxen/staging/branch-name/user-id-hash/ that is a local oxen repo
// Then we can stage data right into here using the same stager, but on a per user basis

pub fn branch_staging_dir(repo: &LocalRepository, branch: &Branch, user_id: &str) -> PathBuf {
    // Just in case they pass in the email or some other random string, hash it for nice dir name
    // This does double-hash right now, since `identifier` is already hashed
    let user_id_hash = util::hasher::hash_str_sha256(user_id);
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(STAGED_DIR)
        .join(&branch.name)
        .join(user_id_hash)
}

pub fn init_or_get(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
) -> Result<LocalRepository, OxenError> {
    // Cleans up the staging dir if there is an error at any point
    match p_init_or_get(repo, branch, user_id) {
        Ok(repo) => {
            log::debug!(
                "Got branch staging dir for userid {:?} at path {:?}",
                user_id,
                repo.path
            );
            Ok(repo)
        }
        Err(e) => {
            let staging_dir = branch_staging_dir(repo, branch, user_id);
            log::debug!(
                "error branch staging dir for userid {:?} at path {:?}",
                user_id,
                staging_dir
            );
            util::fs::remove_dir_all(staging_dir)?;
            Err(e)
        }
    }
}

pub fn p_init_or_get(
    repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
) -> Result<LocalRepository, OxenError> {
    let staging_dir = branch_staging_dir(repo, branch, user_id);
    let oxen_dir = staging_dir.join(OXEN_HIDDEN_DIR);
    let branch_repo = if oxen_dir.exists() {
        log::debug!("stage_file Already have oxen repo 🐂");
        if local_staging_dir_is_up_to_date(repo, &staging_dir, branch)? {
            LocalRepository::new(&staging_dir)?
        } else {
            // need to re-copy over data
            let should_clear = true;
            init_local_repo_staging_dir(repo, &staging_dir, should_clear)?
        }
    } else {
        log::debug!("stage_file Initializing oxen repo! 🐂");
        let should_clear = false;
        init_local_repo_staging_dir(repo, &staging_dir, should_clear)?
    };

    if !api::local::branches::exists(&branch_repo, &branch.name)? {
        api::local::branches::create_checkout(&branch_repo, &branch.name)?;
    }

    Ok(branch_repo)
}

fn local_staging_dir_is_up_to_date(
    repo: &LocalRepository,
    staging_dir: &Path,
    branch: &Branch,
) -> Result<bool, OxenError> {
    log::debug!("local_staging_dir_is_up_to_date path {:?}", staging_dir);
    let staging_repo = LocalRepository::new(staging_dir)?;
    log::debug!(
        "local_staging_dir_is_up_to_date staging_repo {:?}",
        staging_repo
    );

    let oxen_commits = api::local::commits::list_from(repo, &branch.commit_id)?;
    let staging_commits = api::local::commits::list(&staging_repo)?;

    // If the number of commits is different, then we know we need to update
    Ok(oxen_commits.len() == staging_commits.len())
}

pub fn init_local_repo_staging_dir(
    repo: &LocalRepository,
    staging_dir: &Path,
    should_clear: bool,
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
        constants::OBJECTS_DIR,
    ];

    for dir in dirs_to_copy {
        let oxen_dir = oxen_hidden_dir.join(dir);
        let staging_dir = staging_oxen_dir.join(dir);

        log::debug!("Copying {dir} dir {oxen_dir:?} -> {staging_dir:?}");
        if oxen_dir.is_dir() {
            if should_clear {
                util::fs::remove_dir_all(&staging_dir)?;
            }
            util::fs::copy_dir_all(oxen_dir, staging_dir)?;
        } else {
            if should_clear {
                util::fs::remove_file(&staging_dir)?;
            }
            util::fs::copy(oxen_dir, staging_dir)?;
        }
    }

    LocalRepository::new(staging_dir)
}

// Stages a file in a specified directory
pub fn stage_file(
    repo: &LocalRepository,
    branch_repo: &LocalRepository,
    branch: &Branch,
    user_id: &str,
    filepath: &Path,
) -> Result<PathBuf, OxenError> {
    let staging_dir = branch_staging_dir(repo, branch, user_id);
    log::debug!("remote stager before add... staging_dir {:?}", staging_dir);

    // Stager will be in the branch repo
    let stager = Stager::new(branch_repo)?;
    // But we will read from the commit in the main repo
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
    let reader = CommitEntryReader::new(repo, &commit)?;
    log::debug!("about to add file in the stager");
    // Add a schema_reader to stager.add_file for?

    let schema_reader = SchemaReader::new(repo, &commit.id)?;

    stager.add_file(filepath.as_ref(), &reader, &schema_reader)?;
    log::debug!("done adding file in the stager");

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
    match util::fs::remove_file(&full_path) {
        Ok(_) => Ok(()),
        Err(e) => {
            log::error!("Error deleting file {full_path:?} -> {e:?}");
            Err(OxenError::entry_does_not_exist(full_path))
        }
    }
}

pub fn commit(
    repo: &LocalRepository,
    branch_repo: &LocalRepository,
    branch: &Branch,
    new_commit: &NewCommitBody,
    user_id: &str,
) -> Result<Commit, OxenError> {
    log::debug!("commit_staged started on branch: {}", branch.name);

    let staging_dir = branch_staging_dir(repo, branch, user_id);
    let status = status_for_branch(repo, branch_repo, branch)?;

    log::debug!("got branch status: {:#?}", &status);

    let commit_writer = CommitWriter::new(repo)?;
    let timestamp = OffsetDateTime::now_utc();

    let new_commit = NewCommit {
        parent_ids: vec![branch.commit_id.to_owned()],
        message: new_commit.message.to_owned(),
        author: new_commit.author.to_owned(),
        email: new_commit.email.to_owned(),
        timestamp,
    };
    log::debug!("commit_staged: new_commit: {:#?}", &new_commit);

    // This should copy all the files over from the staging dir to the versions dir
    let commit = commit_writer.commit_from_new_on_remote_branch(
        &new_commit,
        &status,
        &staging_dir,
        branch,
        user_id,
    )?;
    api::local::branches::update(repo, &branch.name, &commit.id)?;

    log::debug!("commit_staged cleaning up staging dir: {:?}", staging_dir);
    match util::fs::remove_dir_all(&staging_dir) {
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
    let stager = Stager::new(branch_repo)?;
    // But we will read from the commit in the main repo
    let commit = api::local::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
    let reader = CommitEntryReader::new(repo, &commit)?;
    let status = stager.status(&reader)?;
    status.print_stdout();

    Ok(status)
}

pub fn list_staged_data(
    repo: &LocalRepository,
    branch_repo: &LocalRepository,
    branch: &Branch,
    uuid: &str,
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
            let reader = CommitEntryReader::new(repo, &commit)?;
            if Path::new(".") == directory {
                log::debug!("list_staged_data: status for root");
                let mut status = stager.status(&reader)?;
                add_mod_entries(repo, branch, uuid, &mut status)?;
                Ok(status)
            } else {
                let mut status = stager.status_from_dir(&reader, directory)?;
                add_mod_entries(repo, branch, uuid, &mut status)?;
                Ok(status)
            }
        }
        None => Err(OxenError::commit_id_does_not_exist(&branch.commit_id)),
    }
}

// Modifications to files are staged in a separate DB and applied on commit, so we fetch them from the mod_stager
fn add_mod_entries(
    repo: &LocalRepository,
    branch: &Branch,
    uuid: &str,
    status: &mut StagedData,
) -> Result<(), OxenError> {
    let mod_entries = index::mod_stager::list_mod_entries(repo, branch, uuid)?;

    for path in mod_entries {
        status.modified_files.push(path.to_owned());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::config::UserConfig;
    use crate::core::index;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::test;
    use crate::util;

    #[test]
    fn test_remote_stager_stage_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Stage file contents
            let branch = api::local::branches::current_branch(&repo)?.unwrap();
            let directory = Path::new("data/");
            let filename = Path::new("Readme.md");
            let user_id = UserConfig::identifier()?;
            let branch_dir = index::remote_dir_stager::branch_staging_dir(&repo, &branch, &user_id);
            let full_dir = branch_dir.join(directory);
            let full_path = full_dir.join(filename);
            let entry_contents = "Hello World";
            std::fs::create_dir_all(full_dir)?;
            util::fs::write_to_path(&full_path, entry_contents)?;

            let branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch, &user_id)?;
            index::remote_dir_stager::stage_file(
                &repo,
                &branch_repo,
                &branch,
                &user_id,
                &full_path,
            )?;

            // Verify staged data
            let staged_data = index::remote_dir_stager::list_staged_data(
                &repo,
                &branch_repo,
                &branch,
                &user_id,
                directory,
            )?;
            staged_data.print_stdout();
            assert_eq!(staged_data.staged_files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_remote_commit() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Stage file contents
            let branch = api::local::branches::current_branch(&repo)?.unwrap();
            let directory = Path::new("data/");
            let filename = Path::new("Readme.md");
            let user_id = UserConfig::identifier()?;

            let branch_dir = index::remote_dir_stager::branch_staging_dir(&repo, &branch, &user_id);
            let full_dir = branch_dir.join(directory);
            let full_path = full_dir.join(filename);
            let entry_contents = "Hello World";
            std::fs::create_dir_all(full_dir)?;
            util::fs::write_to_path(&full_path, entry_contents)?;
            let branch_repo = index::remote_dir_stager::init_or_get(&repo, &branch, &user_id)?;
            index::remote_dir_stager::stage_file(
                &repo,
                &branch_repo,
                &branch,
                &user_id,
                &full_path,
            )?;

            let og_commits = api::local::commits::list(&repo)?;
            let new_commit = NewCommitBody {
                author: String::from("Test User"),
                email: String::from("test@oxen.ai"),
                message: String::from("I am committing this remote staged data"),
            };
            index::remote_dir_stager::commit(&repo, &branch_repo, &branch, &new_commit, &user_id)?;

            for commit in og_commits.iter() {
                println!("OG commit: {commit:#?}");
            }

            let new_commits = api::local::commits::list(&repo)?;
            assert_eq!(og_commits.len() + 1, new_commits.len());

            Ok(())
        })
    }
}
