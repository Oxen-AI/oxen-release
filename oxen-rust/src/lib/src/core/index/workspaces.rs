use std::path::PathBuf;

use std::path::Path;

use time::OffsetDateTime;

use crate::api;
use crate::command;
use crate::constants;
use crate::constants::{OXEN_HIDDEN_DIR, WORKSPACES_DIR};
use crate::core::index::CommitEntryReader;
use crate::core::index::Stager;
use crate::error::OxenError;
use crate::model::Commit;
use crate::model::LocalRepository;
use crate::model::NewCommit;
use crate::model::NewCommitBody;
use crate::model::StagedData;
use crate::util;

use super::CommitWriter;

pub mod data_frames;
pub mod files;
pub mod stager;

// These methods create a directory within .oxen/workspaces/commit_id/workspace_id/ that is a local oxen repo
pub fn workspace_dir(repo: &LocalRepository, commit: &Commit, workspace_id: &str) -> PathBuf {
    // Just in case they pass in the email or some other random string, hash it for nice dir name
    let workspace_id_hash = util::hasher::hash_str_sha256(workspace_id);
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(WORKSPACES_DIR)
        .join(&commit.id)
        .join(workspace_id_hash)
}

pub fn init_or_get(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
) -> Result<LocalRepository, OxenError> {
    // Cleans up the staging dir if there is an error at any point
    match p_init_or_get(repo, commit, workspace_id) {
        Ok(repo) => {
            log::debug!(
                "Got branch staging dir for workspace_id {:?} at path {:?}",
                workspace_id,
                repo.path
            );
            Ok(repo)
        }
        Err(e) => {
            let workspace_dir = workspace_dir(repo, commit, workspace_id);
            log::error!("error: {:?}", e);
            log::debug!(
                "error commit workspace dir for workspace_id {:?} at path {:?}",
                workspace_id,
                workspace_dir
            );
            util::fs::remove_dir_all(workspace_dir)?;
            Err(e)
        }
    }
}

pub fn p_init_or_get(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
) -> Result<LocalRepository, OxenError> {
    let workspace_dir = workspace_dir(repo, commit, workspace_id);
    let oxen_dir = workspace_dir.join(OXEN_HIDDEN_DIR);
    let workspace = if oxen_dir.exists() {
        log::debug!("p_init_or_get already have oxen repo directory");
        LocalRepository::new(&workspace_dir)?
    } else {
        log::debug!("p_init_or_get Initializing oxen repo! 🐂");

        let workspace = init_local_repo(repo, &workspace_dir)?;
        workspace
    };

    Ok(workspace)
}

pub fn init_local_repo(
    repo: &LocalRepository,
    staging_dir: &Path,
) -> Result<LocalRepository, OxenError> {
    let oxen_hidden_dir = repo.path.join(constants::OXEN_HIDDEN_DIR);
    let staging_oxen_dir = staging_dir.join(constants::OXEN_HIDDEN_DIR);
    log::debug!("Creating staging_oxen_dir {staging_oxen_dir:?}");
    util::fs::create_dir_all(&staging_oxen_dir)?;

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
            util::fs::copy_dir_all(oxen_dir, staging_dir)?;
        } else {
            util::fs::copy(oxen_dir, staging_dir)?;
        }
    }

    LocalRepository::new(staging_dir)
}

pub fn commit(
    repo: &LocalRepository,
    workspace: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    new_commit: &NewCommitBody,
    branch_name: &str,
) -> Result<Commit, OxenError> {
    log::debug!(
        "commit_staged started for commit {} on branch: {}",
        commit.id,
        branch_name
    );

    // Check if the branch exists, if not created it
    // If it does exist, we can only commit to it if the commit ids match
    // If the commit ids don't match, we need to reject for now
    // TODO: Merge and handle conflicts better
    let mut branch = api::local::branches::get_by_name(repo, branch_name)?;

    if branch.is_none() {
        branch = Some(api::local::branches::create(repo, branch_name, &commit.id)?);
    }

    let branch = branch.unwrap();

    if branch.commit_id != commit.id {
        return Err(OxenError::basic_str(format!(
            "Branch {} is not up to date, cannot commit",
            branch_name,
        )));
    }

    let staging_dir = workspace_dir(repo, commit, workspace_id);
    let status = status_for_workspace(repo, workspace, commit)?;

    log::debug!("got branch status: {:#?}", &status);

    let commit_writer = CommitWriter::new(repo)?;
    let timestamp = OffsetDateTime::now_utc();

    let new_commit = NewCommit {
        parent_ids: vec![commit.id.to_owned()],
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
        &branch,
        workspace_id,
    )?;
    api::local::branches::update(repo, &branch.name, &commit.id)?;

    log::debug!("commit_staged cleaning up staging dir: {:?}", staging_dir);
    match util::fs::remove_dir_all(&staging_dir) {
        Ok(_) => log::debug!("commit_staged: removed staging dir: {:?}", staging_dir),
        Err(e) => log::error!("commit_staged: error removing staging dir: {:?}", e),
    }

    Ok(commit)
}

fn status_for_workspace(
    repo: &LocalRepository,
    workspace: &LocalRepository,
    commit: &Commit,
) -> Result<StagedData, OxenError> {
    let stager = Stager::new(workspace)?;
    let reader = CommitEntryReader::new(repo, commit)?;
    let status = stager.status(&reader)?;
    status.print_stdout();

    Ok(status)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::core::index;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::test;
    use crate::util;

    #[test]
    fn test_remote_stager_stage_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Stage file contents
            let commit = api::local::commits::head_commit(&repo)?;
            let directory = Path::new("data/");
            let filename = Path::new("Readme.md");
            let workspace_id = UserConfig::identifier()?;
            let workspaces_dir = index::workspaces::workspace_dir(&repo, &commit, &workspace_id);
            let full_dir = workspaces_dir.join(directory);
            let full_path = full_dir.join(filename);
            let entry_contents = "Hello World";
            std::fs::create_dir_all(full_dir)?;
            util::fs::write_to_path(&full_path, entry_contents)?;

            let workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;
            index::workspaces::files::add(&repo, &workspace, &commit, &workspace_id, &full_path)?;

            // Verify staged data
            let staged_data = index::workspaces::stager::status(
                &repo,
                &workspace,
                &commit,
                &workspace_id,
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
            let commit = api::local::commits::head_commit(&repo)?;
            let directory = Path::new("data/");
            let filename = Path::new("Readme.md");
            let workspace_id = UserConfig::identifier()?;

            let workspace_dir = index::workspaces::workspace_dir(&repo, &commit, &workspace_id);
            let full_dir = workspace_dir.join(directory);
            let full_path = full_dir.join(filename);
            let entry_contents = "Hello World";
            std::fs::create_dir_all(full_dir)?;
            util::fs::write_to_path(&full_path, entry_contents)?;
            let workspace = index::workspaces::init_or_get(&repo, &commit, &workspace_id)?;
            index::workspaces::files::add(&repo, &workspace, &commit, &workspace_id, &full_path)?;

            let og_commits = api::local::commits::list(&repo)?;
            let new_commit = NewCommitBody {
                author: String::from("Test User"),
                email: String::from("test@oxen.ai"),
                message: String::from("I am committing this remote staged data"),
            };
            index::workspaces::commit(
                &repo,
                &workspace,
                &commit,
                &workspace_id,
                &new_commit,
                DEFAULT_BRANCH_NAME,
            )?;

            for commit in og_commits.iter() {
                println!("OG commit: {commit:#?}");
            }

            let new_commits = api::local::commits::list(&repo)?;
            assert_eq!(og_commits.len() + 1, new_commits.len());

            Ok(())
        })
    }
}
