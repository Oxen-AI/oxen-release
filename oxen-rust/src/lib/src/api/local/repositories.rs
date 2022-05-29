use crate::api;
use crate::command;
use crate::error::OxenError;
use crate::index::{CommitEntryReader, CommitWriter};
use crate::model::{CommitStats, LocalRepository, RepositoryNew};
use crate::util;

use std::path::Path;
use walkdir::WalkDir;

pub fn get_by_name(sync_dir: &Path, name: &str) -> Result<LocalRepository, OxenError> {
    let repo_dir = sync_dir.join(name);

    if !repo_dir.exists() {
        let err = format!("Repo does not exist: {:?}", repo_dir);
        return Err(OxenError::basic_str(&err));
    }

    let repo = LocalRepository::from_dir(&repo_dir)?;
    Ok(repo)
}

pub fn get_head_commit_stats(repo: &LocalRepository) -> Result<CommitStats, OxenError> {
    let commit = command::head_commit(repo)?;
    let reader = CommitEntryReader::new_from_head(repo)?;
    Ok(CommitStats {
        commit,
        num_entries: reader.num_entries()?,
        num_synced_files: util::fs::rcount_files_in_dir(&repo.path),
    })
}

pub fn get_commit_stats_from_id(repo: &LocalRepository, commit_id: &str) -> Result<Option<CommitStats>, OxenError> {
    match api::local::commits::get_by_id(repo, commit_id) {
        Ok(Some(commit)) => {
            let reader = CommitEntryReader::new(repo, &commit)?;
            Ok(Some(CommitStats {
                commit,
                num_entries: reader.num_entries()?,
                num_synced_files: util::fs::rcount_files_in_dir(&repo.path),
            }))
        },
        Ok(None) => {
            Ok(None)
        },
        Err(err) => {
            log::error!("unable to get commit by id: {}", commit_id);
            Err(err)
        }
    }
}

pub fn list(sync_dir: &Path) -> Result<Vec<LocalRepository>, OxenError> {
    log::debug!(
        "api::local::entries::list repositories for dir: {:?}",
        sync_dir
    );
    let mut repos: Vec<LocalRepository> = vec![];
    for entry in WalkDir::new(&sync_dir).into_iter().filter_map(|e| e.ok()) {
        // if the directory has a .oxen dir, let's add it, otherwise ignore
        let local_dir = entry.path();
        let oxen_dir = util::fs::oxen_hidden_dir(local_dir);
        log::debug!("api::local::entries::list got local dir {:?}", local_dir);

        if oxen_dir.exists() {
            if let Ok(repository) = LocalRepository::from_dir(local_dir) {
                repos.push(repository);
            }
        }
    }

    Ok(repos)
}

pub fn create_empty(sync_dir: &Path, new_repo: &RepositoryNew) -> Result<LocalRepository, OxenError> {
    let repo_dir = sync_dir.join(Path::new(&new_repo.name));
    if repo_dir.exists() {
        let err = format!("Repository already exists {:?}", repo_dir);
        return Err(OxenError::basic_str(&err));
    }

    // Create the repo dir
    log::debug!("create_empty repo dir: {:?}", repo_dir);
    std::fs::create_dir_all(&repo_dir)?;

    // Create oxen hidden dir
    let hidden_dir = util::fs::oxen_hidden_dir(&repo_dir);
    log::debug!("create_empty hidden dir: {:?}", hidden_dir);
    std::fs::create_dir_all(&hidden_dir)?;

    // Create config file
    let config_path = util::fs::config_filepath(&repo_dir);
    let local_repo = LocalRepository::new(&repo_dir)?;
    local_repo.save(&config_path)?;

    // Write the root commit
    let commit_writer = CommitWriter::new(&local_repo)?;
    commit_writer.add_commit_from_empty_status(&new_repo.root_commit)?;

    Ok(local_repo)
}

pub fn delete(sync_dir: &Path, repository: LocalRepository) -> Result<LocalRepository, OxenError> {
    let repo_dir = sync_dir.join(Path::new(&repository.name));
    if !repo_dir.exists() {
        let err = format!("Repository does not exist {:?}", repo_dir);
        return Err(OxenError::basic_str(&err));
    }

    std::fs::remove_dir_all(&repo_dir)?;
    Ok(repository)
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::constants;
    use crate::command;
    use crate::error::OxenError;
    use crate::model::{LocalRepository, RepositoryNew, Commit};
    use chrono::Utc;
    use crate::test;
    use std::path::Path;

    #[test]
    fn test_local_repository_api_create_empty() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let name: &str = "testing";
            let initial_commit_id = format!("{}", uuid::Uuid::new_v4());
            let repo_new = RepositoryNew {
                name: String::from(name),
                root_commit: Commit {
                    id: String::from(initial_commit_id),
                    parent_id: None,
                    message: String::from(constants::INITIAL_COMMIT_MSG),
                    author: String::from("Ox"),
                    date: Utc::now(),
                }
            };
            let repo = api::local::repositories::create_empty(sync_dir, &repo_new)?;

            assert_eq!(repo.name, name);

            let repo_path = Path::new(&sync_dir).join(Path::new(name));
            assert!(repo_path.exists());

            // Test that we can successful load a repository from that dir
            let repo = LocalRepository::from_dir(&repo_path)?;
            assert_eq!(repo.name, name);

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_create_list_one() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let name: &str = "testing";
            let repo_dir = sync_dir.join(name);
            command::init(&repo_dir)?;
            let repos = api::local::repositories::list(sync_dir)?;
            assert_eq!(repos.len(), 1);
            assert_eq!(repos[0].name, name);

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_create_list_multiple() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let _ = command::init(&sync_dir.join("testing1"))?;
            let _ = command::init(&sync_dir.join("testing2"))?;
            let _ = command::init(&sync_dir.join("testing3"))?;

            let repos = api::local::repositories::list(sync_dir)?;
            assert_eq!(repos.len(), 3);

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_get_by_name() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let name = "my-repo";
            let _ = command::init(&sync_dir.join(name))?;
            let repo = api::local::repositories::get_by_name(sync_dir, name)?;
            assert_eq!(repo.name, name);
            Ok(())
        })
    }
}
