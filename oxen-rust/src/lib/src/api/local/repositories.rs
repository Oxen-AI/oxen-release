use crate::api;
use crate::command;
use crate::constants;
use crate::error::OxenError;
use crate::index::{CommitDirReader, CommitWriter, RefWriter};
use crate::model::{CommitStats, LocalRepository, RepositoryNew};
use crate::util;

use jwalk::WalkDir;
use std::path::Path;

pub fn get_by_namespace_and_name(
    sync_dir: &Path,
    namespace: &str,
    name: &str,
) -> Result<Option<LocalRepository>, OxenError> {
    let repo_dir = sync_dir.join(namespace).join(name);

    if !repo_dir.exists() {
        log::debug!("Repo does not exist: {:?}", repo_dir);
        return Ok(None);
    }

    let repo = LocalRepository::from_dir(&repo_dir)?;
    Ok(Some(repo))
}

pub fn get_head_commit_stats(repo: &LocalRepository) -> Result<CommitStats, OxenError> {
    let commit = command::head_commit(repo)?;
    let reader = CommitDirReader::new_from_head(repo)?;
    Ok(CommitStats {
        commit,
        num_entries: reader.num_entries()?,
        num_synced_files: util::fs::rcount_files_in_dir(&repo.path),
    })
}

pub fn get_commit_stats_from_id(
    repo: &LocalRepository,
    commit_id: &str,
) -> Result<Option<CommitStats>, OxenError> {
    match api::local::commits::get_by_id(repo, commit_id) {
        Ok(Some(commit)) => {
            let reader = CommitDirReader::new(repo, &commit)?;
            Ok(Some(CommitStats {
                commit,
                num_entries: reader.num_entries()?,
                num_synced_files: util::fs::rcount_files_in_dir(&repo.path),
            }))
        }
        Ok(None) => Ok(None),
        Err(err) => {
            log::error!("unable to get commit by id: {}", commit_id);
            Err(err)
        }
    }
}

pub fn list_namespaces(sync_dir: &Path) -> Result<Vec<String>, OxenError> {
    log::debug!(
        "api::local::entries::list_namespaces repositories for sync dir: {:?}",
        sync_dir
    );
    let mut namespaces: Vec<String> = vec![];
    for path in std::fs::read_dir(sync_dir)? {
        let path = path.unwrap().path();
        if is_namespace_dir(&path) {
            let name = path.file_name().unwrap().to_str().unwrap();
            namespaces.push(String::from(name));
        }
    }

    Ok(namespaces)
}

fn is_namespace_dir(path: &Path) -> bool {
    if let Some(name) = path.to_str() {
        // Make sure it is a directory, that doesn't start with .oxen and has repositories in it
        return path.is_dir()
            && !name.starts_with(constants::OXEN_HIDDEN_DIR)
            && !list_repos_in_namespace(path).is_empty();
    }
    false
}

pub fn list_repos_in_namespace(namespace_path: &Path) -> Vec<LocalRepository> {
    log::debug!(
        "api::local::entries::list_repos_in_namespace repositories for dir: {:?}",
        namespace_path
    );
    let mut repos: Vec<LocalRepository> = vec![];
    for entry in WalkDir::new(namespace_path)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        // if the directory has a .oxen dir, let's add it, otherwise ignore
        let local_dir = entry.path();
        let oxen_dir = util::fs::oxen_hidden_dir(&local_dir);
        log::debug!(
            "api::local::entries::list_repos_in_namespace got local dir {:?}",
            local_dir
        );

        if oxen_dir.exists() {
            if let Ok(repository) = LocalRepository::from_dir(&local_dir) {
                repos.push(repository);
            }
        }
    }

    repos
}

pub fn create_empty(
    sync_dir: &Path,
    new_repo: &RepositoryNew,
) -> Result<LocalRepository, OxenError> {
    let repo_dir = sync_dir
        .join(&new_repo.namespace)
        .join(Path::new(&new_repo.name));
    if repo_dir.exists() {
        let err = format!("Repository already exists {:?}", repo_dir);
        return Err(OxenError::basic_str(err));
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

    // Create HEAD file and point it to DEFAULT_BRANCH_NAME
    {
        // Make go out of scope to release LOCK
        log::debug!("create_empty BEFORE ref writer: {:?}", local_repo.path);
        let ref_writer = RefWriter::new(&local_repo)?;
        ref_writer.set_head(constants::DEFAULT_BRANCH_NAME);
        log::debug!("create_empty AFTER ref writer: {:?}", local_repo.path);
    }

    if let Some(root_commit) = &new_repo.root_commit {
        // Write the root commit
        let commit_writer = CommitWriter::new(&local_repo)?;
        commit_writer.add_commit_from_empty_status(root_commit)?;
    }

    Ok(local_repo)
}

pub fn delete(repo: LocalRepository) -> Result<LocalRepository, OxenError> {
    if !repo.path.exists() {
        let err = format!("Repository does not exist {:?}", repo.path);
        return Err(OxenError::basic_str(err));
    }

    std::fs::remove_dir_all(&repo.path)?;
    Ok(repo)
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::error::OxenError;
    use crate::model::{Commit, LocalRepository, RepositoryNew};
    use crate::test;
    use std::path::Path;
    use time::OffsetDateTime;

    #[test]
    fn test_local_repository_api_create_empty_with_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace: &str = "test-namespace";
            let name: &str = "test-repo-name";
            let initial_commit_id = format!("{}", uuid::Uuid::new_v4());
            let timestamp = OffsetDateTime::now_utc();
            let repo_new = RepositoryNew {
                namespace: String::from(namespace),
                name: String::from(name),
                root_commit: Some(Commit {
                    id: initial_commit_id,
                    parent_ids: vec![],
                    message: String::from(constants::INITIAL_COMMIT_MSG),
                    author: String::from("Ox"),
                    email: String::from("ox@oxen.ai"),
                    timestamp,
                }),
            };
            let _repo = api::local::repositories::create_empty(sync_dir, &repo_new)?;

            let repo_path = Path::new(&sync_dir)
                .join(Path::new(namespace))
                .join(Path::new(name));
            assert!(repo_path.exists());

            // Test that we can successful load a repository from that dir
            let _repo = LocalRepository::from_dir(&repo_path)?;

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_create_empty_no_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace: &str = "test-namespace";
            let name: &str = "test-repo-name";
            let repo_new = RepositoryNew {
                namespace: String::from(namespace),
                name: String::from(name),
                root_commit: None,
            };
            let _repo = api::local::repositories::create_empty(sync_dir, &repo_new)?;

            let repo_path = Path::new(&sync_dir)
                .join(Path::new(namespace))
                .join(Path::new(name));
            assert!(repo_path.exists());

            // Test that we can successful load a repository from that dir
            let _repo = LocalRepository::from_dir(&repo_path)?;

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_list_namespaces_one() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace: &str = "test-namespace";
            let name: &str = "cool-repo";

            let namespace_dir = sync_dir.join(namespace);
            std::fs::create_dir_all(&namespace_dir)?;
            let repo_dir = namespace_dir.join(name);
            command::init(&repo_dir)?;

            let namespaces = api::local::repositories::list_namespaces(sync_dir)?;
            assert_eq!(namespaces.len(), 1);
            assert_eq!(namespaces[0], namespace);

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_list_multiple_namespaces() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace_1 = "my-namespace-1";
            let namespace_1_dir = sync_dir.join(namespace_1);

            let namespace_2 = "my-namespace-2";
            let namespace_2_dir = sync_dir.join(namespace_2);

            // We will not create any repos in the last namespace, to test that it gets filtered out
            let namespace_3 = "my-namespace-3";
            let _ = sync_dir.join(namespace_3);

            let _ = command::init(&namespace_1_dir.join("testing1"))?;
            let _ = command::init(&namespace_1_dir.join("testing2"))?;
            let _ = command::init(&namespace_2_dir.join("testing3"))?;

            let repos = api::local::repositories::list_namespaces(sync_dir)?;
            assert_eq!(repos.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_list_multiple_within_namespace() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace = "my-namespace";
            let namespace_dir = sync_dir.join(namespace);

            let _ = command::init(&namespace_dir.join("testing1"))?;
            let _ = command::init(&namespace_dir.join("testing2"))?;
            let _ = command::init(&namespace_dir.join("testing3"))?;

            let repos = api::local::repositories::list_repos_in_namespace(&namespace_dir);
            assert_eq!(repos.len(), 3);

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_get_by_name() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace = "my-namespace";
            let name = "my-repo";
            let repo_dir = sync_dir.join(namespace).join(name);
            std::fs::create_dir_all(&repo_dir)?;

            let _ = command::init(&repo_dir)?;
            let _repo =
                api::local::repositories::get_by_namespace_and_name(sync_dir, namespace, name)?
                    .unwrap();
            Ok(())
        })
    }
}
