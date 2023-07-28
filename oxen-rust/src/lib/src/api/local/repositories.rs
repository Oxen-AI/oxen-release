use crate::api;
use crate::constants;
use crate::core::index::{CommitEntryReader, CommitWriter, RefWriter};
use crate::error::OxenError;
use crate::model::DataTypeStat;
use crate::model::EntryDataType;
use crate::model::RepoStats;
use crate::model::{CommitStats, LocalRepository, RepositoryNew};
use crate::util;

use jwalk::WalkDir;
use std::collections::HashMap;
use std::path::Path;

pub fn get_by_namespace_and_name(
    sync_dir: &Path,
    namespace: impl AsRef<str>,
    name: impl AsRef<str>,
) -> Result<Option<LocalRepository>, OxenError> {
    let namespace = namespace.as_ref();
    let name = name.as_ref();
    let repo_dir = sync_dir.join(namespace).join(name);

    if !repo_dir.exists() {
        log::debug!("Repo does not exist: {:?}", repo_dir);
        return Ok(None);
    }

    let repo = LocalRepository::from_dir(&repo_dir)?;
    Ok(Some(repo))
}

pub fn get_commit_stats_from_id(
    repo: &LocalRepository,
    commit_id: &str,
) -> Result<Option<CommitStats>, OxenError> {
    match api::local::commits::get_by_id(repo, commit_id) {
        Ok(Some(commit)) => {
            let reader = CommitEntryReader::new(repo, &commit)?;
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

pub fn get_repo_stats(repo: &LocalRepository) -> RepoStats {
    let mut data_size: u64 = 0;
    let mut data_types: HashMap<EntryDataType, DataTypeStat> = HashMap::new();

    match api::local::commits::head_commit(repo) {
        Ok(commit) => match api::local::entries::list_all(repo, &commit) {
            Ok(entries) => {
                for entry in entries {
                    data_size += entry.num_bytes;
                    let full_path = repo.path.join(&entry.path);
                    let data_type = util::fs::file_data_type(&full_path);
                    let data_type_stat = DataTypeStat {
                        data_size: entry.num_bytes,
                        data_type: data_type.to_owned(),
                        file_count: 1,
                    };
                    let stat = data_types.entry(data_type).or_insert(data_type_stat);
                    stat.file_count += 1;
                    stat.data_size += entry.num_bytes;
                }
            }
            Err(err) => {
                log::error!("Err: could not list entries for repo stats {err}");
            }
        },
        Err(err) => {
            log::error!("Err: could not get repo stats {err}");
        }
    }

    RepoStats {
        data_size,
        data_types,
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

pub fn transfer_namespace(
    sync_dir: &Path,
    repo_name: &str,
    from_namespace: &str,
    to_namespace: &str,
) -> Result<LocalRepository, OxenError> {
    log::debug!(
        "transfer_namespace from: {} to: {}",
        from_namespace,
        to_namespace
    );

    let repo_dir = sync_dir.join(from_namespace).join(repo_name);
    let new_repo_dir = sync_dir.join(to_namespace).join(repo_name);

    if !repo_dir.exists() {
        log::debug!("Repo does not exist: {:?}", repo_dir);
        return Err(OxenError::repo_not_found(RepositoryNew::new(
            from_namespace,
            repo_name,
        )));
    }

    std::fs::create_dir_all(&new_repo_dir)?;

    std::fs::rename(&repo_dir, &new_repo_dir)?;

    // Update remote
    let updated_repo = get_by_namespace_and_name(sync_dir, to_namespace, repo_name)?;

    match updated_repo {
        Some(new_repo) => Ok(new_repo),
        None => Err(OxenError::basic_str(
            "Repository not successfully transferred",
        )),
    }
}

pub fn create_empty(
    sync_dir: &Path,
    new_repo: RepositoryNew,
) -> Result<LocalRepository, OxenError> {
    let repo_dir = sync_dir
        .join(&new_repo.namespace)
        .join(Path::new(&new_repo.name));
    if repo_dir.exists() {
        log::error!("Repository already exists {repo_dir:?}");
        return Err(OxenError::repo_already_exists(new_repo));
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

    // Create history dir
    let history_dir = util::fs::oxen_hidden_dir(&repo_dir).join(constants::HISTORY_DIR);
    std::fs::create_dir_all(history_dir)?;

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

    log::debug!("Deleting repo directory: {:?}", repo);
    util::fs::remove_dir_all(&repo.path)?;
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
            let _repo = api::local::repositories::create_empty(sync_dir, repo_new)?;

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
            let _repo = api::local::repositories::create_empty(sync_dir, repo_new)?;

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

    #[test]
    fn test_local_repository_transfer_namespace() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let old_namespace: &str = "test-namespace-old";
            let new_namespace: &str = "test-namespace-new";

            let old_namespace_dir = sync_dir.join(old_namespace);
            let new_namespace_dir = sync_dir.join(new_namespace);

            let name = "moving-repo";

            let initial_commit_id = format!("{}", uuid::Uuid::new_v4());
            let timestamp = OffsetDateTime::now_utc();
            // Create new namespace
            std::fs::create_dir_all(&new_namespace_dir)?;

            let repo_new = RepositoryNew {
                namespace: String::from(old_namespace),
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
            let _repo = api::local::repositories::create_empty(sync_dir, repo_new)?;

            let old_namespace_repos =
                api::local::repositories::list_repos_in_namespace(&old_namespace_dir);
            assert_eq!(old_namespace_repos.len(), 1);

            // Transfer to new namespace
            api::local::repositories::transfer_namespace(
                sync_dir,
                name,
                old_namespace,
                new_namespace,
            )?;

            // Check that the old namespace is empty
            let old_namespace_repos =
                api::local::repositories::list_repos_in_namespace(&old_namespace_dir);
            let new_namespace_repos =
                api::local::repositories::list_repos_in_namespace(&new_namespace_dir);

            assert_eq!(old_namespace_repos.len(), 0);
            assert_eq!(new_namespace_repos.len(), 1);

            Ok(())
        })
    }
}
