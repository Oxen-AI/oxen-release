use crate::command;
use crate::error::OxenError;
use crate::index::Committer;
use crate::model::{CommitHead, CommmitSyncInfo, LocalRepository};
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

pub fn get_commit_head(repo: &LocalRepository) -> Result<Option<CommitHead>, OxenError> {
    match Committer::new(repo) {
        Ok(committer) => match committer.referencer.head_commit_id() {
            Ok(commit_id) => Ok(Some(CommitHead {
                commit_id,
                name: committer.referencer.read_head_ref()?,
                sync_info: CommmitSyncInfo {
                    num_entries: committer.get_num_entries_in_head()?,
                    num_synced_files: util::fs::rcount_files_in_dir(&repo.path),
                },
            })),
            Err(_) => Ok(None),
        },
        Err(_) => Ok(None),
    }
}

pub fn list(sync_dir: &Path) -> Result<Vec<LocalRepository>, OxenError> {
    let mut repos: Vec<LocalRepository> = vec![];
    for entry in WalkDir::new(&sync_dir).into_iter().filter_map(|e| e.ok()) {
        // if the directory has a .oxen dir, let's add it, otherwise ignore
        let local_dir = entry.path();
        let oxen_dir = util::fs::oxen_hidden_dir(local_dir);

        if oxen_dir.exists() {
            let repository = LocalRepository::from_dir(local_dir)?;
            repos.push(repository);
        }
    }

    Ok(repos)
}

pub fn create(sync_dir: &Path, name: &str) -> Result<LocalRepository, OxenError> {
    let repo_dir = sync_dir.join(Path::new(name));
    if repo_dir.exists() {
        let err = format!("Repository already exists {:?}", repo_dir);
        return Err(OxenError::basic_str(&err));
    }

    std::fs::create_dir_all(&repo_dir)?;
    let repository = command::init(&repo_dir)?;
    Ok(repository)
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
    use crate::error::OxenError;
    use crate::model::LocalRepository;
    use crate::test;
    use std::path::Path;

    #[test]
    fn test_local_repository_api_create() -> Result<(), OxenError> {
        test::run_empty_repo_dir_test(|sync_dir| {
            let name: &str = "testing";
            let repo = api::local::repositories::create(sync_dir, name)?;

            assert_eq!(repo.name, name);

            let repo_path = Path::new(&sync_dir).join(Path::new(name));
            assert!(repo_path.exists());

            // Test that we can successfull load a repository from that dir
            let repo = LocalRepository::from_dir(&repo_path)?;
            assert_eq!(repo.name, name);

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_create_list_one() -> Result<(), OxenError> {
        test::run_empty_repo_dir_test(|sync_dir| {
            let name: &str = "testing";
            let _ = api::local::repositories::create(sync_dir, name)?;
            let repos = api::local::repositories::list(sync_dir)?;
            assert_eq!(repos.len(), 1);
            assert_eq!(repos[0].name, name);

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_create_list_multiple() -> Result<(), OxenError> {
        test::run_empty_repo_dir_test(|sync_dir| {
            let _ = api::local::repositories::create(sync_dir, "testing1")?;
            let _ = api::local::repositories::create(sync_dir, "testing2")?;
            let _ = api::local::repositories::create(sync_dir, "testing3")?;

            let repos = api::local::repositories::list(sync_dir)?;
            assert_eq!(repos.len(), 3);

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_cannot_create_name_twice() -> Result<(), OxenError> {
        test::run_empty_repo_dir_test(|sync_dir| {
            let name: &str = "CatsVsDogs";
            // first time is okay
            let _ = api::local::repositories::create(sync_dir, name)?;

            // Second time should throw error
            match api::local::repositories::create(sync_dir, name) {
                Ok(_) => {
                    panic!("Do not allow creation of same repo twice")
                }
                Err(_err) => {
                    // What we want
                }
            };

            Ok(())
        })
    }

    #[test]
    fn test_local_repository_api_get_by_name() -> Result<(), OxenError> {
        test::run_empty_repo_dir_test(|sync_dir| {
            let name = "my-repo";
            let _ = api::local::repositories::create(sync_dir, name)?;
            let repo = api::local::repositories::get_by_name(sync_dir, name)?;
            assert_eq!(repo.name, name);
            Ok(())
        })
    }
}
