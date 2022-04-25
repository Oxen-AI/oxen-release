use crate::command;
use crate::error::OxenError;
use crate::index::Committer;
use crate::model::{CommitHead, CommmitSyncInfo, LocalRepository, RemoteRepository};
use crate::util;
use crate::view::http::{
    MSG_RESOURCE_ALREADY_EXISTS, MSG_RESOURCE_CREATED, MSG_RESOURCE_FOUND, STATUS_SUCCESS,
};
use crate::view::{
    ListRepositoryResponse, RemoteRepositoryHeadResponse, RepositoryNew, RepositoryResponse,
    RepositoryView,
};

use std::path::{Path, PathBuf};
use walkdir::WalkDir;

// TODO: do we need this sync dir here? Or can we pass in somehow else? Would be nice if local and remote APIs were the same
pub struct RepositoryAPI {
    sync_dir: PathBuf,
}

impl RepositoryAPI {
    pub fn new(path: &Path) -> RepositoryAPI {
        RepositoryAPI {
            sync_dir: path.to_path_buf(),
        }
    }

    fn get_sync_dir(&self) -> Result<PathBuf, OxenError> {
        let sync_dir = Path::new(&self.sync_dir);
        if !sync_dir.exists() {
            std::fs::create_dir_all(&sync_dir)?;
        }
        Ok(PathBuf::from(sync_dir))
    }

    pub fn get_by_path(&self, path: &Path) -> Result<RemoteRepositoryHeadResponse, OxenError> {
        let sync_dir = self.get_sync_dir()?;
        let repo_dir = sync_dir.join(path);

        if !repo_dir.exists() {
            let err = format!("Repo does not exist: {:?}", repo_dir);
            return Err(OxenError::basic_str(&err));
        }

        let repo = LocalRepository::from_dir(&repo_dir)?;
        let commit_head: Option<CommitHead> = self.get_commit_head(&repo)?;

        Ok(RemoteRepositoryHeadResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_FOUND),
            repository: RemoteRepository::from_local(&repo)?,
            head: commit_head,
        })
    }

    pub fn get_commit_head(&self, repo: &LocalRepository) -> Result<Option<CommitHead>, OxenError> {
        match Committer::new(repo) {
            Ok(committer) => match committer.referencer.head_commit_id() {
                Ok(commit_id) => Ok(Some(CommitHead {
                    commit_id,
                    name: committer.referencer.read_head()?,
                    sync_info: CommmitSyncInfo {
                        num_entries: committer.get_num_entries_in_head()?,
                        num_synced_files: committer.count_files_from_dir(&repo.path),
                    },
                })),
                Err(_) => Ok(None),
            },
            Err(_) => Ok(None),
        }
    }

    pub fn list(&self) -> Result<ListRepositoryResponse, OxenError> {
        let mut repos: Vec<RepositoryView> = vec![];
        let sync_dir = self.get_sync_dir()?;
        for entry in WalkDir::new(&sync_dir).into_iter().filter_map(|e| e.ok()) {
            // if the directory has a .oxen dir, let's add it, otherwise ignore
            let local_dir = entry.path();
            let oxen_dir = util::fs::oxen_hidden_dir(local_dir);

            if oxen_dir.exists() {
                let repository = LocalRepository::from_dir(local_dir)?;
                repos.push(RepositoryView::from_local(repository));
            }
        }

        Ok(ListRepositoryResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_FOUND),
            repositories: repos,
        })
    }

    pub fn create(&self, repo: &RepositoryNew) -> Result<RepositoryResponse, OxenError> {
        let sync_dir = self.get_sync_dir()?;
        let repo_dir = sync_dir.join(Path::new(&repo.name));
        if repo_dir.exists() {
            return Err(OxenError::basic_str(MSG_RESOURCE_ALREADY_EXISTS));
        }

        std::fs::create_dir_all(&repo_dir)?;
        let repository = command::init(&repo_dir)?;
        Ok(RepositoryResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_CREATED),
            repository: RepositoryView::from_local(repository),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::api::local::RepositoryAPI;
    use crate::error::OxenError;
    use crate::test;
    use crate::view::http::MSG_RESOURCE_ALREADY_EXISTS;
    use crate::view::RepositoryNew;
    use std::fs;
    use std::path::Path;

    fn get_sync_dir() -> String {
        format!("/tmp/oxen/test_sync_dir/{}", uuid::Uuid::new_v4())
    }

    #[test]
    fn test_1_create_repository() -> Result<(), OxenError> {
        let sync_dir = get_sync_dir();
        test::setup_env();

        let name: &str = "testing";
        let repo = RepositoryNew {
            name: String::from(name),
        };
        let api = RepositoryAPI::new(Path::new(&sync_dir));
        let response = api.create(&repo)?;
        assert_eq!(response.repository.name, name);

        let repo_path = Path::new(&sync_dir).join(Path::new(name));
        assert!(repo_path.exists());

        // TODO: test that we can load a repository config from that dir

        // cleanup
        fs::remove_dir_all(sync_dir)?;
        Ok(())
    }

    #[test]
    fn test_2_create_repository_path() -> Result<(), OxenError> {
        let sync_dir = get_sync_dir();
        test::setup_env();

        let name: &str = "CatsVsDogs";
        let repo = RepositoryNew {
            name: String::from(name),
        };
        let api = RepositoryAPI::new(Path::new(&sync_dir));
        let response = api.create(&repo)?;
        assert_eq!(response.repository.name, name);

        let repo_path = Path::new(&sync_dir).join(Path::new(name));
        assert!(repo_path.exists());

        // cleanup
        fs::remove_dir_all(sync_dir)?;
        Ok(())
    }

    #[test]
    fn test_3_create_list_repository() -> Result<(), OxenError> {
        let sync_dir = get_sync_dir();
        test::setup_env();

        let name: &str = "testing";
        let repo = RepositoryNew {
            name: String::from(name),
        };

        let api = RepositoryAPI::new(Path::new(&sync_dir));
        let response = api.create(&repo)?;
        assert_eq!(response.repository.name, name);

        let api = RepositoryAPI::new(Path::new(&sync_dir));
        let response = api.list()?;
        assert_eq!(response.repositories.len(), 1);
        assert_eq!(response.repositories[0].name, name);

        // cleanup
        fs::remove_dir_all(sync_dir)?;
        Ok(())
    }

    #[test]
    fn test_4_create_multidir_list_repository() -> Result<(), OxenError> {
        let sync_dir = get_sync_dir();
        test::setup_env();

        let name: &str = "CatsVsDogs";
        let repo = RepositoryNew {
            name: String::from(name),
        };
        let api = RepositoryAPI::new(Path::new(&sync_dir));
        let response = api.create(&repo)?;
        assert_eq!(response.repository.name, name);

        let response = api.list()?;
        assert_eq!(response.repositories.len(), 1);
        assert_eq!(response.repositories[0].name, name);

        // cleanup
        fs::remove_dir_all(sync_dir)?;
        Ok(())
    }

    #[test]
    fn test_5_cannot_create_repository_twice() -> Result<(), OxenError> {
        let sync_dir = get_sync_dir();
        test::setup_env();

        let name: &str = "CatsVsDogs";
        let repo = RepositoryNew {
            name: String::from(name),
        };
        let api = RepositoryAPI::new(Path::new(&sync_dir));
        let response = api.create(&repo)?;
        assert_eq!(response.repository.name, name);

        match api.create(&repo) {
            Ok(_) => {
                panic!("Do not allow creation of same repo twice")
            }
            Err(err) => {
                let msg = format!("\"{}\"", MSG_RESOURCE_ALREADY_EXISTS);
                assert_eq!(err.to_string(), msg);
            }
        };

        // cleanup
        fs::remove_dir_all(sync_dir)?;
        Ok(())
    }

    #[test]
    fn test_6_create_get_repository_by_path() -> Result<(), OxenError> {
        // TODO: create test function to create/cleanup sync dir

        test::run_empty_repo_test(|_repo| {
            // let sync_dir = get_sync_dir();
            // let api = RepositoryAPI::new(sync_dir);
            // let response = api.get_by_path(Path::new(name))?;
            // assert_eq!(response.repository.name, name);
            Ok(())
        })
    }
}
