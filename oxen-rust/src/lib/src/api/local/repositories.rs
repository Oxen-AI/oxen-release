use crate::index::indexer::OXEN_HIDDEN_DIR;
use crate::index::{Indexer, Committer};
use crate::error::OxenError;
use crate::http::{
    MSG_RESOURCE_ALREADY_EXISTS, MSG_RESOURCE_CREATED, MSG_RESOURCE_FOUND, STATUS_SUCCESS,
};

use crate::http::response::{
    ListRepositoriesResponse,
    RepositoryHeadResponse,
    RepositoryResponse
};

use crate::model::{
    CommitHead,
    CommmitSyncInfo,
    Repository,
    RepositoryNew,
};

use crate::util::FileUtil;

use std::path::{Path, PathBuf};
use walkdir::WalkDir;

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

    pub fn get_by_path(&self, path: &Path) -> Result<RepositoryHeadResponse, OxenError> {
        let sync_dir = self.get_sync_dir()?;
        let repo_path = sync_dir.join(path);

        if !repo_path.exists() {
            let err = format!("Repo does not exist: {:?}", repo_path);
            return Err(OxenError::basic_str(&err));
        }

        let repo = Repository::from(&repo_path);
        let commit_head: Option<CommitHead> = self.get_commit_head(&repo_path)?;

        Ok(RepositoryHeadResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_FOUND),
            repository: repo,
            head: commit_head,
        })
    }

    pub fn get_commit_head(&self, repo_path: &Path) -> Result<Option<CommitHead>, OxenError> {
        match Committer::new(&repo_path) {
            Ok(committer) => {
                match committer.referencer.head_commit_id() {
                    Ok(commit_id) => {
                        Ok(Some(CommitHead {
                            commit_id: commit_id.clone(),
                            name: committer.referencer.read_head()?,
                            sync_info: CommmitSyncInfo {
                                num_entries: committer.get_num_entries_in_head()?,
                                num_synced_files: committer.count_files_from_dir(&repo_path),
                            }
                        }))
                    },
                    Err(_) => Ok(None),
                }
            },
            Err(_) => Ok(None),
        }
    }

    pub fn list(&self) -> Result<ListRepositoriesResponse, OxenError> {
        let mut repos: Vec<Repository> = vec![];
        let sync_dir = self.get_sync_dir()?;
        for entry in WalkDir::new(&sync_dir).into_iter().filter_map(|e| e.ok()) {
            let local_path = entry.path();
            let oxen_dir = local_path.join(Path::new(OXEN_HIDDEN_DIR));

            if oxen_dir.exists() {
                // TODO: get actual ID, and loop until the oxen dir
                let id = format!("{}", uuid::Uuid::new_v4());

                let name = FileUtil::path_relative_to_dir(local_path, &sync_dir)?;
                if let Some(name) = name.to_str() {
                    repos.push(Repository {
                        id,
                        name: name.to_string(),
                        url: String::from(""),
                    });
                }
            }
        }

        Ok(ListRepositoriesResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_FOUND),
            repositories: repos,
        })
    }

    pub fn create(&self, repo: &RepositoryNew) -> Result<RepositoryResponse, OxenError> {
        let id = format!("{}", uuid::Uuid::new_v4());

        let sync_dir = self.get_sync_dir()?;
        let repo_dir = sync_dir.join(Path::new(&repo.name));
        if repo_dir.exists() {
            return Err(OxenError::basic_str(MSG_RESOURCE_ALREADY_EXISTS));
        }

        std::fs::create_dir_all(&repo_dir)?;
        let indexer = Indexer::new(&repo_dir);
        indexer.init_with_name(&repo.name)?;

        let repository = Repository {
            id,
            name: String::from(&repo.name),
            url: String::from(""), // no remote to start
        };
        Ok(RepositoryResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_CREATED),
            repository,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::api::local::RepositoryAPI;
    use crate::error::OxenError;
    use crate::http::MSG_RESOURCE_ALREADY_EXISTS;
    use crate::model::RepositoryNew;
    use crate::test;
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

        let name: &str = "gschoeni/CatsVsDogs";
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

        let name: &str = "gschoeni/CatsVsDogs";
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

        let name: &str = "gschoeni/CatsVsDogs";
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
        let sync_dir = get_sync_dir();
        test::setup_env();

        let name: &str = "testing/My-Repo";
        let repo = RepositoryNew {
            name: String::from(name),
        };

        let api = RepositoryAPI::new(Path::new(&sync_dir));
        api.create(&repo)?;

        let response = api.get_by_path(Path::new(name))?;
        assert_eq!(response.repository.name, name);

        // cleanup
        fs::remove_dir_all(sync_dir)?;
        Ok(())
    }
}
