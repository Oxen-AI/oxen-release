
use crate::model::{Repository, RepositoryResponse, RepositoryNew, ListRepositoriesResponse};
use crate::cli::{Indexer};
use crate::api;
use crate::error::OxenError;
use crate::util::FileUtil;
use crate::cli::indexer::OXEN_HIDDEN_DIR;
use crate::model::http_response::{
    STATUS_SUCCESS,
    MSG_RESOURCE_CREATED,
    MSG_RESOURCE_FOUND,
    MSG_RESOURCE_ALREADY_EXISTS
};

use std::path::{Path, PathBuf};
use walkdir::WalkDir;

pub struct RepositoryAPI {
    sync_dir: PathBuf
}

impl RepositoryAPI {
    pub fn new(path: &Path) -> RepositoryAPI {
        RepositoryAPI {
            sync_dir: path.to_path_buf()
        }
    }

    fn get_sync_dir(&self) -> Result<PathBuf, OxenError> {
        let sync_dir = Path::new(&self.sync_dir);
        if !sync_dir.exists() {
            std::fs::create_dir_all(&sync_dir)?;
        }
        Ok(PathBuf::from(sync_dir))
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

                let name = FileUtil::path_relative_to_dir(&local_path, &sync_dir)?;
                if let Some(name) = name.to_str() {
                    let url = api::endpoint::url_from(&name);
                    repos.push(Repository {
                        id: id,
                        name: name.to_string(),
                        url: url.clone(),
                    });
                }
            }
        }

        Ok(ListRepositoriesResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_FOUND),
            repositories: repos
        })
    }

    pub fn create(&self, repo: &RepositoryNew) -> Result<RepositoryResponse, OxenError> {
        let id = format!("{}", uuid::Uuid::new_v4());
        let url = api::endpoint::url_from(&repo.name);

        let sync_dir = self.get_sync_dir()?;
        let repo_dir = sync_dir.join(Path::new(&repo.name));
        if repo_dir.exists() {
            return Err(OxenError::basic_str(MSG_RESOURCE_ALREADY_EXISTS));
        }

        std::fs::create_dir_all(&repo_dir)?;
        let indexer = Indexer::new(&repo_dir);
        indexer.init()?;

        let repository = Repository {
            id: id,
            name: String::from(&repo.name),
            url: url,
        };
        Ok(RepositoryResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_CREATED),
            repository: repository
        })
    }

}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::model::RepositoryNew;
    use crate::api::local::RepositoryAPI;
    use crate::test;
    use std::fs;
    use std::path::Path;
    use crate::model::http_response::{MSG_RESOURCE_ALREADY_EXISTS};

    fn get_sync_dir() -> String {
        format!("/tmp/oxen/test_sync_dir/{}", uuid::Uuid::new_v4())
    }

    #[test]
    fn test_1_create_repository() -> Result<(), OxenError> {
        let sync_dir = get_sync_dir();
        test::setup_env();

        let name: &str = "testing";
        let repo = RepositoryNew {
            name: String::from(name)
        };
        let api = RepositoryAPI::new(Path::new(&sync_dir));
        let response = api.create(&repo)?;
        assert_eq!(response.repository.name, name);

        let repo_path = Path::new(&sync_dir).join(Path::new(name));
        assert_eq!(repo_path.exists(), true);

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
            name: String::from(name)
        };
        let api = RepositoryAPI::new(Path::new(&sync_dir));
        let response = api.create(&repo)?;
        assert_eq!(response.repository.name, name);

        let repo_path = Path::new(&sync_dir).join(Path::new(name));
        assert_eq!(repo_path.exists(), true);

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
            name: String::from(name)
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
            name: String::from(name)
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
            name: String::from(name)
        };
        let api = RepositoryAPI::new(Path::new(&sync_dir));
        let response = api.create(&repo)?;
        assert_eq!(response.repository.name, name);

        match api.create(&repo) {
            Ok(_) => {
                panic!("Do not allow creation of same repo twice")
            },
            Err(err) => {
                let msg = format!("\"{}\"", MSG_RESOURCE_ALREADY_EXISTS);
                assert_eq!(err.to_string(), msg);
            }
        };

        // cleanup
        fs::remove_dir_all(sync_dir)?;
        Ok(())
    }
}