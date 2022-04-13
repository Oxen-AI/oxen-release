use crate::api;
use crate::cli::indexer::{OXEN_HIDDEN_DIR, REPO_CONFIG_FILE};
use crate::config::{AuthConfig, RepoConfig};
use crate::error::OxenError;
use http::Uri;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Repository {
    pub id: String,
    pub name: String,
    pub url: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RepositoryNew {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RepositoryResponse {
    pub status: String,
    pub status_message: String,
    pub repository: Repository,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListRepositoriesResponse {
    pub status: String,
    pub status_message: String,
    pub repositories: Vec<Repository>,
}

impl Repository {
    pub fn from(repo_dir: &Path) -> Repository {
        let config_file = repo_dir.join(OXEN_HIDDEN_DIR).join(REPO_CONFIG_FILE);
        let config = RepoConfig::new(&config_file);
        config.repository
    }

    pub fn clone_remote(config: &AuthConfig, url: &str) -> Result<RepoConfig, OxenError> {
        match api::repositories::get_by_url(config, url) {
            Ok(repository) => Repository::clone_repo(config, &repository),
            Err(_) => {
                let err = format!("Could not clone remote {} not found", url);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    fn clone_repo(config: &AuthConfig, repo: &Repository) -> Result<RepoConfig, OxenError> {
        // get last part of URL for directory name
        let dir_name = Repository::dirname_from_url(&repo.url)?;

        // if directory already exists -> return Err
        let repo_path = Path::new(&dir_name);
        if repo_path.exists() {
            let err = format!("Directory already exists: {}", dir_name);
            return Err(OxenError::basic_str(&err));
        }

        // if directory does not exist, create it
        std::fs::create_dir(&repo_path)?;

        // if create successful, create .oxen directory
        let oxen_hidden_path = repo_path.join(Path::new(OXEN_HIDDEN_DIR));
        std::fs::create_dir(&oxen_hidden_path)?;

        // save RepoConfig in .oxen directory
        let repo_config_file = oxen_hidden_path.join(Path::new("config.toml"));
        let repo_config = RepoConfig::from(config, repo);
        repo_config.save(&repo_config_file)?;

        println!(
            "🐂 cloned {} to {}\n\ncd {}\noxen pull",
            repo.url, dir_name, dir_name
        );

        Ok(repo_config)
    }

    pub fn dirname_from_url(url: &str) -> Result<String, OxenError> {
        let uri = url.parse::<Uri>()?;
        if let Some(dirname) = uri.path().split('/').last() {
            Ok(String::from(dirname))
        } else {
            Err(OxenError::basic_str(""))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::config::AuthConfig;
    use crate::error::OxenError;
    use crate::model::Repository;
    use crate::test;

    use std::path::Path;

    #[test]
    fn test_get_dirname_from_url() -> Result<(), OxenError> {
        let url = "http://localhost:4000/gschoeni/OxenData";
        let dirname = Repository::dirname_from_url(url)?;
        assert_eq!(dirname, "OxenData");
        Ok(())
    }

    #[test]
    fn test_clone_remote() -> Result<(), OxenError> {
        let name = "OxenDataTest";
        let config = AuthConfig::new(test::auth_cfg_file());
        let repository = api::repositories::create(&config, name)?;
        let url = repository.url;

        let auth_config = AuthConfig::new(test::auth_cfg_file());
        let repo_config = Repository::clone_remote(&auth_config, &url)?;

        let cfg_path = format!("{}/.oxen/config.toml", name);
        let path = Path::new(&cfg_path);
        assert!(path.exists());
        assert_eq!(repo_config.repository.name, repository.name);
        assert_eq!(repo_config.repository.id, repository.id);

        // cleanup
        api::repositories::delete(&repo_config, &repo_config.repository)?;
        std::fs::remove_dir_all(name)?;

        Ok(())
    }
}
