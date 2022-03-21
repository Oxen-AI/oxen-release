use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::Repository;
use crate::model::User;
use crate::util::file_util::FileUtil;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub struct RepoConfig {
    pub host: String,
    pub repository: Repository,
    pub user: User,
}

impl<'a> HTTPConfig<'a> for RepoConfig {
    fn host(&'a self) -> &'a str {
        &self.host
    }

    fn auth_token(&'a self) -> &'a str {
        &self.user.token
    }
}

impl RepoConfig {
    pub fn from(path: &Path) -> RepoConfig {
        let contents = FileUtil::read_from_path(path);
        toml::from_str(&contents).unwrap()
    }

    pub fn new(config: &AuthConfig, repository: &Repository) -> RepoConfig {
        RepoConfig {
            host: config.host.clone(),
            repository: repository.clone(),
            user: config.user.clone(),
        }
    }

    pub fn to_auth(&self) -> AuthConfig {
        AuthConfig {
            host: self.host.clone(),
            user: self.user.clone(),
        }
    }

    pub fn save(&self, path: &Path) -> Result<(), OxenError> {
        let toml = toml::to_string(&self)?;
        FileUtil::write_to_path(path, &toml);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::config::{RepoConfig, HTTPConfig};
    use crate::error::OxenError;
    use crate::test;

    use std::path::Path;

    #[test]
    fn test_read_cfg() {
        let path = test::repo_cfg_file();
        let config = RepoConfig::from(path);
        assert_eq!(config.host(), "localhost:4000");
    }

    #[test]
    fn test_create_repo_cfg() -> Result<(), OxenError> {
        let name: &str = "Test Repo";
        let cfg = test::create_repo_cfg(name)?;
        assert_eq!(cfg.repository.name, name);
        // cleanup
        api::repositories::delete(&cfg, &cfg.repository)?;
        Ok(())
    }

    #[test]
    fn test_save() -> Result<(), OxenError> {
        let final_path = Path::new("/tmp/repo_config.toml");
        let orig_config = RepoConfig::from(test::repo_cfg_file());

        orig_config.save(final_path)?;

        let config = RepoConfig::from(final_path);
        assert_eq!(config.user.name, "Greg");
        assert_eq!(config.repository.name, "Test Repo");
        Ok(())
    }
}
