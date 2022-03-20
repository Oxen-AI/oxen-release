use crate::config::AuthConfig;
use crate::model::Repository;
use crate::model::User;
use crate::util::file_util::FileUtil;
use crate::error::OxenError;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub struct RepoConfig {
    pub host: String,
    pub repository: Repository,
    pub user: User,
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

    pub fn endpoint(&self) -> String {
        format!("http://{}/api/v1", self.host)
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::config::{RepoConfig};
    use crate::error::OxenError;
    use crate::test;

    use std::path::Path;

    #[test]
    fn test_read_cfg() {
        let path = test::repo_cfg_file();
        let config = RepoConfig::from(path);
        assert_eq!(config.endpoint(), "http://localhost:4000/api/v1");
    }

    #[test]
    fn test_create_repo_cfg() -> Result<(), OxenError> {
        let name: &str = "Test Repo";
        let cfg = test::create_repo_cfg(name)?;
        assert_eq!(cfg.repository.name, name);
        // cleanup
        api::repositories::delete(&cfg.to_auth(), &cfg.repository.id)?;
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
