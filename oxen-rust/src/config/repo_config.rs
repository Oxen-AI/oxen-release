use crate::model::User;
use crate::util::file_util::FileUtil;
use crate::config::AuthConfig;
use crate::model::Repository;
use serde::Deserialize;
use std::path::Path;

#[derive(Deserialize)]
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

    pub fn endpoint(&self) -> String {
        format!("http://{}/api/v1", self.host)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::RepoConfig;
    use crate::error::OxenError;
    use crate::api;
    use crate::test;

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
        api::repositories::delete(&cfg.to_auth(), &cfg.repository.id)?;
        Ok(())
    }
}
