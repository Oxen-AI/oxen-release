use crate::cli::indexer::OXEN_HIDDEN_DIR;
use crate::config::HTTPConfig;
use crate::error::OxenError;
use crate::model::User;
use crate::util::file_util::FileUtil;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AuthConfig {
    pub host: String,
    pub user: User,
}

impl PartialEq for AuthConfig {
    fn eq(&self, other: &Self) -> bool {
        self.host == other.host && self.user == other.user
    }
}

impl Eq for AuthConfig {}

impl<'a> HTTPConfig<'a> for AuthConfig {
    fn host(&'a self) -> &'a str {
        &self.host
    }

    fn auth_token(&'a self) -> &'a str {
        &self.user.token
    }
}

impl AuthConfig {
    pub fn new(path: &Path) -> AuthConfig {
        let contents = FileUtil::read_from_path(path);
        toml::from_str(&contents).unwrap()
    }

    pub fn default() -> Result<AuthConfig, OxenError> {
        let err = String::from(
            "AuthConfig::default() not configuration found, run `oxen login` to configure.",
        );
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = home_dir.join(Path::new(OXEN_HIDDEN_DIR));
            let config_file = oxen_dir.join(Path::new("auth_config.toml"));
            if config_file.exists() {
                Ok(AuthConfig::new(&config_file))
            } else {
                Err(OxenError::Basic(err))
            }
        } else {
            Err(OxenError::Basic(err))
        }
    }

    pub fn save_default(&self) -> Result<(), OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = home_dir.join(Path::new(OXEN_HIDDEN_DIR));

            fs::create_dir_all(&oxen_dir)?;
            let config_file = oxen_dir.join(Path::new("auth_config.toml"));
            println!("Saving config to {:?}", config_file);
            self.save(&config_file)
        } else {
            Err(OxenError::basic_str(
                "AuthConfig::save_default() Could not find home dir",
            ))
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
    use crate::config::{AuthConfig, HTTPConfig, RemoteConfig};
    use crate::error::OxenError;
    use crate::model::User;
    use crate::test;
    use std::path::Path;

    #[test]
    fn test_read() {
        let config = AuthConfig::new(test::auth_cfg_file());
        assert_eq!(config.host(), "localhost:4000");
        assert_eq!(config.user.name, "Greg");
    }

    #[test]
    fn test_save() -> Result<(), OxenError> {
        let final_path = Path::new("/tmp/test_save_auth_config.toml");
        let orig_config = AuthConfig::new(test::auth_cfg_file());

        orig_config.save(final_path)?;

        let config = AuthConfig::new(final_path);
        assert_eq!(config.host, "localhost:4000");
        assert_eq!(config.user.name, "Greg");
        Ok(())
    }

    #[test]
    fn test_remote_to_auth_save() -> Result<(), OxenError> {
        let final_path = Path::new("/tmp/test_remote_to_auth_save_auth_config.toml");
        let orig_config = RemoteConfig::from(test::remote_cfg_file());
        let user = User::dummy();
        let auth_config = orig_config.to_auth(&user);
        auth_config.save(final_path)?;

        let config = AuthConfig::new(final_path);
        assert_eq!(config.host, "localhost:4000");
        assert_eq!(config.user.name, user.name);
        Ok(())
    }
}
