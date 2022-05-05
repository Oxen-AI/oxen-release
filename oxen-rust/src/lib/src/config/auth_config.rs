use crate::config::HTTPConfig;
use crate::error::OxenError;
use crate::model::User;
use crate::util;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const AUTH_CONFIG_FILENAME: &str = "auth_config.toml";

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
        let contents = util::fs::read_from_path(path).unwrap();
        toml::from_str(&contents).unwrap()
    }

    pub fn default() -> Result<AuthConfig, OxenError> {
        let err = String::from(
            "AuthConfig::default() not configuration found, acquire an auth_config.toml file from your administrator.",
        );
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = util::fs::oxen_hidden_dir(&home_dir);
            let mut config_file = oxen_dir.join(Path::new(AUTH_CONFIG_FILENAME));
            if std::env::var("TEST").is_ok() {
                config_file = PathBuf::from("data/test/config/auth_config.toml");
            }
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
            let oxen_dir = util::fs::oxen_hidden_dir(&home_dir);

            fs::create_dir_all(&oxen_dir)?;
            let config_file = oxen_dir.join(Path::new(AUTH_CONFIG_FILENAME));
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
        util::fs::write_to_path(path, &toml);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{remote_config::DEFAULT_HOST, AuthConfig, HTTPConfig, RemoteConfig};
    use crate::error::OxenError;
    use crate::model::User;
    use crate::test;
    use std::path::Path;

    #[test]
    fn test_read() {
        let config = AuthConfig::new(test::auth_cfg_file());
        assert_eq!(config.host(), DEFAULT_HOST);
        assert!(!config.user.name.is_empty());
    }

    #[test]
    fn test_save() -> Result<(), OxenError> {
        let final_path = Path::new("/tmp/test_save_auth_config.toml");
        let orig_config = AuthConfig::new(test::auth_cfg_file());

        orig_config.save(final_path)?;

        let config = AuthConfig::new(final_path);
        assert_eq!(config.host, DEFAULT_HOST);
        assert!(!config.user.name.is_empty());
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
        assert_eq!(config.host, DEFAULT_HOST);
        assert_eq!(config.user.name, user.name);
        Ok(())
    }
}
