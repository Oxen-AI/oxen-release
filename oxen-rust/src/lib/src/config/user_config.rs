use crate::error;
use crate::error::OxenError;
use crate::model::User;
use crate::util;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const USER_CONFIG_FILENAME: &str = "user_config.toml";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserConfig {
    pub name: String,
    pub email: String,
    pub token: Option<String>,
}

impl UserConfig {
    pub fn new(path: &Path) -> UserConfig {
        let contents = util::fs::read_from_path(path).unwrap();
        toml::from_str(&contents).unwrap()
    }

    pub fn from_user(user: &User) -> UserConfig {
        UserConfig {
            name: user.name.to_owned(),
            email: user.email.to_owned(),
            token: user.token.to_owned(),
        }
    }

    pub fn default() -> Result<UserConfig, OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = util::fs::oxen_hidden_dir(&home_dir);
            let mut config_file = oxen_dir.join(Path::new(USER_CONFIG_FILENAME));
            if std::env::var("TEST").is_ok() {
                config_file = PathBuf::from("data/test/config/user_config.toml");
            }
            if config_file.exists() {
                Ok(UserConfig::new(&config_file))
            } else {
                log::warn!(
                    "unable to find config file at {:?}. Current working directory is {:?}",
                    config_file,
                    std::env::current_dir().unwrap()
                );
                Err(OxenError::Basic(String::from(
                    error::EMAIL_AND_NAME_NOT_FOUND,
                )))
            }
        } else {
            Err(OxenError::Basic(String::from(
                error::EMAIL_AND_NAME_NOT_FOUND,
            )))
        }
    }

    pub fn save_default(&self) -> Result<(), OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = util::fs::oxen_hidden_dir(&home_dir);

            fs::create_dir_all(&oxen_dir)?;
            let config_file = oxen_dir.join(Path::new(USER_CONFIG_FILENAME));
            log::debug!("Saving config to {:?}", config_file);
            self.save(&config_file)
        } else {
            Err(OxenError::basic_str(
                "Save user config could not find home dir",
            ))
        }
    }

    pub fn save(&self, path: &Path) -> Result<(), OxenError> {
        let toml = toml::to_string(&self)?;
        util::fs::write_to_path(path, &toml);
        Ok(())
    }

    pub fn auth_token(&self) -> Result<String, OxenError> {
        if let Some(token) = &self.token {
            Ok(token.clone())
        } else {
            Err(OxenError::auth_token_not_set())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::UserConfig;
    use crate::error::OxenError;
    use crate::test;
    use std::path::Path;

    #[test]
    fn test_read() {
        let config = UserConfig::new(test::user_cfg_file());
        assert!(!config.name.is_empty());
        assert!(!config.email.is_empty());
    }

    #[test]
    fn test_save() -> Result<(), OxenError> {
        let final_path = Path::new("/tmp/test_save_config.toml");
        let orig_config = UserConfig::new(test::user_cfg_file());

        orig_config.save(final_path)?;

        let config = UserConfig::new(final_path);
        assert!(!config.name.is_empty());
        Ok(())
    }
}
