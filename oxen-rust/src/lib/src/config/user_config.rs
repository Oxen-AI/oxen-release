use crate::error::OxenError;
use crate::model::User;
use crate::util;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const USER_CONFIG_FILENAME: &str = "user_config.toml";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HostConfig {
    pub host: String,
    pub auth_token: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserConfig {
    pub name: String,
    pub email: String,
    host_configs: Vec<HostConfig>,
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
            host_configs: Vec::new(),
        }
    }

    fn new_empty() -> UserConfig {
        UserConfig {
            name: String::from(""),
            email: String::from(""),
            host_configs: Vec::new(),
        }
    }

    pub fn get() -> Result<UserConfig, OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = util::fs::oxen_hidden_dir(&home_dir);
            let mut config_file = oxen_dir.join(Path::new(USER_CONFIG_FILENAME));
            if std::env::var("TEST").is_ok() {
                config_file = PathBuf::from("data/test/config/user_config.toml");
            }
            if config_file.exists() {
                Ok(UserConfig::new(&config_file))
            } else {
                log::debug!(
                    "unable to find config file at {:?}. Current working directory is {:?}",
                    config_file,
                    std::env::current_dir().unwrap()
                );
                Err(OxenError::email_and_name_not_set())
            }
        } else {
            Err(OxenError::email_and_name_not_set())
        }
    }

    pub fn get_or_create() -> Result<UserConfig, OxenError> {
        match Self::get() {
            Ok(config) => Ok(config),
            Err(_err) => {
                let config = Self::new_empty();
                config.save_default()?;
                println!(
                    "🐂 created a new config file in \"$HOME/.oxen/{}",
                    USER_CONFIG_FILENAME
                );
                Ok(config)
            }
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
        util::fs::write_to_path(path, &toml)?;
        Ok(())
    }

    pub fn add_host_auth_token<S: AsRef<str>>(&mut self, host: S, token: S) {
        self.host_configs.push(HostConfig {
            host: String::from(host.as_ref()),
            auth_token: Some(String::from(token.as_ref())),
        });
    }

    pub fn auth_token_for_host<S: AsRef<str>>(&self, host: S) -> Option<String> {
        let host = host.as_ref();
        for config in self.host_configs.iter() {
            if config.host == host {
                if config.auth_token.is_none() {
                    log::debug!("no auth_token found for host \"{}\"", config.host);
                }
                return config.auth_token.clone();
            }
        }
        log::debug!("no host configuration found for {}", host);
        None
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
