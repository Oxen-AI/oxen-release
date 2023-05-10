use crate::error::OxenError;
use crate::model::User;
use crate::{constants, util};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

pub const USER_CONFIG_FILENAME: &str = "user_config.toml";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HostConfig {
    pub host: String,
    pub auth_token: Option<String>,
}

impl HostConfig {
    pub fn from_host(host: &str) -> HostConfig {
        HostConfig {
            host: String::from(host),
            auth_token: None,
        }
    }
}

// Hash on the id field so we can quickly look up
impl PartialEq for HostConfig {
    fn eq(&self, other: &HostConfig) -> bool {
        self.host == other.host
    }
}
impl Eq for HostConfig {}
impl Hash for HostConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host.hash(state);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserConfig {
    pub name: String,
    pub email: String,
    pub default_host: Option<String>,
    pub host_configs: HashSet<HostConfig>,
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
            default_host: Some(String::from(constants::DEFAULT_HOST)),
            host_configs: HashSet::new(),
        }
    }

    pub fn to_user(&self) -> User {
        User {
            name: self.name.to_owned(),
            email: self.email.to_owned(),
        }
    }

    fn new_empty() -> UserConfig {
        UserConfig {
            name: String::from(""),
            email: String::from(""),
            default_host: Some(String::from(constants::DEFAULT_HOST)),
            host_configs: HashSet::new(),
        }
    }

    pub fn get() -> Result<UserConfig, OxenError> {
        let home_dir = util::fs::oxen_home_dir()?;
        let mut config_file = home_dir.join(Path::new(USER_CONFIG_FILENAME));
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
    }

    pub fn identifier() -> Result<String, OxenError> {
        Ok(util::hasher::hash_str(UserConfig::get()?.to_user().email))
    }

    pub fn get_or_create() -> Result<UserConfig, OxenError> {
        match Self::get() {
            Ok(config) => Ok(config),
            Err(_err) => {
                let config = Self::new_empty();
                config.save_default()?;
                println!("🐂 created a new config file in \"$HOME/.oxen/{USER_CONFIG_FILENAME}");
                Ok(config)
            }
        }
    }

    pub fn save_default(&self) -> Result<(), OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = util::fs::oxen_hidden_dir(home_dir);

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
        let host = host.as_ref();
        self.host_configs.replace(HostConfig {
            host: String::from(host),
            auth_token: Some(String::from(token.as_ref())),
        });
    }

    pub fn auth_token_for_host<S: AsRef<str>>(&self, host: S) -> Option<String> {
        let host = host.as_ref();
        if let Some(token) = self.host_configs.get(&HostConfig::from_host(host)) {
            if token.auth_token.is_none() {
                log::debug!("no auth_token found for host \"{}\"", token.host);
            }
            token.auth_token.clone()
        } else {
            log::debug!("no host configuration found for {}", host);
            None
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

    #[test]
    fn test_second_auth_should_overwrite_first() -> Result<(), OxenError> {
        let mut config = UserConfig::new(test::user_cfg_file());
        let og_num_configs = config.host_configs.len();

        let host = "hub.oxen.ai";
        let token_1 = "1234";
        let token_2 = "5678";
        config.add_host_auth_token(host, token_1);
        config.add_host_auth_token(host, token_2);

        assert_eq!(config.host_configs.len(), og_num_configs + 1);
        assert_eq!(config.auth_token_for_host(host), Some(token_2.to_string()));

        Ok(())
    }
}
