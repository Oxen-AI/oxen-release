
use crate::config::endpoint;
use crate::config::AuthConfig;
use crate::error::OxenError;
use crate::model::User;
use crate::util;

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

// Default Hosts
const DEFAULT_ORIGIN_HOST: &str = "hub.oxen.ai";
const DEFAULT_ORIGIN_PORT: &str = "";
const REMOTE_CONFIG_FILENAME: &str = "remote_config.toml";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RemoteConfig {
    pub host: String,
}

impl PartialEq for RemoteConfig {
    fn eq(&self, other: &Self) -> bool {
        self.host == other.host
    }
}

impl Eq for RemoteConfig {}

impl RemoteConfig {
    /// Creates a new remote config if it doesn't exist in ~/.oxen/remote_config.toml
    pub fn new() -> Result<RemoteConfig, OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = util::fs::oxen_hidden_dir(&home_dir);

            fs::create_dir_all(&oxen_dir)?;
            let oxen_config = oxen_dir.join(Path::new(REMOTE_CONFIG_FILENAME));
            let config_str = format!("host = \"{}\"", RemoteConfig::endpoint());

            util::fs::write_to_path(&oxen_config, &config_str);
            Ok(RemoteConfig {
                host: RemoteConfig::endpoint(),
            })
        } else {
            Err(OxenError::basic_str(
                "RemoteConfig::new() Could not find home dir",
            ))
        }
    }

    /// Tries to load a remote config from the default location ~/.oxen/remote_config.toml
    pub fn default() -> Result<RemoteConfig, OxenError> {
        let err = String::from(
            "Remote configuration not found, run `oxen set-remote --global <host>` to configure.",
        );
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = util::fs::oxen_hidden_dir(&home_dir);
            let config_file = oxen_dir.join(Path::new(REMOTE_CONFIG_FILENAME));
            if config_file.exists() {
                Ok(RemoteConfig::from(&config_file))
            } else {
                Err(OxenError::Basic(err))
            }
        } else {
            Err(OxenError::Basic(err))
        }
    }

    pub fn endpoint() -> String {
        if DEFAULT_ORIGIN_PORT == "" {
            return String::from(DEFAULT_ORIGIN_HOST)
        } else {
            return format!("{}:{}", DEFAULT_ORIGIN_HOST, DEFAULT_ORIGIN_PORT)
        }
    }

    pub fn to_auth(&self, user: &User) -> AuthConfig {
        AuthConfig {
            host: self.host.clone(),
            user: user.clone(),
        }
    }

    pub fn save_default(&self) -> Result<(), OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let hidden_dir = util::fs::oxen_hidden_dir(&home_dir);
            fs::create_dir_all(&hidden_dir)?;
            let config_file = hidden_dir.join(Path::new(REMOTE_CONFIG_FILENAME));
            println!("Saving config to {:?}", config_file);
            self.save(&config_file)
        } else {
            Err(OxenError::basic_str(
                "RemoteConfig::save_default() Could not find home dir",
            ))
        }
    }

    pub fn save(&self, path: &Path) -> Result<(), OxenError> {
        let toml = toml::to_string(&self)?;
        util::fs::write_to_path(path, &toml);
        Ok(())
    }

    pub fn from(path: &Path) -> RemoteConfig {
        let contents = util::fs::read_from_path(path).unwrap();
        toml::from_str(&contents).unwrap()
    }

    pub fn http_endpoint(&self) -> String {
        endpoint::http_endpoint(&self.host)
    }
}

#[cfg(test)]
mod tests {
    pub const DEFAULT_HOST: &str = "0.0.0.0:3000";
    use crate::config::RemoteConfig;
    use crate::error::OxenError;
    use crate::test;

    use std::path::Path;

    #[test]
    fn test_read() {
        let config = RemoteConfig::from(test::remote_cfg_file());
        assert_eq!(config.http_endpoint(), format!("http://{}/api/v1", DEFAULT_HOST));
    }

    #[test]
    fn test_save() -> Result<(), OxenError> {
        let config = RemoteConfig::from(test::remote_cfg_file());
        assert_eq!(config.http_endpoint(), format!("http://{}/api/v1", DEFAULT_HOST));

        let export_path = Path::new("/tmp/remote_config.toml");
        config.save(export_path)?;

        let new_config = RemoteConfig::from(export_path);
        assert_eq!(config.http_endpoint(), new_config.http_endpoint());

        Ok(())
    }
}
