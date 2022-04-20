use crate::config::endpoint;
use crate::config::AuthConfig;
use crate::error::OxenError;
use crate::index::indexer::OXEN_HIDDEN_DIR;
use crate::model::User;
use crate::util::file_util::FileUtil;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

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
    pub fn new() -> Result<RemoteConfig, OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = home_dir.join(Path::new(OXEN_HIDDEN_DIR));

            fs::create_dir_all(&oxen_dir)?;
            let default_host = "localhost:4000";
            let oxen_config = oxen_dir.join(Path::new("remote_config.toml"));
            let config_str = format!("host = \"{}\"", default_host);

            FileUtil::write_to_path(&oxen_config, &config_str);
            Ok(RemoteConfig {
                host: String::from(default_host),
            })
        } else {
            Err(OxenError::basic_str(
                "RemoteConfig::new() Could not find home dir",
            ))
        }
    }

    pub fn default() -> Result<RemoteConfig, OxenError> {
        let err = String::from(
            "RemoteConfig::default() not configuration found, run `oxen login` to configure.",
        );
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = home_dir.join(Path::new(OXEN_HIDDEN_DIR));
            let config_file = oxen_dir.join(Path::new("remote_config.toml"));
            if config_file.exists() {
                Ok(RemoteConfig::from(&config_file))
            } else {
                Err(OxenError::Basic(err))
            }
        } else {
            Err(OxenError::Basic(err))
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
            let oxen_dir = home_dir.join(Path::new(OXEN_HIDDEN_DIR));

            fs::create_dir_all(&oxen_dir)?;
            let config_file = oxen_dir.join(Path::new("remote_config.toml"));
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
        FileUtil::write_to_path(path, &toml);
        Ok(())
    }

    pub fn from(path: &Path) -> RemoteConfig {
        let contents = FileUtil::read_from_path(path).unwrap();
        toml::from_str(&contents).unwrap()
    }

    pub fn endpoint(&self) -> String {
        endpoint::http_endpoint(&self.host)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::RemoteConfig;
    use crate::error::OxenError;
    use crate::test;

    use std::path::Path;

    #[test]
    fn test_read() {
        let config = RemoteConfig::from(test::remote_cfg_file());
        assert_eq!(config.endpoint(), "http://localhost:4000/api/v1");
    }

    #[test]
    fn test_save() -> Result<(), OxenError> {
        let config = RemoteConfig::new()?;
        assert_eq!(config.endpoint(), "http://localhost:4000/api/v1");

        let export_path = Path::new("/tmp/remote_cfg.toml");
        config.save(export_path)?;

        let new_config = RemoteConfig::from(export_path);
        assert_eq!(config.endpoint(), new_config.endpoint());

        Ok(())
    }
}
