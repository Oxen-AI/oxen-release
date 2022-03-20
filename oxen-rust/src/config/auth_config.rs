use crate::config::endpoint;
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

impl AuthConfig {
    pub fn new(user: &User) -> Result<AuthConfig, OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = home_dir.join(Path::new(".oxen"));

            fs::create_dir_all(&oxen_dir)?;
            let default_ip = "localhost:4000";
            let oxen_config = oxen_dir.join(Path::new("auth_config.toml"));
            let config_str = format!("host = \"{}\"", default_ip);

            FileUtil::write_to_path(&oxen_config, &config_str);
            Ok(AuthConfig {
                host: String::from(""),
                user: user.clone(),
            })
        } else {
            Err(OxenError::basic_str(
                "AuthConfig::new() Could not find home dir",
            ))
        }
    }

    pub fn default() -> Result<AuthConfig, OxenError> {
        let err = String::from(
            "AuthConfig::default() not configuration found, run `oxen login` to configure.",
        );
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = home_dir.join(Path::new(".oxen"));
            let config_file = oxen_dir.join(Path::new("auth_config.toml"));
            if config_file.exists() {
                Ok(AuthConfig::from(&config_file))
            } else {
                Err(OxenError::Basic(err))
            }
        } else {
            Err(OxenError::Basic(err))
        }
    }

    pub fn save_default(&self) -> Result<(), OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = home_dir.join(Path::new(".oxen"));

            fs::create_dir_all(&oxen_dir)?;
            let config_file = oxen_dir.join(Path::new("config.toml"));
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

    pub fn from(path: &Path) -> AuthConfig {
        let contents = FileUtil::read_from_path(path);
        toml::from_str(&contents).unwrap()
    }

    pub fn endpoint(&self) -> String {
        endpoint::http_endpoint(&self.host)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::AuthConfig;
    use crate::error::OxenError;
    use crate::test;
    use std::path::Path;

    #[test]
    fn test_read() {
        let config = AuthConfig::from(test::auth_cfg_file());
        assert_eq!(config.endpoint(), "http://localhost:4000/api/v1");
        assert_eq!(config.user.name, "Greg");
    }

    #[test]
    fn test_save() -> Result<(), OxenError> {
        let final_path = Path::new("/tmp/auth_config.toml");
        let orig_config = AuthConfig::from(test::auth_cfg_file());

        orig_config.save(final_path)?;

        let config = AuthConfig::from(final_path);
        assert_eq!(config.user.name, "Greg");
        Ok(())
    }
}
