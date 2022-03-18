use crate::error::OxenError;
use crate::model::user::User;
use crate::util::file_util::FileUtil;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OxenConfig {
    pub remote_ip: String,
    pub user: Option<User>,
}

impl PartialEq for OxenConfig {
    fn eq(&self, other: &Self) -> bool {
        self.remote_ip == other.remote_ip && self.user == other.user
    }
}

impl Eq for OxenConfig {}

impl OxenConfig {
    pub fn new() -> Result<OxenConfig, OxenError> {
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = home_dir.join(Path::new(".oxen"));

            fs::create_dir_all(&oxen_dir)?;
            let default_ip = "localhost:4000";
            let oxen_config = oxen_dir.join(Path::new("config.toml"));
            let config_str = format!("remote_ip = \"{}\"", default_ip);

            FileUtil::write_to_path(&oxen_config, &config_str);
            Ok(OxenConfig {
                remote_ip: String::from(default_ip),
                user: None,
            })
        } else {
            Err(OxenError::from_str(
                "OxenConfig::new() Could not find home dir",
            ))
        }
    }

    pub fn default() -> Result<OxenConfig, OxenError> {
        let err = String::from(
            "OxenConfig::default() not configuration found, run `oxen login` to configure.",
        );
        if let Some(home_dir) = dirs::home_dir() {
            let oxen_dir = home_dir.join(Path::new(".oxen"));
            let config_file = oxen_dir.join(Path::new("config.toml"));
            if config_file.exists() {
                Ok(OxenConfig::from(&config_file))
            } else {
                Err(OxenError::Basic(err))
            }
        } else {
            Err(OxenError::Basic(err))
        }
    }

    pub fn add_user(&mut self, user: &User) -> OxenConfig {
        OxenConfig {
            remote_ip: self.remote_ip.clone(),
            user: Some(user.clone()),
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
            Err(OxenError::from_str(
                "OxenConfig::save_default() Could not find home dir",
            ))
        }
    }

    pub fn save(&self, path: &Path) -> Result<(), OxenError> {
        let toml = toml::to_string(&self)?;
        FileUtil::write_to_path(path, &toml);
        Ok(())
    }

    pub fn from(path: &Path) -> OxenConfig {
        let contents = FileUtil::read_from_path(path);
        toml::from_str(&contents).unwrap()
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}/api/v1", self.remote_ip)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::oxen_config::OxenConfig;
    use crate::error::OxenError;
    use crate::model::user::User;
    use std::path::Path;

    #[test]
    fn test_read_test() {
        let path = Path::new("config/oxen_config_test.toml");
        let config = OxenConfig::from(path);
        assert_eq!(config.endpoint(), "http://localhost:4000/api/v1");
    }

    #[test]
    fn test_add_user() -> Result<(), OxenError> {
        let user = User::dummy();
        let path = Path::new("config/oxen_config_test.toml");
        let config = OxenConfig::from(path).add_user(&user);

        let config_user = config.user.unwrap();
        assert_eq!(config_user.token, user.token);
        Ok(())
    }

    #[test]
    fn test_add_user_save() -> Result<(), OxenError> {
        let user = User::dummy();
        let orig_path = Path::new("config/oxen_config_test.toml");
        let final_path = Path::new("/tmp/test_config.toml");

        let orig_config = OxenConfig::from(orig_path);
        let mut new_config = orig_config;

        new_config.add_user(&user).save(final_path)?;

        let config = OxenConfig::from(final_path);
        if let Some(new_user) = config.user {
            assert_eq!(user.token, new_user.token);
        } else {
            panic!("Config does not have user");
        }

        Ok(())
    }
}
