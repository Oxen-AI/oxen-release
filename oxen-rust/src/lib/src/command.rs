use crate::config::{AuthConfig, RepoConfig};
use crate::error::OxenError;
use crate::model::Repository;
use crate::util;

use std::path::Path;

pub fn init(path: &Path) -> Result<RepoConfig, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);
    match AuthConfig::default() {
        Ok(auth_config) => {
            std::fs::create_dir_all(hidden_dir)?;
            let config_path = util::fs::config_filepath(path);
            let config = RepoConfig::from(auth_config, Repository::new(path));
            config.save(&config_path)?;
            Ok(config)
        }
        Err(_) => Err(OxenError::basic_str("")),
    }
}

#[cfg(test)]
mod tests {

    use crate::command;
    use crate::error::OxenError;
    use crate::test;
    use crate::util;

    const BASE_DIR: &str = "data/test/runs";

    #[test]
    fn test_command_init() -> Result<(), OxenError> {
        let repo_dir = test::create_repo_dir(BASE_DIR)?;
        command::init(&repo_dir)?;

        // Init should create the .oxen directory
        let hidden_dir = util::fs::oxen_hidden_dir(&repo_dir);
        let config_file = util::fs::config_filepath(&repo_dir);
        assert!(hidden_dir.exists());
        assert!(config_file.exists());

        Ok(())
    }
}
