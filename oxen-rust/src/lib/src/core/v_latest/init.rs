use std::path::Path;

use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util;

pub fn init(path: &Path) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);
    if hidden_dir.exists() {
        let err = format!("Oxen repository already exists: {path:?}");
        return Err(OxenError::basic_str(err));
    }

    // Cleanup the .oxen dir if init fails
    match init_with_version(path, MinOxenVersion::LATEST) {
        Ok(result) => Ok(result),
        Err(error) => {
            util::fs::remove_dir_all(hidden_dir)?;
            Err(error)
        }
    }
}

pub fn init_with_version(
    path: &Path,
    version: MinOxenVersion,
) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    std::fs::create_dir_all(hidden_dir)?;
    let config_path = util::fs::config_filepath(path);
    if config_path.try_exists()? {
        let err = format!("Oxen repository already exists: {path:?}");
        return Err(OxenError::basic_str(err));
    }

    let repo = LocalRepository::new_from_version(path, version.to_string())?;
    repo.save(&config_path)?;

    Ok(repo)
}
