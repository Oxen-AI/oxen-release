//! # oxen init
//!
//! Initialize a local oxen repository
//!

use std::path::Path;

use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::{api, constants, util};

/// # Initialize an Empty Oxen Repository
/// ```
/// # use liboxen::command;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # use liboxen::test;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
/// let base_dir = Path::new("/tmp/repo_dir_init");
/// command::init(base_dir)?;
/// assert!(base_dir.join(".oxen").exists());
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn init(path: &Path) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);
    if hidden_dir.exists() {
        let err = format!("Oxen repository already exists: {path:?}");
        return Err(OxenError::basic_str(err));
    }

    // Cleanup the .oxen dir if init fails
    match p_init(path) {
        Ok(result) => Ok(result),
        Err(error) => {
            util::fs::remove_dir_all(hidden_dir)?;
            Err(error)
        }
    }
}

fn p_init(path: &Path) -> Result<LocalRepository, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    std::fs::create_dir_all(hidden_dir)?;
    let config_path = util::fs::config_filepath(path);
    let repo = LocalRepository::new(path)?;
    repo.save(&config_path)?;

    api::local::commits::commit_with_no_files(&repo, constants::INITIAL_COMMIT_MSG)?;

    Ok(repo)
}
