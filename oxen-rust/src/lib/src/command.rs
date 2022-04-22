//! # Oxen Commands
//!
//! Top level commands you are likely to run on an Oxen repository
//!

use crate::config::{AuthConfig, RepoConfig};
use crate::error::OxenError;
use crate::model::{Repository, RepoStatus};
use crate::index::{Committer, Stager};
use crate::util;

use std::path::Path;

pub const NO_REPO_MSG: &str = "fatal: no oxen repository exists, looking for directory: .oxen ";

/// # Initialize an Empty Oxen Repository
///
/// ```
/// # use liboxen::command;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// 
/// let base_dir = Path::new("/tmp/repo_dir");
/// command::init(base_dir)?;
/// assert!(base_dir.join(".oxen").exists());
/// 
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
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

/// # Get status of files in repository
///
/// What files are tracked, added, untracked, etc
/// 
/// Empty Repository:
/// 
/// ```
/// use liboxen::command;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// 
/// let base_dir = Path::new("/tmp/repo_dir");
/// // Initialize empty repo
/// command::init(&base_dir)?;
/// // Get status on repo
/// let status = command::status(&base_dir)?;
/// assert!(status.is_clean());
/// 
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
/// 
/// Repository with files
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// 
/// let base_dir = Path::new("/tmp/repo_dir");
/// // Initialize empty repo
/// command::init(&base_dir)?;
/// 
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
/// 
/// // Get status on repo
/// let status = command::status(&base_dir)?;
/// assert_eq!(status.untracked_files.len(), 1);
/// 
/// # std::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn status(path: &Path) -> Result<RepoStatus, OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(&path);
    if !hidden_dir.exists() {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let committer = Committer::new(&path)?;
    let stager = Stager::from(committer)?;

    let added_dirs = stager.list_added_directories()?;
    let added_files = stager.list_added_files()?;
    let untracked_dirs = stager.list_untracked_directories()?;
    let untracked_files = stager.list_untracked_files()?;
    let status = RepoStatus {
        added_dirs,
        added_files,
        untracked_dirs,
        untracked_files,
    };
    Ok(status)
}

#[cfg(test)]
mod tests {

    use crate::command;
    use crate::test;
    use crate::util;

    #[test]
    fn test_command_init() {
        test::run_repo_test(|repo_dir| {
            // Init repo
            let config = command::init(&repo_dir)?;


            // Init should create the .oxen directory
            let hidden_dir = util::fs::oxen_hidden_dir(&repo_dir);
            let config_file = util::fs::config_filepath(&repo_dir);
            assert!(hidden_dir.exists());
            assert!(config_file.exists());
            // Name and id will be random but should be populated
            assert!(!config.repository.id.is_empty());
            assert!(!config.repository.name.is_empty());

            Ok(())
        });
    }

    #[test]
    fn test_command_status_empty() {
        test::run_repo_test(|repo_dir| {
            // Init repo
            command::init(&repo_dir)?;
            // Get status
            let repo_status = command::status(&repo_dir)?;

            assert_eq!(repo_status.added_dirs.len(), 0);
            assert_eq!(repo_status.added_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        });
    }

    #[test]
    fn test_command_status_has_txt_file() {
        test::run_repo_test(|repo_dir| {
            // Init repo
            command::init(&repo_dir)?;
            
            // Write to file
            let hello_file = repo_dir.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World");

            // Get status
            let repo_status = command::status(&repo_dir)?;
            assert_eq!(repo_status.added_dirs.len(), 0);
            assert_eq!(repo_status.added_files.len(), 0);
            assert_eq!(repo_status.untracked_files.len(), 1);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        });
    }
}
