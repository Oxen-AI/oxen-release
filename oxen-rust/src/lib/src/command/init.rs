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
/// let base_dir = Path::new("repo_dir_init");
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

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::core::index::CommitEntryReader;
    use crate::error::OxenError;
    use crate::test;
    use crate::util;

    #[test]
    fn test_command_init() -> Result<(), OxenError> {
        test::run_empty_dir_test(|repo_dir| {
            // Init repo
            let repo = command::init(repo_dir)?;

            // Init should create the .oxen directory
            let hidden_dir = util::fs::oxen_hidden_dir(repo_dir);
            let config_file = util::fs::config_filepath(repo_dir);
            assert!(hidden_dir.exists());
            assert!(config_file.exists());

            // We make an initial parent commit and branch called "main"
            // just to make our lives easier down the line
            let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();
            assert_eq!(orig_branch.name, constants::DEFAULT_BRANCH_NAME);
            assert!(!orig_branch.commit_id.is_empty());

            Ok(())
        })
    }

    #[test]
    fn test_do_not_commit_any_files_on_init() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            test::populate_dir_with_training_data(dir)?;

            let repo = command::init(dir)?;
            let commits = api::local::commits::list(&repo)?;
            let commit = commits.last().unwrap();
            let reader = CommitEntryReader::new(&repo, commit)?;
            let num_entries = reader.num_entries()?;
            assert_eq!(num_entries, 0);

            Ok(())
        })
    }
}
