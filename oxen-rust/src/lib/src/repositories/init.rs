//! # oxen init
//!
//! Initialize a local oxen repository
//!

use std::path::Path;

use crate::constants::MIN_OXEN_VERSION;
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;

/// # Initialize an Empty Oxen Repository
/// ```
/// # use liboxen::command;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # use liboxen::test;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
/// let base_dir = Path::new("repo_dir_init");
/// command::repositori(base_dir)?;
/// assert!(base_dir.join(".oxen").exists());
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn init(path: impl AsRef<Path>) -> Result<LocalRepository, OxenError> {
    init_with_version(path, MIN_OXEN_VERSION)
}

pub fn init_with_version(
    path: impl AsRef<Path>,
    version: MinOxenVersion,
) -> Result<LocalRepository, OxenError> {
    let path = path.as_ref();
    match version {
        MinOxenVersion::V0_10_0 => core::v0_10_0::init(path),
        MinOxenVersion::V0_19_0 => core::v0_19_0::init(path),
    }
}

#[cfg(test)]
mod tests {
    use crate::constants;
    use crate::core::v0_10_0::index::CommitEntryReader;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[test]
    fn test_command_init() -> Result<(), OxenError> {
        test::run_empty_dir_test(|repo_dir| {
            // Init repo
            let repo = repositories::init(repo_dir)?;

            // Init should create the .oxen directory
            let hidden_dir = util::fs::oxen_hidden_dir(repo_dir);
            let config_file = util::fs::config_filepath(repo_dir);
            assert!(hidden_dir.exists());
            assert!(config_file.exists());

            // We make an initial parent commit and branch called "main"
            // just to make our lives easier down the line
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();
            assert_eq!(orig_branch.name, constants::DEFAULT_BRANCH_NAME);
            assert!(!orig_branch.commit_id.is_empty());

            Ok(())
        })
    }

    #[test]
    fn test_do_not_commit_any_files_on_init() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            test::populate_dir_with_training_data(dir)?;

            let repo = repositories::init(dir)?;
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.last().unwrap();
            let reader = CommitEntryReader::new(&repo, commit)?;
            let num_entries = reader.num_entries()?;
            assert_eq!(num_entries, 0);

            Ok(())
        })
    }
}
