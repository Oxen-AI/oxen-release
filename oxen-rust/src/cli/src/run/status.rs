use std::path::PathBuf;

use liboxen::api;
use liboxen::command;
use liboxen::error;
use liboxen::error::OxenError;
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::model::LocalRepository;
use liboxen::util;

use crate::helpers::check_repo_migration_needed;

pub async fn status(directory: Option<PathBuf>, opts: &StagedDataOpts) -> Result<(), OxenError> {
    // Look up from the current dir for .oxen directory
    let repo_dir = util::fs::get_repo_root_from_current_dir()
        .ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;

    let repository = LocalRepository::from_dir(&repo_dir)?;
    check_repo_migration_needed(&repository)?;

    let directory = directory.unwrap_or(repository.path.clone());
    let repo_status = command::status_from_dir(&repository, &directory)?;

    if let Some(current_branch) = api::local::branches::current_branch(&repository)? {
        println!(
            "On branch {} -> {}\n",
            current_branch.name, current_branch.commit_id
        );
    } else {
        let head = api::local::commits::head_commit(&repository)?;
        println!(
            "You are in 'detached HEAD' state.\nHEAD is now at {} {}\n",
            head.id, head.message
        );
    }

    repo_status.print_stdout_with_params(opts);

    Ok(())
}
