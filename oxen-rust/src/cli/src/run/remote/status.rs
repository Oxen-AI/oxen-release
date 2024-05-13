
use std::path::PathBuf;

use liboxen::api;
use liboxen::command;
use liboxen::error;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::util;

use crate::helpers::{
    get_host_from_repo,
    check_remote_version_blocking,
    check_remote_version,
};

pub async fn remote_status(directory: Option<PathBuf>, opts: &StagedDataOpts) -> Result<(), OxenError> {
    // Recursively look up from the current dir for .oxen directory
    let repo_dir =
        util::fs::get_repo_root_from_current_dir()
            .ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;

    let repository = LocalRepository::from_dir(&repo_dir)?;
    let host = get_host_from_repo(&repository)?;
    check_remote_version_blocking(host.clone()).await?;
    check_remote_version(host).await?;

    let directory = directory.unwrap_or(PathBuf::from("."));

    if let Some(current_branch) = api::local::branches::current_branch(&repository)? {
        let remote_repo = api::remote::repositories::get_default_remote(&repository).await?;
        let repo_status =
            command::remote::status(&remote_repo, &current_branch, &directory, opts).await?;
        if let Some(remote_branch) =
            api::remote::branches::get_by_name(&remote_repo, &current_branch.name).await?
        {
            println!(
                "Checking remote branch {} -> {}\n",
                remote_branch.name, remote_branch.commit_id
            );
            repo_status.print_stdout_with_params(opts);
        } else {
            println!("Remote branch '{}' not found", current_branch.name);
        }
    } else {
        let head = api::local::commits::head_commit(&repository)?;
        println!(
            "You are in 'detached HEAD' state.\nHEAD is now at {} {}\nYou cannot query remote status unless you are on a branch.",
            head.id, head.message
        );
    }

    Ok(())
}