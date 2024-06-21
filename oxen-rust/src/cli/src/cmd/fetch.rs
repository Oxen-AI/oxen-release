use async_trait::async_trait;
use clap::Command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::error;
use liboxen::util;
use std::env;

use liboxen::command;

use crate::helpers::{get_host_from_repo, check_remote_version_blocking, check_repo_migration_needed};

use crate::cmd::RunCmd;
pub const NAME: &str = "fetch";
pub struct FetchCmd;

pub fn fetch_args() -> Command {
    Command::new(NAME).about("Download objects and refs from the remote repository")
}

#[async_trait]
impl RunCmd for FetchCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        fetch_args()
    }
    
    async fn run(&self, _args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Look up from the current dir for .oxen directory
        let current_dir = env::current_dir().unwrap();
        let repo_dir =
            util::fs::get_repo_root(&current_dir).ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;

        let repository = LocalRepository::from_dir(&repo_dir)?;
        let host = get_host_from_repo(&repository)?;

        check_repo_migration_needed(&repository)?;
        check_remote_version_blocking(host.clone()).await?;
        command::fetch(&repository).await?;
        Ok(())
    }
}