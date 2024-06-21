use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use std::env;

use liboxen::command;

use liboxen::constants::{DEFAULT_REMOTE_NAME, DEFAULT_BRANCH_NAME};
use crate::helpers::{get_host_from_repo, check_repo_migration_needed, check_remote_version_blocking, check_remote_version};

use crate::cmd::RunCmd;
pub const NAME: &str = "pull";
pub struct PullCmd;


pub fn pull_args() -> Command {
    Command::new(NAME)
    .about("Pull the files up from a remote branch")
    .arg(
        Arg::new("REMOTE")
            .help("Remote you want to pull from")
            .default_value(DEFAULT_REMOTE_NAME)
            .default_missing_value(DEFAULT_REMOTE_NAME),
    )
    .arg(
        Arg::new("BRANCH")
            .help("Branch name to pull")
            .default_value(DEFAULT_BRANCH_NAME)
            .default_missing_value(DEFAULT_BRANCH_NAME),
    )
    .arg(
        Arg::new("all")
            .long("all")
            .help("This pulls the full commit history, all the data files, and all the commit databases. Useful if you want to have the entire history locally or push to a new remote.")
            .action(clap::ArgAction::SetTrue),
    )
}

#[async_trait]
impl RunCmd for PullCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        pull_args()
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse args
        let remote = args
            .get_one::<String>("REMOTE")
            .expect("Must supply a remote");
        let branch = args
            .get_one::<String>("BRANCH")
            .expect("Must supply a branch");

        let all = args.get_flag("all");

        // Get the repo
        let repo_dir = env::current_dir().unwrap();
        let repository = LocalRepository::from_dir(&repo_dir)?;
    
        let host = get_host_from_repo(&repository)?;
        check_repo_migration_needed(&repository)?;
        check_remote_version_blocking(host.clone()).await?;
        check_remote_version(host).await?;
    
        command::pull_remote_branch(&repository, remote, branch, all).await?;
        Ok(())
    }
}