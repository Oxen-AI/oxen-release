use async_trait::async_trait;
use clap::{arg, Command};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use std::env;

use liboxen::command;

use crate::helpers::check_repo_migration_needed;

use crate::cmd::RunCmd;
pub const NAME: &str = "merge";
pub struct MergeCmd;

#[async_trait]
impl RunCmd for MergeCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Merges a branch into the current checked out branch.")
            .arg_required_else_help(true)
            .arg(arg!(<BRANCH> "The name of the branch you want to merge in."))
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse args
        let branch = args
            .get_one::<String>("BRANCH")
            .expect("Must supply a branch");

        let repo_dir = env::current_dir().unwrap();
        let repository = LocalRepository::from_dir(&repo_dir)?;
        check_repo_migration_needed(&repository)?;

        command::merge(&repository, branch)?;
        Ok(())
    }
}
