
use std::path::PathBuf;

use clap::{Arg, Command};
use liboxen::error::OxenError;
use async_trait::async_trait;

use liboxen::command;
use liboxen::model::LocalRepository;
use liboxen::opts::AddOpts;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const ADD: &str = "add";

pub struct AddCmd;

pub fn add_args() -> Command {
    // Setups the CLI args for the init command
    Command::new(ADD)
        .about("Adds the specified files or directories")
        .arg(
            Arg::new("files")
                .required(true)
                .action(clap::ArgAction::Append),
        )
}

#[async_trait]
impl RunCmd for AddCmd {
    fn name(&self) -> &str {
        ADD
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        add_args()
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
       // Parse Args
       let paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(PathBuf::from)
            .collect();

        let opts = AddOpts {
            paths,
            is_remote: false,
            directory: None,
        };

        // Recursively look up from the current dir for .oxen directory
        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        for path in &opts.paths {
            if opts.is_remote {
                command::remote::add(&repository, path, &opts).await?;
            } else {
                command::add(&repository, path)?;
            }
        }
       
        Ok(())
    }
}
