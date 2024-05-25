use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use clap::Command;
use liboxen::error::OxenError;

use crate::cmd::RunCmd;
use crate::helpers::{check_remote_version, get_host_or_default};
use liboxen::command;

pub const NAME: &str = "db";

pub mod get;
pub use get::DbGetCmd;

pub mod list;
pub use list::DbListCmd;

pub struct DbCmd;

#[async_trait]
impl RunCmd for DbCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        let mut command =
            Command::new(NAME).about("Tools to inspect and interact with the local oxen databases");

        // These are all the subcommands the command
        let sub_commands = self.get_subcommands();
        for cmd in sub_commands.values() {
            command = command.subcommand(cmd.args());
        }
        command
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let default = String::from(".");
        let path = args.get_one::<String>("PATH").unwrap_or(&default);

        // Make sure the remote version is compatible
        let host = get_host_or_default()?;
        check_remote_version(host).await?;

        // Initialize the repository
        let directory = dunce::canonicalize(PathBuf::from(&path))?;
        command::init(&directory)?;
        println!("🐂 repository initialized at: {directory:?}");
        Ok(())
    }
}

impl DbCmd {
    fn get_subcommands(&self) -> HashMap<String, Box<dyn RunCmd>> {
        let commands: Vec<Box<dyn RunCmd>> = vec![Box::new(DbListCmd), Box::new(DbGetCmd)];
        let mut runners: HashMap<String, Box<dyn RunCmd>> = HashMap::new();
        for cmd in commands {
            runners.insert(cmd.name().to_string(), cmd);
        }
        runners
    }
}
