
use std::path::PathBuf;

use clap::{arg, Command};
use liboxen::error::OxenError;
use async_trait::async_trait;

use liboxen::command;
use crate::cmd::RunCmd;
use crate::helpers::{
    get_host_or_default,
    check_remote_version,
};

pub const INIT: &str = "init";

pub struct InitCmd;

#[async_trait]
impl RunCmd for InitCmd {
    fn name(&self) -> &str {
        INIT
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(INIT)
            .about("Initializes a local repository")
            .arg(arg!([PATH] "The directory to establish the repo in. Defaults to the current directory."))
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
