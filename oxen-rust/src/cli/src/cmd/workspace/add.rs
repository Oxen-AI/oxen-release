use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::error::OxenError;

use crate::cmd::RunCmd;
pub const NAME: &str = "add";
pub struct WorkspaceAddCmd;

#[async_trait]
impl RunCmd for WorkspaceAddCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Adds a file to the workspace")
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        Ok(())
    }
}
