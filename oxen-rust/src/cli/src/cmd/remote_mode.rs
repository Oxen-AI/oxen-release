pub mod status;
pub use status::RemoteModeStatusCmd;

pub mod checkout;
pub use checkout::RemoteModeCheckoutCmd;

pub mod commit;
pub use commit::RemoteModeCommitCmd;

pub mod list;
pub use list::RemoteModeListCmd;

use async_trait::async_trait;
use clap::Command;

use liboxen::error::OxenError;
use std::collections::HashMap;

use crate::cmd::RunCmd;
pub const NAME: &str = "remote_mode";
pub struct RemoteModeCmd;

// TODO: I'm not sure we should actually have a 'run' function here
// Do we want users to access these commands outside of remote mode?
//

#[async_trait]
impl RunCmd for RemoteModeCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        let mut command = Command::new(NAME)
            .about("Remote mode operations")
            .subcommand_required(true)
            .arg_required_else_help(true);

        let sub_commands = Self::get_subcommands();
        for cmd in sub_commands.values() {
            command = command.subcommand(cmd.args());
        }
        command
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let sub_commands = Self::get_subcommands();
        if let Some((name, sub_matches)) = args.subcommand() {
            let Some(cmd) = sub_commands.get(name) else {
                eprintln!("Unknown schema subcommand {name}");
                return Err(OxenError::basic_str(format!(
                    "Unknown schema subcommand {name}"
                )));
            };

            // Calling await within an await is making it complain?
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(cmd.run(sub_matches))
            })?;
        }
        Ok(())
    }
}

impl RemoteModeCmd {
    fn get_subcommands() -> HashMap<String, Box<dyn RunCmd>> {
        let commands: Vec<Box<dyn RunCmd>> = vec![
            Box::new(RemoteModeCheckoutCmd),
            Box::new(RemoteModeCommitCmd),
            Box::new(RemoteModeListCmd),
            Box::new(RemoteModeStatusCmd),
        ];
        let mut runners: HashMap<String, Box<dyn RunCmd>> = HashMap::new();
        for cmd in commands {
            runners.insert(cmd.name().to_string(), cmd);
        }
        runners
    }

    pub async fn run_subcommands(name: &str, sub_matches: &clap::ArgMatches) -> Result<(), OxenError> {

        let sub_commands = Self::get_subcommands();
        let Some(cmd) = sub_commands.get(name) else {
            return Err(OxenError::basic_str(format!(
                "Command `oxen {name}` not available for remote mode"
            )));
        };

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(cmd.run(sub_matches))
        })?;

        Ok(())
    }
}
