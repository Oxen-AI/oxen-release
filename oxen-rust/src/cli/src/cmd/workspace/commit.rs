use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const NAME: &str = "commit";
pub struct WorkspaceCommitCmd;

#[async_trait]
impl RunCmd for WorkspaceCommitCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Commit the staged files to the repository.")
            .arg(
                Arg::new("workspace_id")
                    .long("workspace_id")
                    .short('w')
                    .required(true)
                    .help("The workspace_id of the workspace"),
            )
            .arg(
                Arg::new("message")
                    .help("The message for the commit. Should be descriptive about what changed.")
                    .long("message")
                    .short('m')
                    .required(true)
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(message) = args.get_one::<String>("message") else {
            return Err(OxenError::basic_str(
                "Err: Usage `oxen workspace commit -w <workspace_id> -m <message>`",
            ));
        };

        let Some(workspace_id) = args.get_one::<String>("workspace_id") else {
            return Err(OxenError::basic_str(
                "Err: Usage `oxen workspace commit -w <workspace_id> -m <message>`",
            ));
        };

        let repo = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repo)?;

        println!("Committing to remote with message: {message}");
        command::workspace::commit(&repo, workspace_id, message).await?;

        Ok(())
    }
}
