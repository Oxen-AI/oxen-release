use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::constants::DEFAULT_BRANCH_NAME;

use crate::cmd::RunCmd;
pub const NAME: &str = "create";
pub struct WorkspaceCreateCmd;

#[async_trait]
impl RunCmd for WorkspaceCreateCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Creates a new workspace")
            .arg(
                Arg::new("branch")
                    .long("branch")
                    .short('b')
                    .default_value(DEFAULT_BRANCH_NAME)
                    .help("The branch to create the workspace from"),
            )
            .arg(
                Arg::new("workspace_id")
                    .long("workspace_id")
                    .short('w')
                    .required(true)
                    .help("The workspace_id of the workspace"),
            )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repo = LocalRepository::from_current_dir()?;

        let Some(branch_name) = args.get_one::<String>("branch") else {
            return Err(OxenError::basic_str("Must supply branch"));
        };

        let Some(workspace_id) = args.get_one::<String>("workspace_id") else {
            return Err(OxenError::basic_str("Must supply workspace_id"));
        };

        command::workspace::create(&repo, &branch_name, &workspace_id).await?;

        Ok(())
    }
}
