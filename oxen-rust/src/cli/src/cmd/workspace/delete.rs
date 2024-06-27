use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
pub const NAME: &str = "delete";
pub struct WorkspaceDeleteCmd;

#[async_trait]
impl RunCmd for WorkspaceDeleteCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about("Deletes a workspace").arg(
            Arg::new("workspace_id")
                .long("workspace_id")
                .short('w')
                .required(true)
                .help("The workspace_id of the workspace to delete"),
        )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repo = LocalRepository::from_current_dir()?;

        let Some(workspace_id) = args.get_one::<String>("workspace_id") else {
            return Err(OxenError::basic_str("Must supply workspace_id"));
        };

        command::workspace::delete(&repo, &workspace_id).await?;

        Ok(())
    }
}
