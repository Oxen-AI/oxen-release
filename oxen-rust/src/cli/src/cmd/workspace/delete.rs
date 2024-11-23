use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::api;
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
            Arg::new("id")
                .required(true)
                .help("The id of the workspace to delete"),
        )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repo = LocalRepository::from_current_dir()?;

        let Some(id) = args.get_one::<String>("id") else {
            return Err(OxenError::basic_str("Must supply id"));
        };

        let remote_repo = api::client::repositories::get_default_remote(&repo).await?;
        api::client::workspaces::delete(&remote_repo, &id).await?;

        Ok(())
    }
}
