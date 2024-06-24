use async_trait::async_trait;
use clap::Command;

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::DiffCmd;
use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const NAME: &str = "commit";
pub struct RemoteDiffCmd;

#[async_trait]
impl RunCmd for RemoteDiffCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        DiffCmd.args()
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let opts = DiffCmd::parse_args(args);

        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        let mut remote_diff = command::remote::diff(&repository, &opts.path_1).await?;
        DiffCmd::print_diff_result(&remote_diff)?;
        DiffCmd::maybe_save_diff_output(&mut remote_diff, opts.output)?;

        // TODO: Allow them to save a remote diff to disk

        Ok(())
    }
}
