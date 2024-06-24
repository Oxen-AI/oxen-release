use async_trait::async_trait;
use clap::Arg;
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
        DiffCmd.args().arg(
            Arg::new("workspace_id")
                .long("workspace_id")
                .short('w')
                .help("The workspace to compare against.")
                .action(clap::ArgAction::Set),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let opts = DiffCmd::parse_args(args);
        let Some(workspace_id) = args.get_one::<String>("workspace_id") else {
            return Err(OxenError::basic_str("Must supply a workspace id."));
        };

        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        let remote_diff = command::remote::diff(&repository, workspace_id, &opts.path_1).await?;
        println!("{:?}", remote_diff);

        // TODO: Allow them to save a remote diff to disk

        Ok(())
    }
}
