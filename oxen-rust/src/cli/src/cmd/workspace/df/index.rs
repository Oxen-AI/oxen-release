use std::time::Instant;

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
pub const NAME: &str = "index";

pub struct WorkspaceDFIndexCmd;

#[async_trait]
impl RunCmd for WorkspaceDFIndexCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Index the data frame for querying.")
            .arg(Arg::new("workspace_id").help("The workspace id to use."))
            .arg(Arg::new("path").help("Path to the data frame you want to index or query."))
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(workspace_id) = args.get_one::<String>("workspace_id") else {
            return Err(OxenError::basic_str("Must supply a workspace id."));
        };

        let Some(path) = args.get_one::<String>("path") else {
            return Err(OxenError::basic_str(
                "Must supply a path to the data frame you want to index.",
            ));
        };

        // Time the indexing
        let start = Instant::now();

        let repository = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;
        api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

        let duration = start.elapsed();
        println!("{:?} indexed in {:?}", path, duration);

        Ok(())
    }
}
