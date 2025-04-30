use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::{api, error::OxenError, model::LocalRepository};

use crate::cmd::RunCmd;
pub const NAME: &str = "download";
pub struct WorkspaceDownloadCmd;

#[async_trait]
impl RunCmd for WorkspaceDownloadCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Download files from a workspace")
            .arg(
                Arg::new("workspace-id")
                    .long("workspace-id")
                    .short('w')
                    .required_unless_present("workspace-name")
                    .help("The workspace ID of the workspace")
                    .conflicts_with("workspace-name"),
            )
            .arg(
                Arg::new("workspace-name")
                    .long("workspace-name")
                    .short('n')
                    .required_unless_present("workspace-id")
                    .help("The name of the workspace")
                    .conflicts_with("workspace-id"),
            )
            .arg(
                Arg::new("file")
                    .long("file")
                    .short('f')
                    .required(true)
                    .help("The path of the file to download from the workspace"),
            )
            .arg(
                Arg::new("output")
                    .long("output")
                    .short('o')
                    .help("The output path where the file should be saved"),
            )
            .arg_required_else_help(true)
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let file_path = args.get_one::<String>("file").expect("Must supply file");

        let workspace_name = args.get_one::<String>("workspace-name");
        let workspace_id = args.get_one::<String>("workspace-id");
        let output_path = args
            .get_one::<String>("output")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(file_path));

        let workspace_identifier = match workspace_id {
            Some(id) => id,
            None => {
                // If no ID is provided, try to get the workspace by name
                if let Some(name) = workspace_name {
                    name
                } else {
                    return Err(OxenError::basic_str(
                        "Either workspace-id or workspace-name must be provided.",
                    ));
                }
            }
        };

        let repository = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;

        // TODO: We wrote a notebook here, and cannot download the contents: https://www.oxen.ai/ox/Notebooks/workspaces/a92e6de8-5223-41b5-99ec-de238f1560b9/file/eval-qwen.py
        //       Workflow improvement: auto commit when the notebook spins down, and be able to navigate to workspaces better from the UI.
        api::client::workspaces::files::download(
            &remote_repo,
            workspace_identifier,
            file_path,
            Some(&output_path),
        )
        .await?;

        Ok(())
    }
}
