use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::{api, error::OxenError, model::LocalRepository, opts::AddOpts};

use crate::cmd::{add::add_args, RunCmd};
pub const NAME: &str = "add";
pub struct WorkspaceAddCmd;

#[async_trait]
impl RunCmd for WorkspaceAddCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        add_args()
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
            .arg_required_else_help(true)
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(PathBuf::from)
            .collect();

        let workspace_name = args.get_one::<String>("workspace-name");
        let workspace_id = args.get_one::<String>("workspace-id");

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

        let opts = AddOpts {
            paths,
            is_remote: false,
            directory: None,
        };

        let repository = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;
        for path in opts.paths.iter() {
            api::client::workspaces::files::add(
                &repository,
                &remote_repo,
                workspace_identifier,
                path,
                &opts,
            )
            .await?;
        }

        Ok(())
    }
}
