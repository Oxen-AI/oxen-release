use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::{api, core::oxenignore, error::OxenError, model::LocalRepository};

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
            .arg(
                Arg::new("directory")
                    .long("directory")
                    .short('d')
                    .help("The destination directory to add the workspace to")
                    .default_value("."),
            )
            .arg_required_else_help(true)
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let mut paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(PathBuf::from)
            .collect();

        let workspace_name = args.get_one::<String>("workspace-name");
        let workspace_id = args.get_one::<String>("workspace-id");
        let directory = args.get_one::<String>("directory").unwrap(); // safe to unwrap because we have a default value

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

        // Handle .oxenignore filtering
        let ignore = oxenignore::create(&repository);
        if let Some(ignore) = &ignore {
            paths.retain(|path| !ignore.matched(path, path.is_dir()).is_ignore());
        }

        // If no paths left after filtering, return early
        if paths.is_empty() {
            return Err(OxenError::basic_str(
                "No files to add after filtering with .oxenignore.",
            ));
        }

        api::client::workspaces::files::add(&remote_repo, workspace_identifier, directory, paths)
            .await?;

        Ok(())
    }
}
