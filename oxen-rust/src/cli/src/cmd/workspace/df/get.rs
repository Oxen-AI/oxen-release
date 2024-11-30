use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::core::df::tabular;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::DFOpts;

use crate::cmd::RunCmd;
pub const NAME: &str = "get";

pub struct WorkspaceDFGetCmd;

#[async_trait]
impl RunCmd for WorkspaceDFGetCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Fetch the remote data frame.")
            .arg(
                Arg::new("path")
                    .help("Path to the data frame you want to get.")
                    .required(true),
            )
            .arg(
                Arg::new("workspace_id")
                    .long("workspace_id")
                    .short('w')
                    .help("The workspace id to use."),
            )
            .arg(
                Arg::new("sort-by-embedding-query")
                    .long("sort-by-embedding-query")
                    .help("Sort the output by an embedding query.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("embedding-column")
                    .long("embedding-column")
                    .help("The column to sort by.")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(path) = args.get_one::<String>("path") else {
            return Err(OxenError::basic_str(
                "Must supply a path to the data frame you want to get.",
            ));
        };
        let Some(workspace_id) = args.get_one::<String>("workspace_id") else {
            return Err(OxenError::basic_str("Must supply a workspace id."));
        };

        let repository = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;
        let mut opts = DFOpts::empty();
        if let Some(sort_by_embedding_query) = args.get_one::<String>("sort-by-embedding-query") {
            opts.sort_by_embedding_query = Some(sort_by_embedding_query.to_string());
        }
        if let Some(embedding_column) = args.get_one::<String>("embedding-column") {
            opts.embedding_column = Some(embedding_column.to_string());
        }
        match api::client::workspaces::data_frames::get(&remote_repo, &workspace_id, &path, opts)
            .await
        {
            Ok(response) => {
                if let Some(data_frame) = response.data_frame {
                    let df = data_frame.view.to_df();
                    let df = tabular::strip_excluded_cols(df)?;
                    println!("{:?}", df);
                } else {
                    return Err(OxenError::basic_str(
                        format!("No data frame found. Index the data frame before querying.\n\n  oxen workspace df index {workspace_id} {path}\n")));
                }
            }
            Err(e) => {
                return Err(OxenError::basic_str(format!("{:?}", e)));
            }
        }

        Ok(())
    }
}
