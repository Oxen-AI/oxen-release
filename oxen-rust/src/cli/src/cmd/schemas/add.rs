use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
pub const NAME: &str = "add";

pub struct SchemasAddCmd;

#[async_trait]
impl RunCmd for SchemasAddCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Stage a schema to a data frame to be committed.")
            .arg(Arg::new("PATH").help("The path of the data frame file."))
            .arg(
                Arg::new("column")
                    .long("column")
                    .short('c')
                    .help("The column that you want to override the data type or metadata for."),
            )
            .arg(
                Arg::new("metadata")
                    .long("metadata")
                    .short('m')
                    .help("Set the metadata for a specific column. Must pass in the -c flag."),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        // Path
        let path = args.get_one::<String>("PATH");

        // Flags
        let column = args.get_one::<String>("column");
        let metadata = args.get_one::<String>("metadata");

        let err_msg = "Must supply a file path, column name and either -m for metadata or -t for data type\n\n  oxen schemas add file.csv -c 'col1' -t 'str'\n";

        let Some(path) = path else {
            return Err(OxenError::basic_str(err_msg));
        };

        // Find the repo
        let repository = LocalRepository::from_current_dir()?;

        // If a column is supplied, then we need to supply a data type or metadata for that column
        if let Some(column) = column {
            if let Some(metadata) = metadata {
                match self.schema_add_column_metadata(&repository, path, column, metadata) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
        } else {
            // No column, check if we are just adding metadata to the schema
            if let Some(metadata) = metadata {
                match self.schema_add_metadata(&repository, path, metadata) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
        }

        Ok(())
    }
}

impl SchemasAddCmd {
    fn schema_add_column_metadata(
        &self,
        repository: &LocalRepository,
        schema_ref: impl AsRef<str>,
        column: impl AsRef<str>,
        metadata: impl AsRef<str>,
    ) -> Result<(), OxenError> {
        // make sure metadata is valid json, return oxen error if not
        let metadata: serde_json::Value = serde_json::from_str(metadata.as_ref()).map_err(|e| {
            OxenError::basic_str(format!(
                "Metadata must be valid JSON: '{}'\n{}",
                metadata.as_ref(),
                e
            ))
        })?;

        for (path, schema) in
            command::schemas::add_column_metadata(repository, schema_ref, column, &metadata)?
        {
            println!("{:?}\n{}", path, schema.verbose_str());
        }

        Ok(())
    }

    fn schema_add_metadata(
        &self,
        repository: &LocalRepository,
        schema_ref: impl AsRef<str>,
        metadata: impl AsRef<str>,
    ) -> Result<(), OxenError> {
        let metadata: serde_json::Value = serde_json::from_str(metadata.as_ref()).map_err(|e| {
            OxenError::basic_str(format!(
                "Metadata must be valid JSON: '{}'\n{}",
                metadata.as_ref(),
                e
            ))
        })?;

        for (path, schema) in
            command::schemas::add_schema_metadata(repository, schema_ref, &metadata)?
        {
            println!("{:?}\n{}", path, schema.verbose_str());
        }

        Ok(())
    }
}
