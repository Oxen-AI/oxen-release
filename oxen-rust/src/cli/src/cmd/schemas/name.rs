use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
pub const NAME: &str = "name";

pub struct SchemasNameCmd;

#[async_trait]
impl RunCmd for SchemasNameCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Name a schema by hash.")
            .arg(Arg::new("HASH").help("Hash of the schema you want to name."))
            .arg(Arg::new("NAME").help("Name of the schema."))
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(schema_ref) = args.get_one::<String>("HASH") else {
            return Err(OxenError::basic_str(
                "Must supply a hash of the schema you want to name.",
            ));
        };
        let Some(val) = args.get_one::<String>("NAME") else {
            return Err(OxenError::basic_str("Must supply a name for the schema."));
        };

        // Find the repo
        let repository = LocalRepository::from_current_dir()?;

        // Name the schema
        command::schemas::set_name(&repository, schema_ref, val)?;

        // Print the schema
        let staged = true;
        let verbose = false;
        let val = command::schemas::show(&repository, val, staged, verbose)?;
        println!("{val}");

        Ok(())
    }
}
