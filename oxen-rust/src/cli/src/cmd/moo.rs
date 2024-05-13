

use clap::{Arg, Command};
use liboxen::error::OxenError;
use async_trait::async_trait;

use crate::cmd::RunCmd;
pub const NAME: &str = "moo";
pub struct MooCmd;

#[async_trait]
impl RunCmd for MooCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Hello, world! 🐂")
            .arg(
                Arg::new("number")
                    .long("number")
                    .short('n')
                    .help("How big is the moo.")
                    .default_value("2")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let n = args
            .get_one::<String>("number")
            .expect("Must supply number")
            .parse::<usize>()
            .expect("number must be a valid integer.");

        // Print the moo with -n number of o's
        println!("m{}!", "o".repeat(n));

        Ok(())
    }
}
