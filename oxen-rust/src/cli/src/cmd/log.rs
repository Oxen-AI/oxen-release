use async_trait::async_trait;
use clap::{arg, ArgMatches, Command};
use colored::Colorize;
use minus::Pager;
use std::fmt::Write;
use time::format_description;

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::LogOpts;

use crate::cmd::RunCmd;
pub const NAME: &str = "log";
pub struct LogCmd;

fn write_to_pager(output: &mut Pager, text: &str) -> Result<(), OxenError> {
    match writeln!(output, "{}", text) {
        Ok(_) => Ok(()),
        Err(_) => Err(OxenError::basic_str("Could not write to pager")),
    }
}

#[async_trait]
impl RunCmd for LogCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about("See log of commits")
        .arg(arg!([REVISION] "The commit or branch id you want to get history from. Defaults to main."),
        )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        // Look up from the current dir for .oxen directory
        let repo = LocalRepository::from_current_dir()?;

        let revision = args.get_one::<String>("REVISION").map(String::from);

        let opts = LogOpts {
            revision,
            remote: false,
        };

        self.log_commits(&repo, &opts).await?;

        Ok(())
    }
}

impl LogCmd {
    pub async fn log_commits(
        &self,
        repo: &LocalRepository,
        opts: &LogOpts,
    ) -> Result<(), OxenError> {
        let commits = api::local::commits::list_with_opts(repo, opts).await?;

        // Fri, 21 Oct 2022 16:08:39 -0700
        let format = format_description::parse(
            "[weekday], [day] [month repr:long] [year] [hour]:[minute]:[second] [offset_hour sign:mandatory]",
        ).unwrap();

        let mut output = Pager::new();

        for commit in commits {
            let commit_id_str = format!("commit {}", commit.id).yellow();
            write_to_pager(&mut output, &format!("{}\n", commit_id_str))?;
            write_to_pager(&mut output, &format!("Author: {}", commit.author))?;
            write_to_pager(
                &mut output,
                &format!("Date:   {}\n", commit.timestamp.format(&format).unwrap()),
            )?;
            write_to_pager(&mut output, &format!("    {}\n", commit.message))?;
        }

        match minus::page_all(output) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error while paging: {}", e);
            }
        }
        Ok(())
    }
}
