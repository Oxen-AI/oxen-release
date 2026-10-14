use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use crate::helpers::check_repo_migration_needed;

use crate::util;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::RmOpts;
use liboxen::repositories;
use std::path::PathBuf;

use crate::cmd::RunCmd;
pub const NAME: &str = "rm";
pub struct RmCmd;

pub fn rm_args() -> Command {
    Command::new(NAME)
        .about("Delete files from the working tree and stage the removal")
        .long_about(
            "Delete files from the working tree and stage the removal.\n\n\
             The next commit records the files as removed. Paths must already be \
             committed. Delete a file that has never been committed with your shell \
             instead.\n\n\
             To bring a deleted file back, run `oxen restore --staged <file>` to \
             unstage the removal, then `oxen restore <file>`.\n\n\
             With `--staged`, the working-tree copies are left in place. A committed path \
             is staged for removal and shows as untracked afterward, and a path that has \
             never been committed has what is staged for it unstaged.",
        )
        .arg(
            Arg::new("files")
                .required(true)
                .help("Files or directories to remove. Directories require `-r`.")
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("staged")
                .long("staged")
                .help("Stages the removal and leaves the working-tree copies in place.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("recursive")
                .long("recursive")
                .short('r')
                .help("Recursively removes directory.")
                .action(clap::ArgAction::SetTrue),
        )
}

#[async_trait]
impl RunCmd for RmCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        rm_args()
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), anyhow::Error> {
        let paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(|p| -> Result<PathBuf, OxenError> {
                let current_dir = std::env::current_dir()?;
                let joined_path = current_dir.join(p);
                util::fs::canonicalize(&joined_path).or_else(|_| Ok(joined_path))
            })
            .collect::<Result<Vec<PathBuf>, OxenError>>()?;

        let opts = RmOpts {
            paths,
            staged: args.get_flag("staged"),
            recursive: args.get_flag("recursive"),
        };

        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        repositories::rm(&repository, &opts).await?;

        Ok(())
    }
}
