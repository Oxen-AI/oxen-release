use async_trait::async_trait;
use clap::{arg, Arg, Command};
use std::path::PathBuf;

use liboxen::api;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::error::OxenError;
use liboxen::opts::fetch_opts::FetchOpts;
use liboxen::opts::CloneOpts;
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::{check_remote_version, check_remote_version_blocking};

pub const NAME: &str = "clone";
pub struct CloneCmd;

#[async_trait]
impl RunCmd for CloneCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Clone a repository by its URL")
            .arg_required_else_help(true)
            .arg(arg!(<URL> "URL of the repository you want to clone"))
            .arg(
                Arg::new("subtree")
                    .long("subtree")
                    .help("Clone a subtree of the repository. Useful if you have a large repository and only want to make changes to a specific directory.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("depth")
                    .long("depth")
                    .help("Used in combination with --subtree. The depth at which to clone the subtree")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("all")
                    .long("all")
                    .help("This downloads the full commit history, all the data files, and all the commit databases. Useful if you want to have the entire history locally or push to a new remote.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("branch")
                    .long("branch")
                    .short('b')
                    .help("The branch you want to pull to when you clone.")
                    .default_value(DEFAULT_BRANCH_NAME)
                    .default_missing_value(DEFAULT_BRANCH_NAME)
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let url = args.get_one::<String>("URL").expect("required");
        let all = args.get_flag("all");
        let branch = args
            .get_one::<String>("branch")
            .expect("Must supply a branch");
        let subtree: Option<PathBuf> = args.get_one::<String>("subtree").map(PathBuf::from);
        let depth: Option<u32> = args.get_one::<u32>("depth").copied();

        let dst = std::env::current_dir().expect("Could not get current working directory");
        // Get the name of the repo from the url
        let name = url.split('/').last().unwrap();
        let dst = dst.join(name);

        let opts = CloneOpts {
            url: url.to_string(),
            dst,
            fetch_opts: FetchOpts {
                branch: branch.to_string(),
                subtree_path: subtree,
                depth,
                all,
                ..FetchOpts::new()
            },
        };

        let host = api::client::get_host_from_url(&opts.url)?;
        check_remote_version_blocking(host.clone()).await?;
        check_remote_version(host).await?;

        repositories::clone(&opts).await?;
        Ok(())
    }
}
