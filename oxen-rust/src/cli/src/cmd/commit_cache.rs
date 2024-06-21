use async_trait::async_trait;
use clap::{arg, Arg, Command};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use liboxen::command;

use std::path::Path;

use crate::cmd::RunCmd;
pub const NAME: &str = "commit-cache";
pub struct CommitCacheCmd;


pub fn commit_cache_args() -> Command {
    Command::new(NAME)
        .about("Compute a commit cache a server repository or set of repositories")
        .arg(Arg::new("PATH").required(true))
        .arg(
            Arg::new("all")
                .long("all")
                .short('a')
                .help("Compute the cache for all the oxen repositories in this directory")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("force")
                .long("force")
                .short('f')
                .help("Force recompute the cache even if it already exists.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(arg!([REVISION] "The commit or branch id you want to compute the cache for. Defaults to main."))
        /* Replace above line with
            Arg::new("REVISION")
                .about("The commit or branch id you want to compute the cache for. Defaults to main.")
                .required(false)
        */
}

#[async_trait]
impl RunCmd for CommitCacheCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        commit_cache_args()
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse args
        let path_str = args.get_one::<String>("PATH").expect("required");
        let path = Path::new(path_str);
    
        let force = args.get_flag("force");
    
        // Call into liboxen
        if args.get_flag("all") {
            match command::commit_cache::compute_cache_on_all_repos(path, force).await {
                Ok(_) => Ok(()),
                Err(err) => {
                    println!("Err: {err}");
                    Err(err)
                }
            }
        } else {
            let revision = args.get_one::<String>("REVISION").map(String::from);
    
            match LocalRepository::new(path) {
                Ok(repo) => match command::commit_cache::compute_cache(&repo, revision, force).await {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        println!("Err: {err}");
                        Err(err)
                    }
                },
                Err(err) => {
                    println!("Err: {err}");
                    Err(err)
                }
            }
        }
    }
}