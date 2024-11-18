use async_trait::async_trait;

use clap::arg;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::error::OxenError;
use std::path::PathBuf;

use liboxen::opts::DownloadOpts;
use liboxen::repositories;

use crate::helpers::check_remote_version_blocking;

use crate::cmd::RunCmd;
pub const NAME: &str = "download";
pub struct DownloadCmd;

#[async_trait]
impl RunCmd for DownloadCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
        .about("Download a specific file from the remote repository")
        .arg(arg!(<URL> "URL of the repository you want to download from"))
        .arg(
            Arg::new("paths")
                .required(true)
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("output")
                .long("output")
                .short('o')
                .help("Output file to store the downloaded data")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("revision")
                .long("revision")
                .help("The branch or commit id to download the data from. Defaults to main branch. If a branch is specified, it will download the latest commit from that branch.")
                .action(clap::ArgAction::Set),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse args
        let opts = DownloadOpts {
            url: args
                .get_one::<String>("URL")
                .expect("Must supply a url")
                .to_string(),
            paths: args
                .get_many::<String>("paths")
                .expect("Must supply paths")
                .map(PathBuf::from)
                .collect(),
            dst: args
                .get_one::<String>("output")
                .map(PathBuf::from)
                .unwrap_or(PathBuf::from(".")),
            revision: args.get_one::<String>("revision").map(String::from),
        };

        let paths = &opts.paths;
        if paths.is_empty() {
            return Err(OxenError::basic_str("Must supply a path to download."));
        }

        // Get the host from the url
        let parsed_url = url::Url::parse(&opts.url)
            .map_err(|e| OxenError::basic_str(format!("Invalid URL: {}", e)))?;

        let mut host = parsed_url
            .host_str()
            .ok_or_else(|| OxenError::basic_str("Could not extract host from URL"))?
            .to_string();

        if let Some(port) = parsed_url.port() {
            host = format!("{}:{}", host, port);
        }

        check_remote_version_blocking(host).await?;

        // Check if the first path is a valid remote repo
        if let Some(remote_repo) = api::client::repositories::get_by_url(&opts.url).await? {
            // Download from the remote without having to have a local repo directory
            let commit_id = opts.remote_commit_id(&remote_repo).await?;
            for path in paths {
                repositories::download(&remote_repo, &path, &opts.dst, &commit_id).await?;
            }
        } else {
            eprintln!("Repository does not exist {}", opts.url);
        }

        Ok(())
    }
}
