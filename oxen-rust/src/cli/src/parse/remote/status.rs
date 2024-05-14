use clap::ArgMatches;

use crate::parse::status::parse_status_args;
use crate::run;
use std::path::PathBuf;

pub async fn status(sub_matches: &ArgMatches) {
    let directory = sub_matches.get_one::<String>("path").map(PathBuf::from);

    let is_remote = true;
    let opts = parse_status_args(sub_matches, is_remote);
    match run::remote::status(directory, &opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}");
        }
    }
}
