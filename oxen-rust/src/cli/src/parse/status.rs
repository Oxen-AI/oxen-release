
use clap::ArgMatches;

use std::path::PathBuf;

use liboxen::model::staged_data::StagedDataOpts;
use crate::run;

pub async fn status(sub_matches: &ArgMatches) {
    let directory = sub_matches.get_one::<String>("path").map(PathBuf::from);

    let is_remote = false;
    let opts = parse_status_args(sub_matches, is_remote);
    match run::status(directory, &opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}");
        }
    }
}

pub fn parse_status_args(sub_matches: &ArgMatches, is_remote: bool) -> StagedDataOpts {
    let skip = sub_matches
        .get_one::<String>("skip")
        .expect("Must supply skip")
        .parse::<usize>()
        .expect("skip must be a valid integer.");
    let limit = sub_matches
        .get_one::<String>("limit")
        .expect("Must supply limit")
        .parse::<usize>()
        .expect("limit must be a valid integer.");
    let print_all = sub_matches.get_flag("print_all");

    StagedDataOpts {
        skip,
        limit,
        print_all,
        is_remote,
    }
}
