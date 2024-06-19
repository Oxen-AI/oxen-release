use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::util;
use std::path::Path;

use crate::cmd::RunCmd;
pub const NAME: &str = "read-lines";
pub struct ReadLinesCmd;


#[async_trait]
impl RunCmd for ReadLinesCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new("read-lines")
        .about("Read a set of lines from a file without loading it all into memory")
        .arg(
            Arg::new("PATH")
                .help("Path to file you want to read")
                .required(true),
        )
        .arg(
            Arg::new("START")
                .help("Start index of file")
                .default_value("0")
                .default_missing_value("0"),
        )
        .arg(
            Arg::new("LENGTH")
                .help("Length you want to read")
                .default_value("10")
                .default_missing_value("10"),
        )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let path_str = args.get_one::<String>("PATH").expect("required");
        let start = args
            .get_one::<String>("START")
            .expect("Must supply START")
            .parse::<usize>()
            .expect("START must be a valid integer.");
        let length = args
            .get_one::<String>("LENGTH")
            .expect("Must supply LENGTH")
            .parse::<usize>()
            .expect("LENGTH must be a valid integer.");
    
        let path = Path::new(path_str);
        let (lines, size) = util::fs::read_lines_paginated_ret_size(path, start, length);
        for line in lines.iter() {
            println!("{line}");
        }
        println!("Total: {size}");
        Ok(())
    }
}
