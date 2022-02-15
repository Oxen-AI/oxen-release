use clap::{App, Arg};

use std::path::PathBuf;
use std::env;

use blob::indexer::Indexer;
use blob::util::hasher;

fn main() {
    let args = App::new("indexer")
        .version("0.0.1")
        .about("Bulk uploads data to our server")
        .arg(
            Arg::with_name("command")
                .help("The command to run (add, commit, push)")
                .takes_value(true)
                .required(true)
        )
        .arg(
            Arg::with_name("directory")
                .help("The directory to find the data in")
                .takes_value(true)
        )
        .get_matches();

    let command = args.value_of("command").unwrap();
    
    

    match command {
        "init" => {
            let dirname = String::from(args.value_of("directory").unwrap());
            let directory = PathBuf::from(&dirname);
            let indexer = Indexer::new(&directory);
            indexer.init()
        },
        "add" => {
            let current_dir = env::current_dir().unwrap();
            let indexer = Indexer::new(&current_dir);
            let dirname = String::from(args.value_of("directory").unwrap());
            let directory = PathBuf::from(&dirname);
            indexer.add_files(&directory)
        },
        "commit" => {
            let current_dir = env::current_dir().unwrap();
            let indexer = Indexer::new(&current_dir);
            indexer.commit_staged()
        },
        "sync" => {
            let current_dir = env::current_dir().unwrap();
            let mut indexer = Indexer::new(&current_dir);
            // Must login to get access token
            match indexer.login() {
                Ok(_) => {
                    indexer.sync();
                },
                Err(err) => {
                    eprintln!("Indexer couldn't log in: {}", err)
                }
            }
        },
        "encode" => {
            let value = String::from(args.value_of("directory").unwrap());
            let encoded = hasher::hash_buffer(value.as_bytes());
            println!("{} => {}", value, encoded)
        },
        _ => {
            println!("Unknown command: {}", command)
        },
    }
}
