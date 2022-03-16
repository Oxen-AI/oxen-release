use clap::{arg, Command};

use std::path::PathBuf;
use std::env;

use oxen::indexer::Indexer;

const NO_REPO_MSG: &str = "fatal: no oxen repository exists, looking for directory: .oxen ";

fn init(path: &str) {
    let directory = PathBuf::from(&path);
    let indexer = Indexer::new(&directory);
    indexer.init()
}

fn clone(url: &str) {
    let _indexer = Indexer::clone(&url);
}

fn add(path: &str) {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let indexer = Indexer::new(&current_dir);
    let directory = PathBuf::from(&path);
    indexer.add_files(&directory)
}

fn push(directory: &str) {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let mut indexer = Indexer::new(&current_dir);

    // Must login to get access token
    match indexer.login() {
        Ok(_) => {
            // Create remote dataset
            indexer.create_dataset_if_not_exists(directory);
            match indexer.push(directory) {
                Ok(_) => {
                    println!("Done.")
                },
                Err(err) => {
                    eprintln!("Error: {}", err)
                }
            }
        },
        Err(err) => {
            eprintln!("Error: {}", err)
        }
    }
}

fn list_datasets() {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let mut indexer = Indexer::new(&current_dir);
    match indexer.login() {
        Ok(_) => {
            match indexer.list_datasets() {
                Ok(datasets) => {
                    for dataset in datasets.iter() {
                        println!("{}", dataset.name);
                    }
                },
                Err(err) => {
                    eprintln!("Indexer couldn't list datasets: {}", err)
                }
            }
        },
        Err(err) => {
            eprintln!("Indexer couldn't log in: {}", err)
        }
    }
}

fn create(args: Vec<&std::ffi::OsStr>) {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let err_str = "Must supply create with a type. Ex:\n\noxen create -d \"my_dataset\"";
    if args.len() != 2 {
        eprintln!("{}", err_str)
    } else {
        let flag = args[0];
        match flag.to_str().unwrap() {
            "-d" => {
                let name_arg = args[1];
                match name_arg.to_str() {
                    Some(name) => {
                        println!("Creating dataset name [{}]", name);
                        println!("TODO!!");
                    },
                    None => {
                        eprintln!("Invalid dataset name: \"{:?}\"", name_arg)
                    }
                }
            },
            _ => {
                eprintln!("oxen create used with unknown flag {:?}", flag)
            }
        }
    }
}

fn commit(args: Vec<&std::ffi::OsStr>) {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let err_str = "Must supply a commit message with -m. Ex:\n\noxen commit -m \"Adding data\"";
    if args.len() != 2 {
        eprintln!("{}", err_str)
    } else {
        let flag = args[0];
        match flag.to_str().unwrap() {
            "-m" => {
                let msg_arg = args[1];
                match msg_arg.to_str() {
                    Some(message) => {
                        println!("Committing with msg [{}]", message);
                        let indexer = Indexer::new(&current_dir);
                        indexer.commit(&message);
                    },
                    None => {
                        eprintln!("Invalid commit message: \"{:?}\"", msg_arg)
                    }
                }
            },
            _ => {
                eprintln!("{}", err_str)
            }
        }
    }
}

fn status() {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let indexer = Indexer::new(&current_dir);
    indexer.status()
}

fn main() {
    let matches = Command::new("oxen")
        .version("0.0.1")
        .about("Data management toolchain")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .allow_invalid_utf8_for_external_subcommands(true)
        .subcommand(
            Command::new("init")
                .about("Initializes a local repository")
                .arg(arg!(<PATH> "The directory to establish the repo in"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("add")
                .about("Adds the specified files or directories")
                .arg(arg!(<PATH> ... "The files or directory to add"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("ls")
                .about("Lists the directories within a repo")
                .arg_required_else_help(true)
                .arg(arg!(<OBJECT> "Run ls locally or remote (remote, local)")),
        )
        .subcommand(
            Command::new("clone")
                .about("Clone a repository by its URL")
                .arg_required_else_help(true)
                .arg(arg!(<URL> "URL of the repository you want to clone")),
        )
        .subcommand(
            Command::new("push")
                .about("Push the files up to the remote repository, given a directory")
                .arg_required_else_help(true)
                .arg(arg!(<DIRECTORY> "Name of directory to push to")),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("init", sub_matches)) => {
            let path = sub_matches.value_of("PATH").expect("required");
            init(path)
        }
        Some(("add", sub_matches)) => {
            let path = sub_matches.value_of("PATH").expect("required");
            add(path)
        }
        Some(("push", sub_matches)) => {
            let directory = sub_matches.value_of("DIRECTORY").expect("required");
            push(directory)
        }
        Some(("ls", sub_matches)) => {
            let object_type = sub_matches.value_of("OBJECT").expect("required");
            match object_type {
                "remote" => {
                    list_datasets()
                }
                _ => {
                    println!("Unknown object type: {}", object_type)
                },
            }
        }
        Some(("clone", sub_matches)) => {
            let url = sub_matches.value_of("URL").expect("required");
            clone(url);
        }
        Some((ext, sub_matches)) => {
            let args = sub_matches
                .values_of_os("")
                .unwrap_or_default()
                .collect::<Vec<_>>();

            match ext {
                "commit" => {
                    commit(args)
                },
                "create" => {
                    create(args)
                },
                "status" => {
                    status()
                },
                _ => {
                    println!("Unknown command {}", ext)
                }
            }
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachabe!()
    }
}
