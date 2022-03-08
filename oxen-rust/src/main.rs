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

fn push(dataset: &str) {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let mut indexer = Indexer::new(&current_dir);
    // Must login to get access token
    match indexer.login() {
        Ok(_) => {
            match indexer.push(dataset) {
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
            Command::new("list")
                .about("Lists the datasets within a repo")
                .arg_required_else_help(true)
                .arg(arg!(<OBJECT> "Name of the object you want to list (datasets)")),
        )
        .subcommand(
            Command::new("push")
                .about("Push the files up to the remote repository, given a dataset")
                .arg_required_else_help(true)
                .arg(arg!(<DATASET> "Name of dataset to push to")),
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
            let dataset = sub_matches.value_of("DATASET").expect("required");
            push(dataset)
        }
        Some(("list", sub_matches)) => {
            let object_type = sub_matches.value_of("OBJECT").expect("required");
            match object_type {
                "datasets" => {
                    list_datasets()
                }
                _ => {
                    println!("Unknown object type: {}", object_type)
                },
            }
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

    /*
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
        "list_datasets" => {
            let current_dir = env::current_dir().unwrap();
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
        },
        _ => {
            println!("Unknown command: {}", command)
        },
    }
    */
}
