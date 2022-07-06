use clap::{arg, Arg, Command};
use env_logger::Env;
use std::path::Path;

use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};
pub mod dispatch;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() {
    env_logger::init_from_env(Env::default());

    let command = Command::new("oxen")
        .version(VERSION)
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
            Command::new("set-remote")
                .about("Sets remote url for repository")
                .arg(arg!(<NAME> "The remote name"))
                .arg(arg!(<URL> "The remote url"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("status").about("See at what files are ready to be added or committed"),
        )
        .subcommand(Command::new("log").about("See log of commits"))
        .subcommand(
            Command::new("add")
                .about("Adds the specified files or directories")
                .arg(arg!(<PATH> ... "The files or directory to add"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("branch")
                .about("Manage branches in repository")
                .arg(
                    Arg::new("name")
                        .help("Name of the branch")
                        .conflicts_with("all")
                        .exclusive(true),
                )
                .arg(
                    Arg::new("all")
                        .long("all")
                        .short('a')
                        .help("List all the local branches")
                        .conflicts_with("name")
                        .conflicts_with("remote")
                        .exclusive(true)
                        .takes_value(false),
                )
                .arg(
                    Arg::new("remote")
                        .long("remote")
                        .short('r')
                        .help("List all the remote branches")
                        .conflicts_with("name")
                        .conflicts_with("all")
                        .exclusive(true)
                        .takes_value(false),
                ),
        )
        .subcommand(
            Command::new("checkout")
                .about("Checks out a branches in the repository")
                .arg(Arg::new("name").help("Name of the branch").exclusive(true))
                .arg(
                    Arg::new("create")
                        .long("branch")
                        .short('b')
                        .help("Create the branch and check it out")
                        .exclusive(true)
                        .takes_value(true),
                ),
        )
        .subcommand(
            Command::new("merge")
                .about("Merges a branch into the current checked out branch.")
                .arg_required_else_help(true)
                .arg(arg!(<BRANCH> "The name of the branch you want to merge in.")),
        )
        .subcommand(
            Command::new("clone")
                .about("Clone a repository by its URL")
                .arg_required_else_help(true)
                .arg(arg!(<URL> "URL of the repository you want to clone")),
        )
        .subcommand(
            Command::new("inspect")
                .about("Inspect a key-val pair db")
                .arg_required_else_help(true)
                .arg(arg!(<PATH> "The path to the database you want to inspect")),
        )
        .subcommand(
            Command::new("push")
                .about("Push the the files to the remote branch")
                .arg(arg!(<REMOTE> "Remote you want to pull from"))
                .arg(arg!(<BRANCH> "Branch name to pull")),
        )
        .subcommand(
            Command::new("pull")
                .about("Pull the files up from a remote branch")
                .arg(arg!(<REMOTE> "Remote you want to pull from"))
                .arg(arg!(<BRANCH> "Branch name to pull")),
        );

    let matches = command.get_matches();

    match matches.subcommand() {
        Some(("init", sub_matches)) => {
            let path = sub_matches
                .value_of("PATH")
                .ok_or(".")
                .expect("Must provide path to repository.");

            match dispatch::init(path) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("create-remote", _sub_matches)) => match dispatch::create_remote() {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{}", err)
            }
        },
        Some(("set-remote", sub_matches)) => {
            let name = sub_matches.value_of("NAME").expect("required");
            let url = sub_matches.value_of("URL").expect("required");

            match dispatch::set_remote(name, url) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("status", _sub_matches)) => match dispatch::status() {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{}", err);
            }
        },
        Some(("log", _sub_matches)) => match dispatch::log_commits() {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{}", err)
            }
        },
        Some(("add", sub_matches)) => {
            let path = sub_matches.value_of("PATH").expect("required");

            match dispatch::add(path) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("branch", sub_matches)) => {
            if sub_matches.is_present("all") {
                if let Err(err) = dispatch::list_branches() {
                    eprintln!("{}", err)
                }
            } else if sub_matches.is_present("remote") {
                if let Err(err) = dispatch::list_remote_branches() {
                    eprintln!("{}", err)
                }
            } else if let Some(name) = sub_matches.value_of("name") {
                if let Err(err) = dispatch::create_branch(name) {
                    eprintln!("{}", err)
                }
            } else {
                eprintln!("`oxen branch` must supply name or -a to list all")
            }
        }
        Some(("checkout", sub_matches)) => {
            if sub_matches.is_present("create") {
                let name = sub_matches.value_of("create").expect("required");
                if let Err(err) = dispatch::create_checkout_branch(name) {
                    eprintln!("{}", err)
                }
            } else if sub_matches.is_present("name") {
                let name = sub_matches.value_of("name").expect("required");
                if let Err(err) = dispatch::checkout(name) {
                    eprintln!("{}", err)
                }
            } else {
                eprintln!("Err: Usage `oxen checkout <name>`");
            }
        }
        Some(("merge", sub_matches)) => {
            let branch = sub_matches
                .value_of("BRANCH")
                .or(Some(DEFAULT_BRANCH_NAME))
                .unwrap();
            match dispatch::merge(branch) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("push", sub_matches)) => {
            let remote = sub_matches
                .value_of("REMOTE")
                .or(Some(DEFAULT_REMOTE_NAME))
                .unwrap();
            let branch = sub_matches
                .value_of("BRANCH")
                .or(Some(DEFAULT_BRANCH_NAME))
                .unwrap();
            match dispatch::push(remote, branch) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("pull", sub_matches)) => {
            let remote = sub_matches
                .value_of("REMOTE")
                .or(Some(DEFAULT_REMOTE_NAME))
                .unwrap();
            let branch = sub_matches
                .value_of("BRANCH")
                .or(Some(DEFAULT_BRANCH_NAME))
                .unwrap();
            match dispatch::pull(remote, branch) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("clone", sub_matches)) => {
            let url = sub_matches.value_of("URL").expect("required");
            match dispatch::clone(url) {
                Ok(_) => {}
                Err(err) => {
                    println!("Err: {}", err)
                }
            }
        }
        Some(("inspect", sub_matches)) => {
            let path_str = sub_matches.value_of("PATH").expect("required");
            let path = Path::new(path_str);
            match dispatch::inspect(path) {
                Ok(_) => {}
                Err(err) => {
                    println!("Err: {}", err)
                }
            }
        }
        // TODO: Get these in the help command instead of just falling back
        Some((ext, sub_matches)) => {
            let args = sub_matches
                .values_of_os("")
                .unwrap_or_default()
                .collect::<Vec<_>>();

            match ext {
                "login" => dispatch::login(),
                "commit" => dispatch::commit(args),
                _ => {
                    println!("Unknown command {}", ext);
                    Ok(())
                }
            }
            .unwrap_or_default()
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachabe!()
    }
}
