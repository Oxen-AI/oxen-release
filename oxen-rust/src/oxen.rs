use clap::{arg, Command};

use liboxen::cli::dispatch;

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
            dispatch::init(path)
        }
        Some(("add", sub_matches)) => {
            let path = sub_matches.value_of("PATH").expect("required");
            dispatch::add(path)
        }
        Some(("push", sub_matches)) => {
            let directory = sub_matches.value_of("DIRECTORY").expect("required");
            dispatch::push(directory)
        }
        Some(("ls", sub_matches)) => {
            let object_type = sub_matches.value_of("OBJECT").expect("required");
            match object_type {
                "remote" => dispatch::list_datasets(),
                _ => {
                    println!("Unknown object type: {}", object_type)
                }
            }
        }
        Some(("clone", sub_matches)) => {
            let url = sub_matches.value_of("URL").expect("required");
            dispatch::clone(url);
        }
        Some((ext, sub_matches)) => {
            let args = sub_matches
                .values_of_os("")
                .unwrap_or_default()
                .collect::<Vec<_>>();

            match ext {
                "commit" => dispatch::commit(args),
                "create" => dispatch::create(args),
                "status" => dispatch::status(),
                _ => {
                    println!("Unknown command {}", ext)
                }
            }
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachabe!()
    }
}
