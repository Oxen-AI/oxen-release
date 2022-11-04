use clap::{arg, Arg, Command};
use env_logger::Env;
use liboxen::opts::RestoreOpts;
use std::path::{Path, PathBuf};

use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};
use liboxen::util;
pub mod dispatch;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() {
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
                .arg(arg!([PATH] "The directory to establish the repo in. Defaults to the current directory."))
        )
        .subcommand(
            Command::new("config")
                .about("Sets the user configuration in ~/.oxen/user_config.toml")
                .arg(
                    Arg::new("name")
                        .long("name")
                        .short('n')
                        .help("Set the name you want your commits to be saved as.")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("email")
                        .long("email")
                        .short('e')
                        .help("Set the email you want your commits to be saved as.")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("auth-token")
                        .long("auth")
                        .short('a')
                        .number_of_values(2)
                        .value_names(&["HOST", "TOKEN"])
                        .help("Set the authentication token for a specific oxen-server host.")
                        .takes_value(true),
                )
        )
        .subcommand(
            Command::new("create-remote")
                .about("Creates a remote repository with the name on the host")
                .arg(arg!(<NAMESPACE> "The namespace you would like to use"))
                .arg(arg!(<NAME> "The remote host"))
                .arg(arg!(<HOST> "The remote host"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("remote")
                .about("Manage set of tracked repositories")
                .subcommand(
                    Command::new("add")
                        .arg(arg!(<NAME> "The remote name"))
                        .arg(arg!(<URL> "The remote url"))
                    )
                .subcommand(
                    Command::new("remove")
                        .arg(arg!(<NAME> "The name of the remote you want to remove"))
                    )
                .arg(
                    Arg::new("verbose")
                        .long("verbose")
                        .short('v')
                        .help("Be a little more verbose and show remote url after name.")
                        .takes_value(false),
                )
        )
        .subcommand(
            Command::new("status")
                .about("See at what files are ready to be added or committed")
                .arg(
                    Arg::new("skip")
                        .long("skip")
                        .short('s')
                        .help("Allows you to skip and paginate through the file list preview.")
                        .default_value("0")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .short('l')
                        .help("Allows you to view more file list preview.")
                        .default_value("10")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("print_all")
                        .long("print_all")
                        .short('a')
                        .help("If present, does not truncate the output of status at all.")
                        .takes_value(false),
                )
        )
        .subcommand(Command::new("log").about("See log of commits"))
        .subcommand(
            Command::new("df")
                .about("View and transform data frames. Supported types: csv, tsv, ndjson, jsonl, parquet.")
                .arg(arg!(<PATH> ... "The file path you want to process."))
                .arg_required_else_help(true)
                .arg(
                    Arg::new("output")
                        .long("output")
                        .short('o')
                        .help("Output file to store the transformed data")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("columns")
                        .long("columns")
                        .short('c')
                        .help("A comma separated set of columns names to look at. Ex file,x,y")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("filter")
                        .long("filter")
                        .short('f')
                        .help("An filter the row data based on an expression. Supported Ops (=, !=, >, <, <= , >=) Supported dtypes (str,int,float)")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("vstack")
                        .long("vstack")
                        .help("Combine row data from different files. The number of columns must match.")
                        .takes_value(true)
                        .multiple_values(true),
                )
                .arg(
                    Arg::new("slice")
                        .long("slice")
                        .short('s')
                        .help("A continuous slice of the data you want to look at. Format: 'start..end' Ex) '10..25' will take 15 elements, starting at 10 and ending at 25.")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("take")
                        .long("take")
                        .short('t')
                        .help("A comma separated set of row indices to look at. Ex 1,22,313")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("add-col")
                        .long("add-col")
                        .help("Add a column with a default value to the data table. If used with --add-row, row is added first, then column. Format 'name:val:dtype'")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("add-row")
                        .long("add-row")
                        .help("Add a row and cast to the values data types to match the current schema. If used with --add-col, row is added first, then column. Format 'comma,separated,vals'")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("randomize")
                        .long("randomize")
                        .help("Randomize the order of the table"),
                )
                .arg(
                    Arg::new("schema")
                        .long("schema")
                        .help("Print the full list of columns and data types within the schema."),
                )
        )
        .subcommand(
            Command::new("schemas")
                .about("Manage schemas that are created from committing tabular data")
                .subcommand(
                    Command::new("list")
                )
                .subcommand(
                    Command::new("show")
                        .arg(arg!(<NAME_OR_HASH> ... "Name or the hash of the schema you want to view."))
                )
                .subcommand(
                    Command::new("name")
                        .arg(Arg::new("HASH").help("Hash of the schema you want to name."))
                        .arg(Arg::new("NAME").help("Name of the schema."))
                )
        )
        .subcommand(
            Command::new("add")
                .about("Adds the specified files or directories")
                .arg(arg!(<PATH> ... "The files or directory to add"))
                .arg_required_else_help(true)
        )
        .subcommand(
            Command::new("restore")
                .about("Unstage or discard uncommitted local changes.")
                .arg(Arg::new("PATH").multiple_values(true))
                .arg_required_else_help(true)
                .arg(
                    Arg::new("staged")
                        .help("Removes the file from the staging area, but leaves its actual modifications untouched.")
                        .takes_value(false),
                )
        )
        .subcommand(
            Command::new("branch")
                .about("Manage branches in repository")
                .arg(Arg::new("name").help("Name of the branch").exclusive(true))
                .arg(
                    Arg::new("all")
                        .long("all")
                        .short('a')
                        .help("List both local and remote branches")
                        .exclusive(true)
                        .takes_value(false),
                )
                .arg(
                    Arg::new("remote")
                        .long("remote")
                        .short('r')
                        .help("List all the remote branches")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("force-delete")
                        .long("force-delete")
                        .short('D')
                        .help("Force remove the local branch")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("delete")
                        .long("delete")
                        .short('d')
                        .help("Remove the local branch if it is safe to")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("show-current")
                        .long("show-current")
                        .help("Print the current branch")
                        .exclusive(true)
                        .takes_value(false),
                ),
        )
        .subcommand(
            Command::new("checkout")
                .about("Checks out a branches in the repository")
                .arg(Arg::new("name").help("Name of the branch or commit id to checkout"))
                .arg(
                    Arg::new("create")
                        .long("branch")
                        .short('b')
                        .help("Create the branch and check it out")
                        .exclusive(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::new("theirs")
                        .long("theirs")
                        .help("Checkout the content of the conflicting branch and take it as your data. Will overwrite your working file.")
                        .takes_value(false),
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
                .arg(
                    Arg::new("delete")
                        .long("delete")
                        .short('d')
                        .help("Remove the remote branch")
                        .takes_value(false),
                )
                .arg(arg!(<BRANCH> "Branch name to pull")),
        )
        .subcommand(
            Command::new("pull")
                .about("Pull the files up from a remote branch")
                .arg(arg!(<REMOTE> "Remote you want to pull from"))
                .arg(arg!(<BRANCH> "Branch name to pull")),
        )
        .subcommand(
            Command::new("diff")
                .about("Compare file from a commit history")
                .arg(Arg::new("FILE_OR_COMMIT_ID").required(true))
                .arg(Arg::new("PATH").required(false)),
        )
        .subcommand(
            Command::new("read-lines")
                .about("Read a set of lines from a file without loading it all into memory")
                .arg(arg!(<PATH> "Path to file you want to read"))
                .arg(arg!(<START> "Start index of file"))
                .arg(arg!(<LENGTH> "Length you want to read")),
        );

    let matches = command.get_matches();

    match matches.subcommand() {
        Some(("init", sub_matches)) => {
            let path = sub_matches.value_of("PATH").unwrap_or(".");

            match dispatch::init(path) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("create-remote", sub_matches)) => {
            let namespace = sub_matches.value_of("NAMESPACE").expect("required");
            let name = sub_matches.value_of("NAME").expect("required");
            let host = sub_matches.value_of("HOST").expect("required");

            match dispatch::create_remote(namespace, name, host).await {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("remote", sub_matches)) => {
            if let Some(subcommand) = sub_matches.subcommand() {
                match subcommand {
                    ("add", sub_matches) => {
                        let name = sub_matches.value_of("NAME").expect("required");
                        let url = sub_matches.value_of("URL").expect("required");

                        match dispatch::add_remote(name, url) {
                            Ok(_) => {}
                            Err(err) => {
                                eprintln!("{}", err)
                            }
                        }
                    }
                    ("remove", sub_matches) => {
                        let name = sub_matches.value_of("NAME").expect("required");

                        match dispatch::remove_remote(name) {
                            Ok(_) => {}
                            Err(err) => {
                                eprintln!("{}", err)
                            }
                        }
                    }
                    (command, _) => {
                        eprintln!("Invalid subcommand: {}", command)
                    }
                }
            } else if sub_matches.is_present("verbose") {
                dispatch::list_remotes_verbose().expect("Unable to list remotes.");
            } else {
                dispatch::list_remotes().expect("Unable to list remotes.");
            }
        }
        Some(("config", sub_matches)) => {
            // if let [host, token] = sub_matches
            //     .values_of("auth-token")
            //     .unwrap()
            //     .collect::<Vec<_>>()[..]
            if let Some(auth) = sub_matches.values_of("auth-token") {
                if let [host, token] = auth.collect::<Vec<_>>()[..] {
                    match dispatch::set_auth_token(host, token) {
                        Ok(_) => {}
                        Err(err) => {
                            eprintln!("{}", err)
                        }
                    }
                } else {
                    eprintln!("invalid arguments for --auth");
                }
            }

            if let Some(name) = sub_matches.value_of("name") {
                match dispatch::set_user_name(name) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{}", err)
                    }
                }
            }

            if let Some(email) = sub_matches.value_of("email") {
                match dispatch::set_user_email(email) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{}", err)
                    }
                }
            }
        }
        Some(("status", sub_matches)) => {
            let skip: usize = sub_matches
                .value_of("skip")
                .unwrap_or("0")
                .parse::<usize>()
                .expect("Skip must be a valid integer.");
            let limit: usize = sub_matches
                .value_of("limit")
                .unwrap_or("10")
                .parse::<usize>()
                .expect("Limit must be a valid integer.");
            let print_all = sub_matches.is_present("print_all");

            match dispatch::status(skip, limit, print_all) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err);
                }
            }
        }
        Some(("log", _sub_matches)) => match dispatch::log_commits() {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{}", err)
            }
        },
        Some(("df", sub_matches)) => {
            let path = sub_matches.value_of("PATH").expect("required");
            if sub_matches.is_present("schema") {
                match dispatch::df_schema(path) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{}", err)
                    }
                }
            } else {
                let vstack: Option<Vec<PathBuf>> =
                    if let Some(vstack) = sub_matches.values_of("vstack") {
                        let vals: Vec<PathBuf> = vstack.map(std::path::PathBuf::from).collect();
                        Some(vals)
                    } else {
                        None
                    };

                let opts = liboxen::media::DFOpts {
                    output: sub_matches.value_of("output").map(std::path::PathBuf::from),
                    slice: sub_matches.value_of("slice").map(String::from),
                    take: sub_matches.value_of("take").map(String::from),
                    columns: sub_matches.value_of("columns").map(String::from),
                    filter: sub_matches.value_of("filter").map(String::from),
                    vstack,
                    add_col: sub_matches.value_of("add-col").map(String::from),
                    add_row: sub_matches.value_of("add-row").map(String::from),
                    should_randomize: sub_matches.is_present("randomize"),
                };

                match dispatch::df(path, opts) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{}", err)
                    }
                }
            }
        }
        Some(("schemas", sub_matches)) => {
            if let Some(subcommand) = sub_matches.subcommand() {
                match subcommand {
                    ("list", _sub_matches) => match dispatch::schema_list() {
                        Ok(_) => {}
                        Err(err) => {
                            eprintln!("{}", err)
                        }
                    },
                    ("show", sub_matches) => {
                        let val = sub_matches.value_of("NAME_OR_HASH").expect("required");
                        match dispatch::schema_show(val) {
                            Ok(_) => {}
                            Err(err) => {
                                eprintln!("{}", err)
                            }
                        }
                    }
                    ("name", sub_matches) => {
                        let hash = sub_matches.value_of("HASH").expect("required");
                        let val = sub_matches.value_of("NAME").expect("required");
                        match dispatch::schema_name(hash, val) {
                            Ok(_) => {}
                            Err(err) => {
                                eprintln!("{}", err)
                            }
                        }
                    }
                    (cmd, _) => {
                        eprintln!("Unknown subcommand {}", cmd)
                    }
                }
            } else {
                match dispatch::schema_list() {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{}", err)
                    }
                }
            }
        }

        Some(("add", sub_matches)) => {
            let path = sub_matches.value_of("PATH").expect("required");

            match dispatch::add(path) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("restore", sub_matches)) => {
            let mut paths = sub_matches.values_of("PATH").expect("required");

            let opts = if paths.len() == 2 {
                let commit_or_branch = paths.next().unwrap();
                let path = paths.next().expect("required");
                RestoreOpts {
                    path: PathBuf::from(path),
                    staged: sub_matches.is_present("staged"),
                    source_ref: Some(String::from(commit_or_branch)),
                }
            } else {
                let path = paths.next().expect("required");
                RestoreOpts {
                    path: PathBuf::from(path),
                    staged: sub_matches.is_present("staged"),
                    source_ref: None,
                }
            };

            match dispatch::restore(opts) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("branch", sub_matches)) => {
            if sub_matches.is_present("all") {
                if let Err(err) = dispatch::list_all_branches().await {
                    eprintln!("{}", err)
                }
            } else if let Some(remote_name) = sub_matches.value_of("remote") {
                if let Some(branch_name) = sub_matches.value_of("delete") {
                    if let Err(err) = dispatch::delete_remote_branch(remote_name, branch_name).await
                    {
                        eprintln!("{}", err)
                    }
                } else if let Err(err) = dispatch::list_remote_branches(remote_name).await {
                    eprintln!("{}", err)
                }
            } else if let Some(name) = sub_matches.value_of("name") {
                if let Err(err) = dispatch::create_branch(name) {
                    eprintln!("{}", err)
                }
            } else if let Some(name) = sub_matches.value_of("delete") {
                if let Err(err) = dispatch::delete_branch(name) {
                    eprintln!("{}", err)
                }
            } else if let Some(name) = sub_matches.value_of("force-delete") {
                if let Err(err) = dispatch::force_delete_branch(name) {
                    eprintln!("{}", err)
                }
            } else if sub_matches.is_present("show-current") {
                if let Err(err) = dispatch::show_current_branch() {
                    eprintln!("{}", err)
                }
            } else if let Err(err) = dispatch::list_branches() {
                eprintln!("{}", err)
            }
        }
        Some(("checkout", sub_matches)) => {
            if sub_matches.is_present("create") {
                let name = sub_matches.value_of("create").expect("required");
                if let Err(err) = dispatch::create_checkout_branch(name) {
                    eprintln!("{}", err)
                }
            } else if sub_matches.is_present("theirs") {
                let name = sub_matches.value_of("name").expect("required");
                if let Err(err) = dispatch::checkout_theirs(name) {
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
                .unwrap_or(DEFAULT_BRANCH_NAME);
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
                .unwrap_or(DEFAULT_REMOTE_NAME);
            let branch = sub_matches
                .value_of("BRANCH")
                .unwrap_or(DEFAULT_BRANCH_NAME);

            if sub_matches.is_present("delete") {
                println!("Delete remote branch {}/{}", remote, branch);
            } else {
                match dispatch::push(remote, branch).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{}", err)
                    }
                }
            }
        }
        Some(("pull", sub_matches)) => {
            let remote = sub_matches
                .value_of("REMOTE")
                .unwrap_or(DEFAULT_REMOTE_NAME);
            let branch = sub_matches
                .value_of("BRANCH")
                .unwrap_or(DEFAULT_BRANCH_NAME);
            match dispatch::pull(remote, branch).await {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{}", err)
                }
            }
        }
        Some(("diff", sub_matches)) => {
            // First arg is optional
            let file_or_commit_id = sub_matches.value_of("FILE_OR_COMMIT_ID").expect("required");
            let path = sub_matches.value_of("PATH");
            if let Some(path) = path {
                match dispatch::diff(Some(file_or_commit_id), path) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{}", err)
                    }
                }
            } else {
                match dispatch::diff(None, file_or_commit_id) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{}", err)
                    }
                }
            }
        }
        Some(("clone", sub_matches)) => {
            let url = sub_matches.value_of("URL").expect("required");
            match dispatch::clone(url).await {
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
        Some(("read-lines", sub_matches)) => {
            let path_str = sub_matches.value_of("PATH").expect("required");
            let start: usize = sub_matches
                .value_of("START")
                .unwrap_or("0")
                .parse::<usize>()
                .unwrap();
            let length: usize = sub_matches
                .value_of("LENGTH")
                .unwrap_or("10")
                .parse::<usize>()
                .unwrap();

            let path = Path::new(path_str);
            let (lines, size) = util::fs::read_lines_paginated_ret_size(path, start, length);
            for line in lines.iter() {
                println!("{}", line);
            }
            println!("Total: {}", size);
        }
        // TODO: Get these in the help command instead of just falling back
        Some((ext, sub_matches)) => {
            let args = sub_matches
                .values_of_os("")
                .unwrap_or_default()
                .collect::<Vec<_>>();

            match ext {
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
