use crate::cmd_setup::{ADD, COMMIT, DF, DIFF, LOG, LS, RESTORE, RM, STATUS};
use crate::dispatch;
use clap::ArgMatches;
use liboxen::constants::{
    DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE, DEFAULT_REMOTE_NAME,
};
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::model::ContentType;
use liboxen::model::LocalRepository;
use liboxen::opts::{AddOpts, CloneOpts, LogOpts, PaginateOpts, RmOpts};
use liboxen::util;
use liboxen::{command, opts::RestoreOpts};
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub async fn init(sub_matches: &ArgMatches) {
    let path = sub_matches.value_of("PATH").unwrap_or(".");

    match dispatch::init(path).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub fn config(sub_matches: &ArgMatches) {
    if let Some(remote) = sub_matches.values_of("set-remote") {
        if let [name, url] = remote.collect::<Vec<_>>()[..] {
            match dispatch::add_remote(name, url) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            }
        } else {
            eprintln!("invalid arguments for --set-remote");
        }
    }

    if let Some(name) = sub_matches.value_of("delete-remote") {
        match dispatch::remove_remote(name) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }

    if let Some(auth) = sub_matches.values_of("auth-token") {
        if let [host, token] = auth.collect::<Vec<_>>()[..] {
            match dispatch::set_auth_token(host, token) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
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
                eprintln!("{err}")
            }
        }
    }

    if let Some(email) = sub_matches.value_of("email") {
        match dispatch::set_user_email(email) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }

    if let Some(email) = sub_matches.value_of("default-host") {
        match dispatch::set_default_host(email) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }
}

pub async fn create_remote(sub_matches: &ArgMatches) {
    let namespace = sub_matches.value_of("NAMESPACE").expect("required");
    let name = sub_matches.value_of("NAME").expect("required");
    let host = sub_matches.value_of("HOST").expect("required");

    match dispatch::create_remote(namespace, name, host).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

/// The subcommands for interacting with the remote staging area.
pub async fn remote(sub_matches: &ArgMatches) {
    if let Some(subcommand) = sub_matches.subcommand() {
        match subcommand {
            (STATUS, sub_matches) => {
                remote_status(sub_matches).await;
            }
            (ADD, sub_matches) => {
                remote_add(sub_matches).await;
            }
            (RM, sub_matches) => {
                remote_rm(sub_matches).await;
            }
            (RESTORE, sub_matches) => {
                remote_restore(sub_matches).await;
            }
            (COMMIT, sub_matches) => {
                remote_commit(sub_matches).await;
            }
            (LOG, sub_matches) => {
                remote_log(sub_matches).await;
            }
            (DF, sub_matches) => {
                remote_df(sub_matches).await;
            }
            (DIFF, sub_matches) => {
                remote_diff(sub_matches).await;
            }
            (LS, sub_matches) => {
                remote_ls(sub_matches).await;
            }
            (command, _) => {
                eprintln!("Invalid subcommand: {command}")
            }
        }
    } else if sub_matches.is_present("verbose") {
        dispatch::list_remotes_verbose().expect("Unable to list remotes.");
    } else {
        dispatch::list_remotes().expect("Unable to list remotes.");
    }
}

async fn remote_add(sub_matches: &ArgMatches) {
    let paths: Vec<PathBuf> = sub_matches
        .values_of("files")
        .expect("Must supply files")
        .map(PathBuf::from)
        .collect();

    let opts = AddOpts {
        paths,
        is_remote: true,
        directory: sub_matches.value_of("path").map(PathBuf::from),
    };
    match dispatch::add(opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

fn parse_status_args(sub_matches: &ArgMatches, is_remote: bool) -> StagedDataOpts {
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

    StagedDataOpts {
        skip,
        limit,
        print_all,
        is_remote,
    }
}

async fn remote_status(sub_matches: &ArgMatches) {
    let directory = sub_matches.value_of("path").map(PathBuf::from);

    let is_remote = true;
    let opts = parse_status_args(sub_matches, is_remote);
    match dispatch::status(directory, &opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}");
        }
    }
}

fn parse_pagination_args(sub_matches: &ArgMatches) -> PaginateOpts {
    let page_num: usize = sub_matches
        .value_of("page")
        .unwrap_or(format!("{}", DEFAULT_PAGE_NUM).as_str())
        .parse::<usize>()
        .expect("Page must be a valid integer.");
    let page_size: usize = sub_matches
        .value_of("page-size")
        .unwrap_or(format!("{}", DEFAULT_PAGE_SIZE).as_str())
        .parse::<usize>()
        .expect("Page size must be a valid integer.");

    PaginateOpts {
        page_num,
        page_size,
    }
}

async fn remote_ls(sub_matches: &ArgMatches) {
    let opts = parse_pagination_args(sub_matches);
    let path = sub_matches.value_of("PATH").map(PathBuf::from);
    match dispatch::remote_ls(path, &opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}");
        }
    }
}

pub async fn status(sub_matches: &ArgMatches) {
    let directory = sub_matches.value_of("path").map(PathBuf::from);

    let is_remote = false;
    let opts = parse_status_args(sub_matches, is_remote);
    match dispatch::status(directory, &opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}");
        }
    }
}

async fn remote_log(sub_matches: &ArgMatches) {
    let committish = sub_matches.value_of("COMMITTISH").map(String::from);

    let opts = LogOpts {
        committish,
        remote: true,
    };
    match dispatch::log_commits(opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn log(sub_matches: &ArgMatches) {
    let committish = sub_matches.value_of("COMMITTISH").map(String::from);

    let opts = LogOpts {
        committish,
        remote: false,
    };
    match dispatch::log_commits(opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

fn parse_df_sub_matches(sub_matches: &ArgMatches, is_remote: bool) -> liboxen::df::DFOpts {
    let vstack: Option<Vec<PathBuf>> = if let Some(vstack) = sub_matches.values_of("vstack") {
        let values: Vec<PathBuf> = vstack.map(std::path::PathBuf::from).collect();
        Some(values)
    } else {
        None
    };

    // CSV is easier from the CLI, but JSON is easier from API, so default to CSV here.
    let content_type = sub_matches.value_of("content-type").unwrap_or("csv");

    liboxen::df::DFOpts {
        output: sub_matches.value_of("output").map(std::path::PathBuf::from),
        slice: sub_matches.value_of("slice").map(String::from),
        page_size: sub_matches
            .value_of("page-size")
            .map(String::from)
            .unwrap_or_else(|| String::from(""))
            .parse::<usize>()
            .ok(),
        page: sub_matches
            .value_of("page")
            .map(String::from)
            .unwrap_or_else(|| String::from(""))
            .parse::<usize>()
            .ok(),
        take: sub_matches.value_of("take").map(String::from),
        columns: sub_matches.value_of("columns").map(String::from),
        filter: sub_matches.value_of("filter").map(String::from),
        aggregate: sub_matches.value_of("aggregate").map(String::from),
        col_at: sub_matches.value_of("col-at").map(String::from),
        vstack,
        add_col: sub_matches.value_of("add-col").map(String::from),
        add_row: sub_matches.value_of("add-row").map(String::from),
        delete_row: sub_matches.value_of("delete-row").map(String::from),
        sort_by: sub_matches.value_of("sort").map(String::from),
        unique: sub_matches.value_of("unique").map(String::from),
        content_type: ContentType::from_str(content_type).unwrap(),
        is_remote,
        should_randomize: sub_matches.is_present("randomize"),
        should_reverse: sub_matches.is_present("reverse"),
    }
}

async fn remote_df(sub_matches: &ArgMatches) {
    let path = sub_matches.value_of("DF_SPEC").expect("required");
    let is_remote = true;
    let opts = parse_df_sub_matches(sub_matches, is_remote);

    match dispatch::remote_df(path, opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub fn df(sub_matches: &ArgMatches) {
    let path = sub_matches.value_of("DF_SPEC").expect("required");
    if sub_matches.is_present("schema") || sub_matches.is_present("schema_flat") {
        match dispatch::df_schema(path, sub_matches.is_present("schema_flat")) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    } else {
        let is_remote = false;
        let opts = parse_df_sub_matches(sub_matches, is_remote);

        match dispatch::df(path, opts) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }
}

pub fn schemas(sub_matches: &ArgMatches) {
    if let Some(subcommand) = sub_matches.subcommand() {
        match subcommand {
            ("list", sub_matches) => {
                match dispatch::schema_list(sub_matches.is_present("staged")) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            ("show", sub_matches) => {
                let val = sub_matches.value_of("NAME_OR_HASH").expect("required");

                match dispatch::schema_show(val, sub_matches.is_present("staged")) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            ("name", sub_matches) => {
                let hash = sub_matches.value_of("HASH").expect("required");
                let val = sub_matches.value_of("NAME").expect("required");
                match dispatch::schema_name(hash, val) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            (cmd, _) => {
                eprintln!("Unknown subcommand {cmd}")
            }
        }
    } else {
        match dispatch::schema_list(false) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }
}

pub async fn add(sub_matches: &ArgMatches) {
    let paths: Vec<PathBuf> = sub_matches
        .values_of("files")
        .expect("Must supply files")
        .map(PathBuf::from)
        .collect();

    let opts = AddOpts {
        paths,
        is_remote: false,
        directory: None,
    };
    match dispatch::add(opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn remote_rm(sub_matches: &ArgMatches) {
    let paths: Vec<PathBuf> = sub_matches
        .values_of("files")
        .expect("Must supply files")
        .map(PathBuf::from)
        .collect();

    let opts = RmOpts {
        // The path will get overwritten for each file that is removed
        path: paths.first().unwrap().to_path_buf(),
        staged: sub_matches.is_present("staged"),
        recursive: sub_matches.is_present("recursive"),
        remote: true,
    };

    match dispatch::rm(paths, &opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn rm(sub_matches: &ArgMatches) {
    let paths: Vec<PathBuf> = sub_matches
        .values_of("files")
        .expect("Must supply files")
        .map(PathBuf::from)
        .collect();

    let opts = RmOpts {
        // The path will get overwritten for each file that is removed
        path: paths.first().unwrap().to_path_buf(),
        staged: sub_matches.is_present("staged"),
        recursive: sub_matches.is_present("recursive"),
        remote: false,
    };

    match dispatch::rm(paths, &opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn remote_restore(sub_matches: &ArgMatches) {
    let path = sub_matches.value_of("PATH").expect("required");

    // For now, restore remote just un-stages all the changes done to the file on the remote
    let opts = RestoreOpts {
        path: PathBuf::from(path),
        staged: sub_matches.is_present("staged"),
        is_remote: true,
        source_ref: None,
    };

    match dispatch::restore(opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn restore(sub_matches: &ArgMatches) {
    let path = sub_matches.value_of("PATH").expect("required");

    let opts = if let Some(source) = sub_matches.value_of("source") {
        RestoreOpts {
            path: PathBuf::from(path),
            staged: sub_matches.is_present("staged"),
            is_remote: false,
            source_ref: Some(String::from(source)),
        }
    } else {
        RestoreOpts {
            path: PathBuf::from(path),
            staged: sub_matches.is_present("staged"),
            is_remote: false,
            source_ref: None,
        }
    };

    match dispatch::restore(opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn branch(sub_matches: &ArgMatches) {
    if sub_matches.is_present("all") {
        if let Err(err) = dispatch::list_all_branches().await {
            eprintln!("{err}")
        }
    } else if let Some(remote_name) = sub_matches.value_of("remote") {
        if let Some(branch_name) = sub_matches.value_of("delete") {
            if let Err(err) = dispatch::delete_remote_branch(remote_name, branch_name).await {
                eprintln!("{err}")
            }
        } else if let Err(err) = dispatch::list_remote_branches(remote_name).await {
            eprintln!("{err}")
        }
    } else if let Some(name) = sub_matches.value_of("name") {
        if let Err(err) = dispatch::create_branch(name) {
            eprintln!("{err}")
        }
    } else if let Some(name) = sub_matches.value_of("delete") {
        if let Err(err) = dispatch::delete_branch(name) {
            eprintln!("{err}")
        }
    } else if let Some(name) = sub_matches.value_of("force-delete") {
        if let Err(err) = dispatch::force_delete_branch(name) {
            eprintln!("{err}")
        }
    } else if sub_matches.is_present("show-current") {
        if let Err(err) = dispatch::show_current_branch() {
            eprintln!("{err}")
        }
    } else if let Err(err) = dispatch::list_branches() {
        eprintln!("{err}")
    }
}

pub async fn checkout(sub_matches: &ArgMatches) {
    if sub_matches.is_present("create") {
        let name = sub_matches.value_of("create").expect("required");
        if let Err(err) = dispatch::create_checkout_branch(name) {
            eprintln!("{err}")
        }
    } else if sub_matches.is_present("theirs") {
        let name = sub_matches.value_of("name").expect("required");
        if let Err(err) = dispatch::checkout_theirs(name) {
            eprintln!("{err}")
        }
    } else if sub_matches.is_present("name") {
        let name = sub_matches.value_of("name").expect("required");
        if let Err(err) = dispatch::checkout(name).await {
            eprintln!("{err}")
        }
    } else {
        eprintln!("Err: Usage `oxen checkout <name>`");
    }
}

pub fn merge(sub_matches: &ArgMatches) {
    let branch = sub_matches
        .value_of("BRANCH")
        .unwrap_or(DEFAULT_BRANCH_NAME);
    match dispatch::merge(branch) {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn push(sub_matches: &ArgMatches) {
    let remote = sub_matches
        .value_of("REMOTE")
        .unwrap_or(DEFAULT_REMOTE_NAME);
    let branch = sub_matches
        .value_of("BRANCH")
        .unwrap_or(DEFAULT_BRANCH_NAME);

    if sub_matches.is_present("delete") {
        println!("Delete remote branch {remote}/{branch}");
    } else {
        match dispatch::push(remote, branch).await {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }
}

pub async fn pull(sub_matches: &ArgMatches) {
    let remote = sub_matches
        .value_of("REMOTE")
        .unwrap_or(DEFAULT_REMOTE_NAME);
    let branch = sub_matches
        .value_of("BRANCH")
        .unwrap_or(DEFAULT_BRANCH_NAME);
    match dispatch::pull(remote, branch).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn remote_diff(sub_matches: &ArgMatches) {
    let is_remote = true;
    p_diff(sub_matches, is_remote).await
}

pub async fn diff(sub_matches: &ArgMatches) {
    let is_remote = false;
    p_diff(sub_matches, is_remote).await
}

async fn p_diff(sub_matches: &ArgMatches, is_remote: bool) {
    // First arg is optional
    let file_or_commit_id = sub_matches.value_of("FILE_OR_COMMIT_ID").expect("required");
    let path = sub_matches.value_of("PATH");
    if let Some(path) = path {
        match dispatch::diff(Some(file_or_commit_id), path, is_remote).await {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    } else {
        match dispatch::diff(None, file_or_commit_id, is_remote).await {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }
}

pub async fn clone(sub_matches: &ArgMatches) {
    let url = sub_matches.value_of("URL").expect("required");
    let shallow = sub_matches.is_present("shallow");
    let branch = sub_matches
        .value_of("branch")
        .unwrap_or(DEFAULT_BRANCH_NAME);
    let dst = std::env::current_dir().expect("Could not get current working directory");

    let opts = CloneOpts {
        url: url.to_string(),
        dst,
        shallow,
        branch: branch.to_string(),
    };

    match dispatch::clone(&opts).await {
        Ok(_) => {}
        Err(err) => {
            println!("Err: {err}")
        }
    }
}

pub async fn remote_commit(sub_matches: &ArgMatches) {
    let message = sub_matches.value_of("message").expect("required");

    let is_remote = true;
    match dispatch::commit(message, is_remote).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn commit(sub_matches: &ArgMatches) {
    let message = sub_matches.value_of("message").expect("required");

    let is_remote = false;
    match dispatch::commit(message, is_remote).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub fn migrate(sub_matches: &ArgMatches) {
    let path_str = sub_matches.value_of("PATH").expect("required");
    let path = Path::new(path_str);

    if sub_matches.is_present("all") {
        match command::migrate_all_repos(path) {
            Ok(_) => {}
            Err(err) => {
                println!("Err: {err}")
            }
        }
    } else {
        match LocalRepository::new(path) {
            Ok(repo) => match command::migrate_repo(&repo) {
                Ok(_) => {}
                Err(err) => {
                    println!("Err: {err}")
                }
            },
            Err(err) => {
                println!("Err: {err}")
            }
        }
    }
}

pub fn kvdb_inspect(sub_matches: &ArgMatches) {
    let path_str = sub_matches.value_of("PATH").expect("required");
    let path = Path::new(path_str);
    match dispatch::inspect(path) {
        Ok(_) => {}
        Err(err) => {
            println!("Err: {err}")
        }
    }
}

pub fn read_lines(sub_matches: &ArgMatches) {
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
        println!("{line}");
    }
    println!("Total: {size}");
}
