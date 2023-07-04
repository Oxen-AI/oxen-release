// TODO: better define relationship between parse_and_run and dispatch and command
//       * do we want to break each command into a separate file?
//       * what is the common functionality in dispatch right now?
//           * create local repo
//           * printing errors as strings

use crate::cmd_setup::{ADD, COMMIT, DF, DIFF, DOWNLOAD, LOG, LS, METADATA, RESTORE, RM, STATUS};
use crate::dispatch;
use clap::ArgMatches;
use liboxen::error::OxenError;
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::model::LocalRepository;
use liboxen::model::{ContentType, EntryDataType};
use liboxen::opts::{AddOpts, CloneOpts, LogOpts, PaginateOpts, RmOpts};
use liboxen::util;
use liboxen::{command, opts::RestoreOpts};
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub async fn init(sub_matches: &ArgMatches) {
    let default = String::from(".");
    let path = sub_matches.get_one::<String>("PATH").unwrap_or(&default);

    match dispatch::init(path).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub fn config(sub_matches: &ArgMatches) {
    if let Some(remote) = sub_matches.get_many::<String>("set-remote") {
        if let [name, url] = remote.collect::<Vec<_>>()[..] {
            match dispatch::set_remote(name, url) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            }
        } else {
            eprintln!("invalid arguments for --set-remote");
        }
    }

    if let Some(name) = sub_matches.get_one::<String>("delete-remote") {
        match dispatch::delete_remote(name) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }

    if let Some(auth) = sub_matches.get_many::<String>("auth-token") {
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

    if let Some(name) = sub_matches.get_one::<String>("name") {
        match dispatch::set_user_name(name) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }

    if let Some(email) = sub_matches.get_one::<String>("email") {
        match dispatch::set_user_email(email) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }

    if let Some(email) = sub_matches.get_one::<String>("default-host") {
        match dispatch::set_default_host(email) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }
}

pub async fn create_remote(sub_matches: &ArgMatches) {
    let namespace = sub_matches
        .get_one::<String>("NAMESPACE")
        .expect("required");
    let name = sub_matches.get_one::<String>("NAME").expect("required");
    let host = sub_matches.get_one::<String>("HOST").expect("required");

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
            (DOWNLOAD, sub_matches) => {
                remote_download(sub_matches).await;
            }
            (LS, sub_matches) => {
                remote_ls(sub_matches).await;
            }
            (METADATA, sub_matches) => match remote_metadata(sub_matches).await {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            },
            (command, _) => {
                eprintln!("Invalid subcommand: {command}")
            }
        }
    } else if sub_matches.get_flag("verbose") {
        match dispatch::list_remotes_verbose() {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    } else {
        match dispatch::list_remotes() {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }
}

async fn remote_download(sub_matches: &ArgMatches) {
    let path = sub_matches
        .get_one::<String>("path")
        .expect("Must supply path");

    // Make `oxen remote download $path` work
    match dispatch::remote_download(path).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

async fn remote_add(sub_matches: &ArgMatches) {
    let paths = sub_matches
        .get_many::<String>("files")
        .expect("Must supply files")
        .map(PathBuf::from)
        .collect();

    let opts = AddOpts {
        paths,
        is_remote: true,
        directory: sub_matches.get_one::<String>("path").map(PathBuf::from),
    };
    match dispatch::add(opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

async fn remote_metadata(sub_matches: &ArgMatches) -> Result<(), OxenError> {
    if let Some(subcommand) = sub_matches.subcommand() {
        match subcommand {
            ("list", sub_matches) => {
                remote_metadata_list(sub_matches).await;
            }
            ("aggregate", sub_matches) => {
                remote_metadata_aggregate(sub_matches).await?;
            }
            (command, _) => {
                eprintln!("Invalid subcommand: {command}")
            }
        }
    } else {
        match dispatch::remote_metadata_list_dir(PathBuf::from(".")).await {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }
    Ok(())
}

async fn remote_metadata_aggregate(sub_matches: &ArgMatches) -> Result<(), OxenError> {
    let directory = sub_matches
        .get_one::<String>("path")
        .map(PathBuf::from)
        .unwrap_or(PathBuf::from("."));

    let column = sub_matches
        .get_one::<String>("column")
        .ok_or(OxenError::basic_str("Must supply column"))?;

    match sub_matches.get_one::<String>("type") {
        Some(data_type) => match data_type.parse::<EntryDataType>() {
            Ok(EntryDataType::Dir) => {
                match dispatch::remote_metadata_aggregate_dir(directory, &column).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            Ok(_) => {
                todo!("implement other types")
            }
            Err(err) => {
                let err = format!("{err:?}");
                return Err(OxenError::basic_str(err));
            }
        },
        None => {
            let err = "Must supply type".to_string();
            return Err(OxenError::basic_str(err));
        }
    };

    Ok(())
}

async fn remote_metadata_list(sub_matches: &ArgMatches) {
    let directory = sub_matches
        .get_one::<String>("path")
        .map(PathBuf::from)
        .unwrap_or(PathBuf::from("."));

    match sub_matches.get_one::<String>("type") {
        Some(data_type) => match data_type.parse::<EntryDataType>() {
            Ok(EntryDataType::Dir) => match dispatch::remote_metadata_list_dir(directory).await {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            },
            Ok(EntryDataType::Image) => {
                match dispatch::remote_metadata_list_image(directory).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            Ok(_) => {
                todo!("implement other types")
            }
            Err(err) => {
                eprintln!("{err:?}");
            }
        },
        None => {
            eprintln!("Must supply type");
        }
    }
}

fn parse_status_args(sub_matches: &ArgMatches, is_remote: bool) -> StagedDataOpts {
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

async fn remote_status(sub_matches: &ArgMatches) {
    let directory = sub_matches.get_one::<String>("path").map(PathBuf::from);

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
    let page_num = sub_matches
        .get_one::<String>("page")
        .expect("Must supply page")
        .parse::<usize>()
        .expect("page must be a valid integer.");
    let page_size = sub_matches
        .get_one::<String>("page-size")
        .expect("Must supply page-size")
        .parse::<usize>()
        .expect("page-size must be a valid integer.");

    PaginateOpts {
        page_num,
        page_size,
    }
}

async fn remote_ls(sub_matches: &ArgMatches) {
    let opts = parse_pagination_args(sub_matches);
    let path = sub_matches.get_one::<String>("PATH").map(PathBuf::from);
    match dispatch::remote_ls(path, &opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}");
        }
    }
}

pub async fn status(sub_matches: &ArgMatches) {
    let directory = sub_matches.get_one::<String>("path").map(PathBuf::from);

    let is_remote = false;
    let opts = parse_status_args(sub_matches, is_remote);
    match dispatch::status(directory, &opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}");
        }
    }
}

pub fn info(sub_matches: &ArgMatches) {
    let path = sub_matches.get_one::<String>("path").map(PathBuf::from);

    if path.is_none() {
        eprintln!("Must supply path.");
        return;
    }

    let path = path.unwrap();
    let verbose = sub_matches.get_flag("verbose");
    let output_as_json = sub_matches.get_flag("json");

    match dispatch::info(path, verbose, output_as_json) {
        Ok(_) => {}
        Err(err) => {
            eprintln!("Error getting info: {err}")
        }
    }
}

async fn remote_log(sub_matches: &ArgMatches) {
    let committish = sub_matches
        .get_one::<String>("COMMITTISH")
        .map(String::from);

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
    let committish = sub_matches
        .get_one::<String>("COMMITTISH")
        .map(String::from);

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

fn parse_df_sub_matches(sub_matches: &ArgMatches) -> liboxen::opts::DFOpts {
    let vstack: Option<Vec<PathBuf>> =
        if let Some(vstack) = sub_matches.get_many::<String>("vstack") {
            let values: Vec<PathBuf> = vstack.map(std::path::PathBuf::from).collect();
            Some(values)
        } else {
            None
        };

    // CSV is easier from the CLI, but JSON is easier from API, so default to CSV here.
    let mut content_type = "csv";
    let maybe_content_type = sub_matches.get_one::<String>("content-type");
    if let Some(c) = maybe_content_type {
        content_type = c;
    }

    liboxen::opts::DFOpts {
        output: sub_matches
            .get_one::<String>("output")
            .map(std::path::PathBuf::from),
        delimiter: sub_matches.get_one::<String>("delimiter").map(String::from),
        slice: sub_matches.get_one::<String>("slice").map(String::from),
        page_size: sub_matches
            .get_one::<String>("page-size")
            .map(|x| x.parse::<usize>().expect("page-size must be valid int")),
        page: sub_matches
            .get_one::<String>("page")
            .map(|x| x.parse::<usize>().expect("page must be valid int")),
        head: sub_matches
            .get_one::<String>("head")
            .map(|x| x.parse::<usize>().expect("head must be valid int")),
        tail: sub_matches
            .get_one::<String>("tail")
            .map(|x| x.parse::<usize>().expect("tail must be valid int")),
        take: sub_matches.get_one::<String>("take").map(String::from),
        columns: sub_matches.get_one::<String>("columns").map(String::from),
        filter: sub_matches.get_one::<String>("filter").map(String::from),
        aggregate: sub_matches.get_one::<String>("aggregate").map(String::from),
        col_at: sub_matches.get_one::<String>("col-at").map(String::from),
        vstack,
        add_col: sub_matches.get_one::<String>("add-col").map(String::from),
        add_row: sub_matches.get_one::<String>("add-row").map(String::from),
        delete_row: sub_matches
            .get_one::<String>("delete-row")
            .map(String::from),
        sort_by: sub_matches.get_one::<String>("sort").map(String::from),
        unique: sub_matches.get_one::<String>("unique").map(String::from),
        content_type: ContentType::from_str(content_type).unwrap(),
        should_randomize: sub_matches.get_flag("randomize"),
        should_reverse: sub_matches.get_flag("reverse"),
    }
}

async fn remote_df(sub_matches: &ArgMatches) {
    let path = sub_matches.get_one::<String>("DF_SPEC").expect("required");
    let opts = parse_df_sub_matches(sub_matches);

    match dispatch::remote_df(path, opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub fn df(sub_matches: &ArgMatches) {
    let opts = parse_df_sub_matches(sub_matches);
    let path = sub_matches.get_one::<String>("DF_SPEC").expect("required");
    if sub_matches.get_flag("schema") || sub_matches.get_flag("schema_flat") {
        match dispatch::df_schema(path, sub_matches.get_flag("schema_flat"), opts) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    } else {
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
            ("list", sub_matches) => match dispatch::schema_list(sub_matches.get_flag("staged")) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("{err}")
                }
            },
            ("show", sub_matches) => {
                let val = sub_matches
                    .get_one::<String>("NAME_OR_HASH")
                    .expect("required");

                match dispatch::schema_show(val, sub_matches.get_flag("staged")) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            ("name", sub_matches) => {
                let hash = sub_matches.get_one::<String>("HASH").expect("required");
                let val = sub_matches.get_one::<String>("NAME").expect("required");
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
        .get_many::<String>("files")
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
        .get_many::<String>("files")
        .expect("Must supply files")
        .map(PathBuf::from)
        .collect();

    let opts = RmOpts {
        // The path will get overwritten for each file that is removed
        path: paths.first().unwrap().to_path_buf(),
        staged: sub_matches.get_flag("staged"),
        recursive: sub_matches.get_flag("recursive"),
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
        .get_many::<String>("files")
        .expect("Must supply files")
        .map(PathBuf::from)
        .collect();

    let opts = RmOpts {
        // The path will get overwritten for each file that is removed
        path: paths.first().unwrap().to_path_buf(),
        staged: sub_matches.get_flag("staged"),
        recursive: sub_matches.get_flag("recursive"),
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
    let path = sub_matches.get_one::<String>("PATH").expect("required");

    // For now, restore remote just un-stages all the changes done to the file on the remote
    let opts = RestoreOpts {
        path: PathBuf::from(path),
        staged: sub_matches.get_flag("staged"),
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
    let path = sub_matches.get_one::<String>("PATH").expect("required");

    let opts = if let Some(source) = sub_matches.get_one::<String>("source") {
        RestoreOpts {
            path: PathBuf::from(path),
            staged: sub_matches.get_flag("staged"),
            is_remote: false,
            source_ref: Some(String::from(source)),
        }
    } else {
        RestoreOpts {
            path: PathBuf::from(path),
            staged: sub_matches.get_flag("staged"),
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
    if sub_matches.get_flag("all") {
        if let Err(err) = dispatch::list_all_branches().await {
            eprintln!("{err}")
        }
    } else if let Some(remote_name) = sub_matches.get_one::<String>("remote") {
        if let Some(branch_name) = sub_matches.get_one::<String>("delete") {
            if let Err(err) = dispatch::delete_remote_branch(remote_name, branch_name).await {
                eprintln!("{err}")
            }
        } else if let Err(err) = dispatch::list_remote_branches(remote_name).await {
            eprintln!("{err}")
        }
    } else if let Some(name) = sub_matches.get_one::<String>("name") {
        if let Err(err) = dispatch::create_branch(name) {
            eprintln!("{err}")
        }
    } else if let Some(name) = sub_matches.get_one::<String>("delete") {
        if let Err(err) = dispatch::delete_branch(name) {
            eprintln!("{err}")
        }
    } else if let Some(name) = sub_matches.get_one::<String>("force-delete") {
        if let Err(err) = dispatch::force_delete_branch(name) {
            eprintln!("{err}")
        }
    } else if let Some(name) = sub_matches.get_one::<String>("move") {
        if let Err(err) = dispatch::rename_current_branch(name) {
            eprintln!("{err}")
        }
    } else if sub_matches.get_flag("show-current") {
        if let Err(err) = dispatch::show_current_branch() {
            eprintln!("{err}")
        }
    } else if let Err(err) = dispatch::list_branches() {
        eprintln!("{err}")
    }
}

pub async fn checkout(sub_matches: &ArgMatches) {
    if let Some(name) = sub_matches.get_one::<String>("create") {
        if let Err(err) = dispatch::create_checkout_branch(name) {
            eprintln!("{err}")
        }
    } else if sub_matches.get_flag("ours") {
        let name = sub_matches.get_one::<String>("name");

        if name.is_none() {
            eprintln!("Err: Usage `oxen checkout --ours <name>`");
            return;
        }

        if let Err(err) = dispatch::checkout_ours(name.unwrap()) {
            eprintln!("{err}")
        }
    } else if sub_matches.get_flag("theirs") {
        let name = sub_matches.get_one::<String>("name");

        if name.is_none() {
            eprintln!("Err: Usage `oxen checkout --theirs <name>`");
            return;
        }

        if let Err(err) = dispatch::checkout_theirs(name.unwrap()) {
            eprintln!("{err}")
        }
    } else if let Some(name) = sub_matches.get_one::<String>("name") {
        if let Err(err) = dispatch::checkout(name).await {
            eprintln!("{err}")
        }
    } else {
        eprintln!("Err: Usage `oxen checkout <name>`");
    }
}

pub fn merge(sub_matches: &ArgMatches) {
    let branch = sub_matches
        .get_one::<String>("BRANCH")
        .expect("Must supply a branch");
    match dispatch::merge(branch) {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn push(sub_matches: &ArgMatches) {
    let remote = sub_matches
        .get_one::<String>("REMOTE")
        .expect("Must supply a remote");

    let branch = sub_matches
        .get_one::<String>("BRANCH")
        .expect("Must supply a branch");

    if sub_matches.get_flag("delete") {
        match dispatch::delete_remote_branch(remote, branch).await {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
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
        .get_one::<String>("REMOTE")
        .expect("Must supply a remote");
    let branch = sub_matches
        .get_one::<String>("BRANCH")
        .expect("Must supply a branch");
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
    let file_or_commit_id = sub_matches
        .get_one::<String>("FILE_OR_COMMITTISH")
        .expect("required");
    let path = sub_matches.get_one::<String>("PATH");
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
    let url = sub_matches.get_one::<String>("URL").expect("required");
    let shallow = sub_matches.get_flag("shallow");
    let all = sub_matches.get_flag("all");
    let branch = sub_matches
        .get_one::<String>("branch")
        .expect("Must supply a branch");

    let dst = std::env::current_dir().expect("Could not get current working directory");

    let opts = CloneOpts {
        url: url.to_string(),
        dst,
        shallow,
        all,
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
    let message = sub_matches.get_one::<String>("message").expect("required");

    let is_remote = true;
    match dispatch::commit(message, is_remote).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn commit(sub_matches: &ArgMatches) {
    let message = sub_matches.get_one::<String>("message").expect("required");

    let is_remote = false;
    match dispatch::commit(message, is_remote).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn compute_commit_cache(sub_matches: &ArgMatches) {
    let path_str = sub_matches.get_one::<String>("PATH").expect("required");
    let path = Path::new(path_str);

    let force = sub_matches.get_flag("force");

    if sub_matches.get_flag("all") {
        match command::commit_cache::compute_cache_on_all_repos(path, force).await {
            Ok(_) => {}
            Err(err) => {
                println!("Err: {err}")
            }
        }
    } else {
        let committish = sub_matches
            .get_one::<String>("COMMITTISH")
            .map(String::from);

        match LocalRepository::new(path) {
            Ok(repo) => {
                match command::commit_cache::compute_cache(&repo, committish, force).await {
                    Ok(_) => {}
                    Err(err) => {
                        println!("Err: {err}")
                    }
                }
            }
            Err(err) => {
                println!("Err: {err}")
            }
        }
    }
}

pub fn kvdb_inspect(sub_matches: &ArgMatches) {
    let path_str = sub_matches.get_one::<String>("PATH").expect("required");
    let path = Path::new(path_str);
    match dispatch::inspect(path) {
        Ok(_) => {}
        Err(err) => {
            println!("Err: {err}")
        }
    }
}

pub fn read_lines(sub_matches: &ArgMatches) {
    let path_str = sub_matches.get_one::<String>("PATH").expect("required");
    let start = sub_matches
        .get_one::<String>("START")
        .expect("Must supply START")
        .parse::<usize>()
        .expect("START must be a valid integer.");
    let length = sub_matches
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
}
