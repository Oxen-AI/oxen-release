// TODO: better define relationship between parse_and_run and dispatch and command
//       * do we want to break each command into a separate file?
//       * what is the common functionality in dispatch right now?
//           * create local repo
//           * printing errors as strings

use crate::cmd_setup::{
    ADD, COMMIT, DF, DIFF, DOWNLOAD, INDEX_DATASET, LOG, LS, METADATA, RESTORE, RM, STATUS,
};
use crate::dispatch;
use clap::ArgMatches;
use liboxen::command::migrate::{
    CacheDataFrameSizeMigration, CreateMerkleTreesMigration, Migrate, PropagateSchemasMigration,
    UpdateVersionFilesMigration,
};
use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_HOST, DEFAULT_REMOTE_NAME};
use liboxen::error::OxenError;
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::model::LocalRepository;
use liboxen::model::{ContentType, EntryDataType};
use liboxen::opts::{AddOpts, CloneOpts, DownloadOpts, InfoOpts, ListOpts, LogOpts, RmOpts};
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
    // The format is namespace/name
    let namespace_name = sub_matches.get_one::<String>("name").expect("required");
    // Default the host to the oxen.ai hub
    let host = sub_matches
        .get_one::<String>("host")
        .map(String::from)
        .unwrap_or(DEFAULT_HOST.to_string());
    // Default scheme
    let scheme = sub_matches
        .get_one::<String>("scheme")
        .map(String::from)
        .unwrap_or("https".to_string());

    // Validate the format
    let parts: Vec<&str> = namespace_name.split('/').collect();
    if parts.len() != 2 {
        eprintln!("Invalid name format. Must be namespace/name");
        return;
    }

    let namespace = parts[0];
    let name = parts[1];
    let empty = !sub_matches.get_flag("add_readme");
    let is_public = sub_matches.get_flag("is_public");

    match dispatch::create_remote(namespace, name, host, scheme, empty, is_public).await {
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
            (INDEX_DATASET, sub_matches) => match remote_index_dataset(sub_matches).await {
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

pub async fn download(sub_matches: &ArgMatches) {
    let opts = DownloadOpts {
        paths: sub_matches
            .get_many::<String>("paths")
            .expect("Must supply paths")
            .map(PathBuf::from)
            .collect(),
        dst: sub_matches
            .get_one::<String>("output")
            .map(PathBuf::from)
            .unwrap_or(PathBuf::from(".")),
        remote: sub_matches
            .get_one::<String>("remote")
            .map(String::from)
            .unwrap_or(DEFAULT_REMOTE_NAME.to_string()),
        host: sub_matches
            .get_one::<String>("host")
            .map(String::from)
            .unwrap_or(DEFAULT_HOST.to_string()),
        branch: sub_matches.get_one::<String>("branch").map(String::from),
        commit_id: sub_matches.get_one::<String>("commit-id").map(String::from),
    };

    // `oxen download $namespace/$repo_name $path`
    match dispatch::download(opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

async fn remote_download(sub_matches: &ArgMatches) {
    let opts = DownloadOpts {
        paths: sub_matches
            .get_many::<String>("paths")
            .expect("Must supply paths")
            .map(PathBuf::from)
            .collect(),
        dst: sub_matches
            .get_one::<String>("output")
            .map(PathBuf::from)
            .unwrap_or(PathBuf::from(".")),
        remote: sub_matches
            .get_one::<String>("remote")
            .map(String::from)
            .unwrap_or(DEFAULT_REMOTE_NAME.to_string()),
        host: sub_matches
            .get_one::<String>("host")
            .map(String::from)
            .unwrap_or(DEFAULT_HOST.to_string()),
        branch: sub_matches.get_one::<String>("branch").map(String::from),
        commit_id: sub_matches.get_one::<String>("commit-id").map(String::from),
    };

    // Make `oxen remote download $path` work
    // TODO: pass in Vec<Path> where the first one could be a remote repo like ox/SQuAD
    match dispatch::remote_download(opts).await {
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

async fn remote_index_dataset(sub_matches: &ArgMatches) -> Result<(), OxenError> {
    let path = PathBuf::from(
        sub_matches
            .get_one::<String>("path")
            .expect("Path is required"),
    );
    match dispatch::remote_index_dataset(path).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
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

async fn remote_ls(sub_matches: &ArgMatches) {
    let opts = ListOpts {
        paths: sub_matches
            .get_many::<String>("paths")
            .expect("Must supply paths")
            .map(PathBuf::from)
            .collect(),
        remote: sub_matches
            .get_one::<String>("remote")
            .map(String::from)
            .unwrap_or(DEFAULT_REMOTE_NAME.to_string()),
        host: sub_matches
            .get_one::<String>("host")
            .map(String::from)
            .unwrap_or(DEFAULT_HOST.to_string()),
        branch_name: sub_matches
            .get_one::<String>("branch")
            .map(String::from)
            .unwrap_or(DEFAULT_BRANCH_NAME.to_string()),
        page_num: sub_matches
            .get_one::<String>("page")
            .expect("Must supply page")
            .parse::<usize>()
            .expect("page must be a valid integer."),
        page_size: sub_matches
            .get_one::<String>("page-size")
            .expect("Must supply page-size")
            .parse::<usize>()
            .expect("page-size must be a valid integer."),
    };

    match dispatch::remote_ls(&opts).await {
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
    let revision = sub_matches.get_one::<String>("revision").map(String::from);

    if path.is_none() {
        eprintln!("Must supply path.");
        return;
    }

    let path = path.unwrap();
    let verbose = sub_matches.get_flag("verbose");
    let output_as_json = sub_matches.get_flag("json");

    let opts = InfoOpts {
        path,
        revision,
        verbose,
        output_as_json,
    };

    match dispatch::info(opts) {
        Ok(_) => {}
        Err(err) => {
            eprintln!("Error getting info: {err}")
        }
    }
}

async fn remote_log(sub_matches: &ArgMatches) {
    let revision = sub_matches.get_one::<String>("REVISION").map(String::from);

    let opts = LogOpts {
        revision,
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
    let revision = sub_matches.get_one::<String>("REVISION").map(String::from);

    let opts = LogOpts {
        revision,
        remote: false,
    };
    match dispatch::log_commits(opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
        }
    }
}

pub async fn fetch(_: &ArgMatches) {
    match dispatch::fetch().await {
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

    let mut content_type = "json";
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
        row: sub_matches
            .get_one::<String>("row")
            .map(|x| x.parse::<usize>().expect("row must be valid int")),
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
        sql: sub_matches.get_one::<String>("sql").map(String::from),
        text2sql: sub_matches.get_one::<String>("text2sql").map(String::from),
        host: sub_matches.get_one::<String>("host").map(String::from),
        unique: sub_matches.get_one::<String>("unique").map(String::from),
        content_type: ContentType::from_str(content_type).unwrap(),
        should_randomize: sub_matches.get_flag("randomize"),
        should_reverse: sub_matches.get_flag("reverse"),
        committed: sub_matches.get_flag("committed"),
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
    if sub_matches.get_flag("schema") || sub_matches.get_flag("schema-flat") {
        match dispatch::df_schema(path, sub_matches.get_flag("schema-flat"), opts) {
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
            ("add", sub_matches) => {
                // Path
                let path = sub_matches.get_one::<String>("PATH");

                // Flags
                let column = sub_matches.get_one::<String>("column");
                let metadata = sub_matches.get_one::<String>("metadata");

                let err_msg = "Must supply a file path, column name and either -m for metadata or -t for data type\n\n  oxen schemas add file.csv -c 'col1' -t 'str'\n";

                if path.is_none() {
                    eprintln!("{err_msg}");
                    return;
                }

                let path = path.unwrap();

                // If a column is supplied, then we need to supply a data type or metadata for that column
                if let Some(column) = column {
                    if let Some(metadata) = metadata {
                        match dispatch::schema_add_column_metadata(path, column, metadata) {
                            Ok(_) => {}
                            Err(err) => {
                                eprintln!("{err}")
                            }
                        }
                    }
                } else {
                    // No column, check if we are just adding metadata to the schema
                    if let Some(metadata) = metadata {
                        match dispatch::schema_add_metadata(path, metadata) {
                            Ok(_) => {}
                            Err(err) => {
                                eprintln!("{err}")
                            }
                        }
                    }
                }
            }
            ("rm", sub_matches) => {
                let val = sub_matches
                    .get_one::<String>("NAME_OR_HASH")
                    .expect("required");

                match dispatch::schema_rm(val, sub_matches.get_flag("staged")) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            (cmd, _) => {
                eprintln!("Unknown schema subcommand {cmd}")
            }
        }
    } else if let Some(schema_ref) = sub_matches.get_one::<String>("SCHEMA_REF") {
        match dispatch::schema_show(
            schema_ref,
            sub_matches.get_flag("staged"),
            !sub_matches.get_flag("flatten"), // default to verbose
        ) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    } else {
        match dispatch::schema_list(sub_matches.get_flag("staged")) {
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

    let all = sub_matches.get_flag("all");
    match dispatch::pull(remote, branch, all).await {
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
    let resource1 = sub_matches
        .get_one::<String>("RESOURCE1")
        .expect("required");
    let resource2 = sub_matches.get_one::<String>("RESOURCE2");

    let (file1, revision1) = parse_file_and_revision(resource1);

    let file1 = PathBuf::from(file1);

    let (file2, revision2) = match resource2 {
        Some(resource) => {
            let (file, revision) = parse_file_and_revision(resource);
            (Some(PathBuf::from(file)), revision)
        }
        None => (None, None),
    };

    let keys: Vec<String> = match sub_matches.get_many::<String>("keys") {
        Some(values) => values.cloned().collect(),
        None => Vec::new(),
    };

    // We changed the external name to compares, need to refactor internals still
    let maybe_targets = sub_matches.get_many::<String>("compares");

    let targets = match maybe_targets {
        Some(values) => values.cloned().collect(),
        None => Vec::new(),
    };

    let output = sub_matches.get_one::<String>("output").map(PathBuf::from);

    match dispatch::diff(
        file1, revision1, file2, revision2, keys, targets, output, is_remote,
    )
    .await
    {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
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
    // Get the name of the repo from the url
    let name = url.split('/').last().unwrap();
    let dst = dst.join(name);

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
        let revision = sub_matches.get_one::<String>("REVISION").map(String::from);

        match LocalRepository::new(path) {
            Ok(repo) => match command::commit_cache::compute_cache(&repo, revision, force).await {
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
pub async fn migrate(sub_matches: &ArgMatches) {
    if let Some((direction, sub_matches)) = sub_matches.subcommand() {
        match direction {
            "up" | "down" => {
                if let Some((migration, sub_matches)) = sub_matches.subcommand() {
                    if migration == UpdateVersionFilesMigration.name() {
                        if let Err(err) =
                            run_migration(&UpdateVersionFilesMigration, direction, sub_matches)
                        {
                            eprintln!("Error running migration: {}", err);
                        }
                    } else if migration == PropagateSchemasMigration.name() {
                        if let Err(err) =
                            run_migration(&PropagateSchemasMigration, direction, sub_matches)
                        {
                            eprintln!("Error running migration: {}", err);
                            std::process::exit(1);
                        }
                    } else if migration == CacheDataFrameSizeMigration.name() {
                        if let Err(err) =
                            run_migration(&CacheDataFrameSizeMigration, direction, sub_matches)
                        {
                            eprintln!("Error running migration: {}", err);
                            std::process::exit(1);
                        }
                    } else if migration == CreateMerkleTreesMigration.name() {
                        if let Err(err) =
                            run_migration(&CreateMerkleTreesMigration, direction, sub_matches)
                        {
                            eprintln!("Error running migration: {}", err);
                            std::process::exit(1);
                        }
                    } else {
                        eprintln!("Invalid migration: {}", migration);
                    }
                }
            }
            command => {
                eprintln!("Invalid subcommand: {}", command);
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

pub fn run_migration(
    migration: &dyn Migrate,
    direction: &str,
    sub_matches: &ArgMatches,
) -> Result<(), OxenError> {
    let path_str = sub_matches.get_one::<String>("PATH").expect("required");
    let path = Path::new(path_str);

    let all = sub_matches.get_flag("all");

    match direction {
        "up" => {
            migration.up(path, all)?;
        }
        "down" => {
            migration.down(path, all)?;
        }
        _ => {
            eprintln!("Invalid migration direction: {}", direction);
        }
    }

    Ok(())
}

pub async fn save(sub_matches: &ArgMatches) {
    // Match on the PATH arg
    let repo_str = sub_matches.get_one::<String>("PATH").expect("Required");
    let output_str = sub_matches.get_one::<String>("output").expect("Required");

    let repo_path = Path::new(repo_str);
    let output_path = Path::new(output_str);

    dispatch::save(repo_path, output_path).expect("Error saving repo backup.");
}

pub async fn load(sub_matches: &ArgMatches) {
    // Match on both SRC_PATH and DEST_PATH
    let src_path_str = sub_matches.get_one::<String>("SRC_PATH").expect("required");
    let dest_path_str = sub_matches
        .get_one::<String>("DEST_PATH")
        .expect("required");
    let no_working_dir = sub_matches.get_flag("no-working-dir");

    let src_path = Path::new(src_path_str);
    let dest_path = Path::new(dest_path_str);

    dispatch::load(src_path, dest_path, no_working_dir).expect("Error loading repo from backup.");
}

fn parse_file_and_revision(file_revision: &str) -> (String, Option<String>) {
    let parts: Vec<&str> = file_revision.split(':').collect();
    if parts.len() == 2 {
        (parts[0].to_string(), Some(parts[1].to_string()))
    } else {
        (parts[0].to_string(), None)
    }
}
