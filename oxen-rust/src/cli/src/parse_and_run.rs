// TODO: better define relationship between parse_and_run and dispatch and command
//       * do we want to break each command into a separate file?
//       * what is the common functionality in dispatch right now?
//           * create local repo
//           * printing errors as strings

use crate::cmd;
use crate::cmd::remote::commit::RemoteCommitCmd;
use crate::cmd::BranchCmd;
use crate::cmd::RunCmd;
use crate::cmd_setup::{COMMIT, DF, DIFF, DOWNLOAD, LOG, LS, METADATA, RESTORE, RM, STATUS};
use crate::dispatch;

use clap::ArgMatches;
use liboxen::command;
use liboxen::command::migrate::{
    AddDirectoriesToCacheMigration, CacheDataFrameSizeMigration, CreateMerkleTreesMigration,
    Migrate, PropagateSchemasMigration, UpdateVersionFilesMigration,
};
use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_HOST, DEFAULT_REMOTE_NAME};
use liboxen::error::OxenError;
use liboxen::model::EntryDataType;
use liboxen::model::LocalRepository;
use liboxen::opts::{AddOpts, DownloadOpts, InfoOpts, ListOpts, RmOpts, UploadOpts};
use std::path::{Path, PathBuf};

/// The subcommands for interacting with the remote staging area.
pub async fn remote(sub_matches: &ArgMatches) {
    if let Some(subcommand) = sub_matches.subcommand() {
        match subcommand {
            (STATUS, sub_matches) => {
                let cmd = cmd::remote::RemoteStatusCmd {};
                match cmd.run(sub_matches).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            ("add", sub_matches) => {
                remote_add(sub_matches).await;
            }
            (RM, sub_matches) => {
                let cmd = cmd::remote::RemoteRmCmd {};
                match cmd.run(sub_matches).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            (RESTORE, sub_matches) => {
                let cmd = cmd::remote::RemoteRestoreCmd {};
                match cmd.run(sub_matches).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            (COMMIT, sub_matches) => {
                let cmd = RemoteCommitCmd {};
                match cmd.run(sub_matches).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            (LOG, sub_matches) => {
                let cmd = cmd::remote::RemoteLogCmd {};
                match cmd.run(sub_matches).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            (DF, sub_matches) => {
                let cmd = cmd::remote::RemoteDfCmd {};
                match cmd.run(sub_matches).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
            (DIFF, sub_matches) => {
                let cmd = cmd::remote::RemoteDiffCmd {};
                match cmd.run(sub_matches).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
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
        let repo = LocalRepository::from_current_dir().expect("Could not find a repository");
        match list_remotes_verbose(&repo) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    } else {
        let repo = LocalRepository::from_current_dir().expect("Could not find a repository");
        match list_remotes(&repo) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{err}")
            }
        }
    }
}

pub fn list_remotes(repo: &LocalRepository) -> Result<(), OxenError> {
    for remote in repo.remotes.iter() {
        println!("{}", remote.name);
    }

    Ok(())
}

pub fn list_remotes_verbose(repo: &LocalRepository) -> Result<(), OxenError> {
    for remote in repo.remotes.iter() {
        println!("{}\t{}", remote.name, remote.url);
    }

    Ok(())
}

pub async fn upload(sub_matches: &ArgMatches) {
    let opts = UploadOpts {
        paths: sub_matches
            .get_many::<String>("paths")
            .expect("Must supply paths")
            .map(PathBuf::from)
            .collect(),
        dst: sub_matches
            .get_one::<String>("dst")
            .map(PathBuf::from)
            .unwrap_or(PathBuf::from(".")),
        message: sub_matches
            .get_one::<String>("message")
            .map(String::from)
            .expect("Must supply a commit message"),
        branch: sub_matches.get_one::<String>("branch").map(String::from),
        remote: sub_matches
            .get_one::<String>("remote")
            .map(String::from)
            .unwrap_or(DEFAULT_REMOTE_NAME.to_string()),
        host: sub_matches
            .get_one::<String>("host")
            .map(String::from)
            .unwrap_or(DEFAULT_HOST.to_string()),
    };

    // `oxen upload $namespace/$repo_name $path`
    match dispatch::upload(opts).await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
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
        revision: sub_matches.get_one::<String>("revision").map(String::from),
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
        revision: sub_matches.get_one::<String>("revision").map(String::from),
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

async fn remote_ls(sub_matches: &ArgMatches) {
    let paths = sub_matches.get_many::<String>("paths");

    let paths = if let Some(paths) = paths {
        paths.map(PathBuf::from).collect()
    } else {
        vec![PathBuf::from(".")]
    };

    let opts = ListOpts {
        paths,
        remote: sub_matches
            .get_one::<String>("remote")
            .map(String::from)
            .unwrap_or(DEFAULT_REMOTE_NAME.to_string()),
        host: sub_matches
            .get_one::<String>("host")
            .map(String::from)
            .unwrap_or(DEFAULT_HOST.to_string()),
        revision: sub_matches
            .get_one::<String>("revision")
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

pub async fn fetch(_: &ArgMatches) {
    match dispatch::fetch().await {
        Ok(_) => {}
        Err(err) => {
            eprintln!("{err}")
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

pub async fn push(sub_matches: &ArgMatches) {
    let remote = sub_matches
        .get_one::<String>("REMOTE")
        .expect("Must supply a remote");

    let branch = sub_matches
        .get_one::<String>("BRANCH")
        .expect("Must supply a branch");

    if sub_matches.get_flag("delete") {
        let repo =
            LocalRepository::from_current_dir().expect("Could not get current working directory");
        BranchCmd
            .delete_remote_branch(&repo, remote, branch)
            .await
            .expect("Could not delete remote branch");
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
                    } else if migration == AddDirectoriesToCacheMigration.name() {
                        if let Err(err) =
                            run_migration(&AddDirectoriesToCacheMigration, direction, sub_matches)
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
