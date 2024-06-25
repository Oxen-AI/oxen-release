// TODO: better define relationship between parse_and_run and dispatch and command
//       * do we want to break each command into a separate file?
//       * what is the common functionality in dispatch right now?
//           * create local repo
//           * printing errors as strings

use crate::cmd;
use crate::cmd::remote::commit::RemoteCommitCmd;
use crate::cmd::RunCmd;
use crate::cmd_setup::{COMMIT, DF, DIFF, DOWNLOAD, LOG, LS, RESTORE, RM, STATUS};
use crate::dispatch;

use clap::ArgMatches;
use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_HOST, DEFAULT_REMOTE_NAME};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::{AddOpts, DownloadOpts, ListOpts};
use std::path::PathBuf;

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
