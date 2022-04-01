use crate::api;
use colored::Colorize;
use std::env;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};

use crate::cli::Indexer;
use crate::cli::Stager;
use crate::config::{AuthConfig, RemoteConfig};
use crate::error::OxenError;
use crate::model::Repository;

const NO_REPO_MSG: &str = "fatal: no oxen repository exists, looking for directory: .oxen ";
const STAGED_DB_DIR: &str = "staged";

pub fn login() -> Result<(), OxenError> {
    println!("🐂 Login\n\nEnter your email:");
    let mut email = String::new();
    let stdin = io::stdin();
    stdin.lock().read_line(&mut email).unwrap();
    println!("Enter your password:");
    let password = rpassword::read_password().unwrap();

    // RemoteConfig tells us where to login
    let remote_config = RemoteConfig::new()?;
    let user = api::login(&remote_config, email.trim(), password.trim())?;

    // AuthConfig is saved next to it with user token
    let auth_config = remote_config.to_auth(&user);
    auth_config.save_default()?;

    Ok(())
}

pub fn init(path: &str) {
    let directory = PathBuf::from(&path);
    let indexer = Indexer::new(&directory);
    indexer.init()
}

pub fn clone(url: &str) -> Result<(), OxenError> {
    let auth_cfg = AuthConfig::default()?;
    Repository::clone_remote(&auth_cfg, url)?;
    Ok(())
}

pub fn add(path: &str) -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        let err = format!("{}", NO_REPO_MSG);
        return Err(OxenError::basic_str(&err));
    }

    let indexer = Indexer::new(&current_dir);
    let stage_index_dir = Path::new(&indexer.hidden_dir).join(Path::new(STAGED_DB_DIR));
    let stager = Stager::new(&stage_index_dir, &current_dir)?;
    stager.add(Path::new(path))?;

    Ok(())
}

pub fn push(directory: &str) -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let indexer = Indexer::new(&current_dir);

    // Remove trailing slash from directory names
    let mut name = String::from(directory);
    if name.ends_with('/') {
        name.pop();
    }
    indexer.create_dataset_if_not_exists(&name)?;
    indexer.push(&name)
}

pub fn pull() -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let indexer = Indexer::new(&current_dir);
    indexer.pull()
}

pub fn pull_remote(remote: &str) -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    Err(OxenError::basic_str(&format!(
        "TODO: Implement pull_remote {}",
        remote
    )))
}

pub fn pull_remote_branch(remote: &str, branch: &str) -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    Err(OxenError::basic_str(&format!(
        "TODO: Implement pull_remote_branch {} {}",
        remote, branch
    )))
}

pub fn list_datasets() -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let indexer = Indexer::new(&current_dir);
    let datasets = indexer.list_datasets()?;
    for dataset in datasets.iter() {
        println!("{}", dataset.name);
    }
    Ok(())
}

pub fn create(args: Vec<&std::ffi::OsStr>) -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        return Err(OxenError::basic_str(NO_REPO_MSG));
    }

    let config = AuthConfig::default()?;
    let err_str = "Must supply create with a type. Ex:\n\noxen create -d \"my_dataset\"";
    if args.len() != 2 {
        Err(OxenError::basic_str(err_str))
    } else {
        let flag = args[0];
        let value = args[1];
        p_create(&config, flag, value)
    }
}

fn p_create(
    config: &AuthConfig,
    flag: &std::ffi::OsStr,
    value: &std::ffi::OsStr,
) -> Result<(), OxenError> {
    match flag.to_str().unwrap() {
        "-d" => {
            let name = value.to_str().unwrap_or_default();
            println!("Creating dataset name [{}]", name);
            println!("TODO!!");
            Ok(())
        }
        "-r" => {
            let name = value.to_str().unwrap_or_default();
            let repository = api::repositories::create(config, name)?;
            println!("Created repository name [{}]", repository.name);
            Ok(())
        }
        _ => {
            let err = format!("oxen create used with unknown flag {:?}", flag);
            Err(OxenError::Basic(err))
        }
    }
}

pub fn commit(args: Vec<&std::ffi::OsStr>) -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return Err(OxenError::basic_str(NO_REPO_MSG));
    }

    let err_str = "Must supply a commit message with -m. Ex:\n\noxen commit -m \"Adding data\"";
    if args.len() != 2 {
        let err = err_str.to_string();
        return Err(OxenError::Basic(err));
    }

    let err_str = "Must supply a commit message with -m. Ex:\n\noxen commit -m \"Adding data\"";
    let flag = args[0];
    let value = args[1];
    match flag.to_str().unwrap() {
        "-m" => {
            let message = value.to_str().unwrap_or_default();
            println!("Committing with msg [{}]", message);
            let indexer = Indexer::new(&current_dir);
            indexer.commit(message)?;
            Ok(())
        }
        _ => {
            eprintln!("{}", err_str);
            Err(OxenError::basic_str(err_str))
        }
    }
}

pub fn status() -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        let err = format!("{}", NO_REPO_MSG);
        return Err(OxenError::basic_str(&err));
    }

    let indexer = Indexer::new(&current_dir);
    let stage_index_dir = Path::new(&indexer.hidden_dir).join(Path::new(STAGED_DB_DIR));
    let stager = Stager::new(&stage_index_dir, &current_dir)?;

    let added_directories = stager.list_added_directories()?;
    let added_files = stager.list_added_files()?;
    let untracked_directories = stager.list_untracked_directories()?;
    let untracked_files = stager.list_untracked_files()?;

    if added_directories.is_empty()
        && added_files.is_empty()
        && untracked_files.is_empty()
        && untracked_directories.is_empty()
    {
        println!("nothing to commit, working tree clean");
        return Ok(());
    }

    // List added files
    if !added_directories.is_empty() || !added_files.is_empty() {
        println!("Changes to be committed:");
        for (dir, count) in added_directories.iter() {
            let added_file_str = format!("  added:  {}/", dir.to_str().unwrap()).green();
            let num_files_str = format!("with {} files", count);
            println!("{} {}", added_file_str, num_files_str);
        }

        for file in added_files.iter() {
            if let Some(parent) = file.parent() {
                // If it is a top level file
                if parent == current_dir {
                    // Make sure we can grab the filename
                    if let Some(filename) = file.file_name() {
                        let added_file_str =
                            format!("  added:  {}", filename.to_str().unwrap()).green();
                        println!("{}", added_file_str);
                    }
                }
            }
        }

        print!("\n");
    }

    if !untracked_directories.is_empty() || !untracked_files.is_empty() {
        println!("Untracked files:");
        println!("  (use \"oxen add <file>...\" to update what will be committed)");

        // List untracked directories
        for (dir, count) in untracked_directories.iter() {
            // Make sure we can grab the filename
            if let Some(filename) = dir.file_name() {
                let added_file_str = format!("  {}/", filename.to_str().unwrap()).red();
                let num_files_str = match count {
                    1 => {
                        format!("with {} file", count)
                    }
                    _ => {
                        format!("with {} files", count)
                    }
                };

                println!("{} {}", added_file_str, num_files_str);
            }
        }

        // List untracked files
        for file in untracked_files.iter() {
            // Make sure it has a parent (it should... unless you are tracking an entire OS from /)
            if let Some(parent) = file.parent() {
                // If it is a top level file
                if parent == current_dir {
                    // Make sure we can grab the filename
                    if let Some(filename) = file.file_name() {
                        let added_file_str = format!("{}", filename.to_str().unwrap()).red();
                        println!("  {}", added_file_str);
                    }
                }
            }
        }
        print!("\n");
    }

    Ok(())
}
