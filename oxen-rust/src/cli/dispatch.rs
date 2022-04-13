use crate::api;
use colored::Colorize;
use std::env;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cli::{Committer, Indexer, Stager};

use crate::config::{AuthConfig, RemoteConfig};
use crate::error::OxenError;
use crate::model::Repository;

const NO_REPO_MSG: &str = "fatal: no oxen repository exists, looking for directory: .oxen ";
const RUN_LOGIN_MSG: &str = "fatal: no oxen user, run `oxen login` to login";

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

pub fn init(path: &str) -> Result<(), OxenError> {
    let directory = PathBuf::from(&path);

    let indexer = Indexer::new(&directory);
    indexer.init()?;

    Ok(())
}

pub fn clone(url: &str) -> Result<(), OxenError> {
    let auth_cfg = AuthConfig::default()?;
    Repository::clone_remote(&auth_cfg, url)?;
    Ok(())
}

pub fn set_remote(url: &str) -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let mut indexer = Indexer::new(&current_dir);
    indexer.set_remote(url)
}

pub fn add(path: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&repo_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let stager = Stager::new(&repo_dir)?;
    stager.add(Path::new(path))?;

    Ok(())
}

pub fn push() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&repo_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let indexer = Indexer::new(&repo_dir);
    let committer = Committer::new(&repo_dir)?;

    indexer.push(&committer)
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
    if AuthConfig::default().is_err() {
        println!("{}", RUN_LOGIN_MSG);
        return Err(OxenError::basic_str(RUN_LOGIN_MSG));
    }

    let repo_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&repo_dir) {
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
            // We might need a higher level coordinater so that
            // Stager and committer don't have a circular dependency
            let committer = Arc::new(Committer::new(&repo_dir)?);
            let stager = Stager::from(Arc::clone(&committer))?;

            match committer.commit(&stager, message) {
                Ok(commit_id) => {
                    println!("Successfully committed id {}", commit_id);
                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
        _ => {
            eprintln!("{}", err_str);
            Err(OxenError::basic_str(err_str))
        }
    }
}

pub fn log_commits() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&repo_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let committer = Arc::new(Committer::new(&repo_dir)?);

    for commit in committer.list_commits()? {
        let commit_id_str = format!("commit {}", commit.id).yellow();
        println!("{}\n", commit_id_str);
        println!("Author: {}", commit.author);
        println!("Date:   {}\n", commit.date);
        println!("    {}\n", commit.message);
    }

    Ok(())
}

pub fn status() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&repo_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let committer = Arc::new(Committer::new(&repo_dir)?);
    let stager = Stager::from(Arc::clone(&committer))?;

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
            // Make sure we can grab the filename
            let added_file_str = format!("  added:  {}/", dir.to_str().unwrap()).green();
            let num_files_str = match count {
                1 => {
                    format!("with untracked {} file\n", count)
                }
                0 => {
                    // Skip since we don't have any untracked files in this dir
                    String::from("")
                }
                _ => {
                    format!("with untracked {} files\n", count)
                }
            };
            print!("{} {}", added_file_str, num_files_str);
        }

        for file in added_files.iter() {
            let mut break_both = false;
            for (dir, _size) in added_directories.iter() {
                // println!("checking if file {:?} starts with {:?}", file, dir);
                if file.starts_with(&dir) {
                    break_both = true;
                    continue;
                }
            }

            if break_both {
                continue;
            }

            let added_file_str = format!("  added:  {}", file.to_str().unwrap()).green();
            println!("{}", added_file_str);
        }

        println!();
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
                        format!("with untracked {} file\n", count)
                    }
                    0 => {
                        // Skip since we don't have any untracked files in this dir
                        String::from("")
                    }
                    _ => {
                        format!("with untracked {} files\n", count)
                    }
                };

                if !num_files_str.is_empty() {
                    print!("{} {}", added_file_str, num_files_str);
                }
            }
        }

        // List untracked files
        for file in untracked_files.iter() {
            let mut break_both = false;
            for (dir, _size) in untracked_directories.iter() {
                // println!("checking if file {:?} starts with {:?}", file, dir);
                if file.starts_with(&dir) {
                    break_both = true;
                    continue;
                }
            }

            if break_both {
                continue;
            }

            let added_file_str = file.to_str().unwrap().to_string().red();
            println!("  {}", added_file_str);
        }
        println!();
    }

    Ok(())
}
