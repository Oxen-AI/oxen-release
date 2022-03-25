use crate::api;
use std::env;
use std::io::{self, BufRead};
use std::path::PathBuf;

use crate::cli::indexer::Indexer;
use crate::config::{AuthConfig, RemoteConfig};
use crate::error::OxenError;
use crate::model::Repository;

const NO_REPO_MSG: &str = "fatal: no oxen repository exists, looking for directory: .oxen ";

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

pub fn add(path: &str) {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let indexer = Indexer::new(&current_dir);
    let directory = PathBuf::from(&path);
    indexer.add_files(&directory)
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
    if name.ends_with('/') { name.pop(); }
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
        println!("{}", NO_REPO_MSG);
        return Err(OxenError::basic_str(NO_REPO_MSG));
    }

    let indexer = Indexer::new(&current_dir);
    indexer.status()
}
