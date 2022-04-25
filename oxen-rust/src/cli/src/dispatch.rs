use liboxen::api;
use liboxen::command;
use liboxen::util;
use liboxen::config::{AuthConfig, RemoteConfig};
use liboxen::error::OxenError;
use liboxen::index::{Committer, Indexer, Stager};
use liboxen::model::{LocalRepository};

use colored::Colorize;
use std::env;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// CLI Messages
pub const NO_REPO_MSG: &str = "fatal: no oxen repository exists, looking for directory: .oxen ";
pub const RUN_LOGIN_MSG: &str = "fatal: no oxen user, run `oxen login` to login";

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
    let directory = std::fs::canonicalize(PathBuf::from(&path))?;
    command::init(&directory)?;
    println!("🐂 repository initialized at: {:?}", directory);
    Ok(())
}

pub fn clone(url: &str) -> Result<(), OxenError> {
    let auth_cfg = AuthConfig::default()?;
    LocalRepository::clone_remote(auth_cfg, url)?;
    Ok(())
}

pub fn set_remote(url: &str) -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !util::fs::repo_exists(&current_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let mut repo = LocalRepository::from_dir(&current_dir)?;
    repo.set_remote("origin", url);
    repo.save_default()?;
    Ok(())
}

pub fn add(path: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    if !util::fs::repo_exists(&repo_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let stager = Stager::new(&repo_dir)?;
    stager.add(Path::new(path))?;

    Ok(())
}

pub fn push() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    if !util::fs::repo_exists(&repo_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let repository = LocalRepository::from_dir(&repo_dir)?;
    let indexer = Indexer::new(&repository)?;
    let committer = Arc::new(Committer::new(&repo_dir)?);

    indexer.push(&committer)
}

pub fn pull() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    if !util::fs::repo_exists(&repo_dir) {
        let err = NO_REPO_MSG.to_string();
        return Err(OxenError::basic_str(&err));
    }

    let repository = LocalRepository::from_dir(&repo_dir)?;
    let indexer = Indexer::new(&repository)?;
    indexer.pull()
}

pub fn commit(args: Vec<&std::ffi::OsStr>) -> Result<(), OxenError> {
    if AuthConfig::default().is_err() {
        println!("{}", RUN_LOGIN_MSG);
        return Err(OxenError::basic_str(RUN_LOGIN_MSG));
    }

    let repo_dir = env::current_dir().unwrap();
    if !util::fs::repo_exists(&repo_dir) {
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
            // TODO Create a higher level coordinater so that
            // Stager and committer don't have a circular dependency
            let committer = Committer::new(&repo_dir)?;
            let mut stager = Stager::from(committer)?;

            match stager.commit(message) {
                Ok(commit_id) => {
                    println!("Successfully committed id {}", commit_id);
                    stager.unstage()?;
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
    if !util::fs::repo_exists(&repo_dir) {
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
    // Should we let user call this from any directory and look up for parent?
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let repo_status = command::status(&repository)?;

    if repo_status.is_clean(){
        println!("nothing to commit, working tree clean");
        return Ok(());
    }

    // List added files
    if repo_status.has_added_entries() {
        repo_status.print_added();
    }

    if repo_status.has_untracked_entries() {
        repo_status.print_untracked();
    }

    Ok(())
}
