use liboxen::api;
use liboxen::command;
use liboxen::config::RemoteConfig;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use colored::Colorize;
use std::env;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};

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
    let dst = std::env::current_dir()?;
    command::clone(url, &dst)?;
    Ok(())
}

pub fn create_remote() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    let remote = command::create_remote(&repo)?;
    println!("Remote url: {}", remote.url);

    Ok(())
}

pub fn set_remote(name: &str, url: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let mut repo = LocalRepository::from_dir(&repo_dir)?;

    command::set_remote(&mut repo, name, url)?;

    Ok(())
}

pub fn add(path: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::add(&repository, Path::new(path))?;

    Ok(())
}

pub fn push(remote: &str, branch: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::push_remote_branch(&repository, remote, branch)?;
    Ok(())
}

pub fn pull(remote: &str, branch: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::pull_remote_branch(&repository, remote, branch)?;
    Ok(())
}

pub fn commit(args: Vec<&std::ffi::OsStr>) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

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
            println!("Committing with message: {}", message);
            command::commit(&repo, message)?;
            Ok(())
        }
        _ => {
            eprintln!("{}", err_str);
            Err(OxenError::basic_str(err_str))
        }
    }
}

pub fn log_commits() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    for commit in command::log(&repository)? {
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

    if let Some(current_branch) = command::current_branch(&repository)? {
        println!(
            "On branch {} -> {}\n",
            current_branch.name, current_branch.commit_id
        );
    } else { 
        let head = command::head_commit(&repository)?;
        println!(
            "You are in 'detached HEAD' state.\nHEAD is now at {} {}\n",
            head.id, head.message
        );
    }

    repo_status.print();

    Ok(())
}

pub fn create_branch(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::create_branch(&repository, name)?;
    Ok(())
}

pub fn checkout(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::checkout(&repository, name)?;
    Ok(())
}

pub fn create_checkout_branch(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::create_checkout_branch(&repository, name)?;
    Ok(())
}

pub fn list_branches() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let branches = command::list_branches(&repository)?;

    for branch in branches.iter() {
        if branch.is_head {
            let branch_str = format!("* {}", branch.name).green();
            println!("{}", branch_str)
        } else {
            println!("{}", branch.name)
        }
    }

    Ok(())
}

pub fn inspect(path: &Path) -> Result<(), OxenError> {
    command::inspect(path)
}
