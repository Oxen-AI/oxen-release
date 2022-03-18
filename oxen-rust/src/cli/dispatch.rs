use crate::api;
use std::env;
use std::io::{self, BufRead};
use std::path::PathBuf;

use crate::cli::indexer::Indexer;
use crate::config::oxen_config::OxenConfig;
use crate::error::OxenError;

const NO_REPO_MSG: &str = "fatal: no oxen repository exists, looking for directory: .oxen ";

pub fn login() -> Result<(), OxenError> {
    println!("🐂 Login\n\nEnter your email:");
    let mut email = String::new();
    let stdin = io::stdin();
    stdin.lock().read_line(&mut email).unwrap();
    println!("Enter your password:");
    let password = rpassword::read_password().unwrap();

    let mut config = OxenConfig::new()?;
    let user = api::login(&config, email.trim(), password.trim())?;
    config.add_user(&user).save_default()?;

    Ok(())
}

pub fn init(path: &str) {
    let directory = PathBuf::from(&path);
    let indexer = Indexer::new(&directory);
    indexer.init()
}

pub fn clone(url: &str) {
    let _indexer = Indexer::clone(url);
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

pub fn push(directory: &str) {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let mut indexer = Indexer::new(&current_dir);

    // Must login to get access token
    match indexer.login() {
        Ok(_) => {
            // Create remote dataset
            indexer.create_dataset_if_not_exists(directory);
            match indexer.push(directory) {
                Ok(_) => {
                    println!("Done.")
                }
                Err(err) => {
                    eprintln!("Error: {}", err)
                }
            }
        }
        Err(err) => {
            eprintln!("Error: {}", err)
        }
    }
}

pub fn list_datasets() {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let mut indexer = Indexer::new(&current_dir);
    match indexer.login() {
        Ok(_) => match indexer.list_datasets() {
            Ok(datasets) => {
                for dataset in datasets.iter() {
                    println!("{}", dataset.name);
                }
            }
            Err(err) => {
                eprintln!("Indexer couldn't list datasets: {}", err)
            }
        },
        Err(err) => {
            eprintln!("Indexer couldn't log in: {}", err)
        }
    }
}

pub fn create(args: Vec<&std::ffi::OsStr>) -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        return Err(OxenError::Basic(String::from(NO_REPO_MSG)));
    }

    let config = OxenConfig::default()?;

    let err_str = "Must supply create with a type. Ex:\n\noxen create -d \"my_dataset\"";
    if args.len() != 2 {
        Err(OxenError::Basic(String::from(err_str)))
    } else {
        let flag = args[0];
        match flag.to_str().unwrap() {
            "-d" => {
                let name_arg = args[1];
                match name_arg.to_str() {
                    Some(name) => {
                        println!("Creating dataset name [{}]", name);
                        println!("TODO!!");
                        Err(OxenError::Basic(String::from("TODO: create dataset")))
                    }
                    None => {
                        let err = format!("Invalid dataset name: \"{:?}\"", name_arg);
                        Err(OxenError::Basic(err))
                    }
                }
            }
            "-r" => {
                let name_arg = args[1];
                match name_arg.to_str() {
                    Some(name) => {
                        println!("Creating repository...");
                        let repository = api::repositories::create(&config, name)?;
                        println!("Created repository name [{}]", repository.name);
                        Ok(())
                    }
                    None => {
                        let err = format!("Invalid dataset name: \"{:?}\"", name_arg);
                        Err(OxenError::Basic(err))
                    }
                }
            }
            _ => {
                let err = format!("oxen create used with unknown flag {:?}", flag);
                Err(OxenError::Basic(err))
            }
        }
    }
}

pub fn commit(args: Vec<&std::ffi::OsStr>) -> Result<(), OxenError> {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return Err(OxenError::Basic(String::from(NO_REPO_MSG)));
    }

    let err_str = "Must supply a commit message with -m. Ex:\n\noxen commit -m \"Adding data\"";
    if args.len() != 2 {
        let err = err_str.to_string();
        Err(OxenError::Basic(err))
    } else {
        let flag = args[0];
        match flag.to_str().unwrap() {
            "-m" => {
                let msg_arg = args[1];
                match msg_arg.to_str() {
                    Some(message) => {
                        println!("Committing with msg [{}]", message);
                        let indexer = Indexer::new(&current_dir);
                        indexer.commit(message);
                        Ok(())
                    }
                    None => {
                        let err = format!("Invalid commit message: \"{:?}\"", msg_arg);
                        Err(OxenError::Basic(err))
                    }
                }
            }
            _ => {
                eprintln!("{}", err_str);
                Err(OxenError::Basic(String::from(err_str)))
            }
        }
    }
}

pub fn status() {
    let current_dir = env::current_dir().unwrap();
    if !Indexer::repo_exists(&current_dir) {
        println!("{}", NO_REPO_MSG);
        return;
    }

    let indexer = Indexer::new(&current_dir);
    indexer.status()
}
