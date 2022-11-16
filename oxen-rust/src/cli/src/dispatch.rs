use liboxen::command;
use liboxen::config::UserConfig;
use liboxen::df::df_opts::DFOpts;
use liboxen::error;
use liboxen::error::OxenError;
use liboxen::model::schema;
use liboxen::model::LocalRepository;
use liboxen::opts::RestoreOpts;
use liboxen::util;

use colored::Colorize;
use std::env;
use std::path::{Path, PathBuf};

pub fn init(path: &str) -> Result<(), OxenError> {
    let directory = std::fs::canonicalize(PathBuf::from(&path))?;
    command::init(&directory)?;
    println!("🐂 repository initialized at: {:?}", directory);
    Ok(())
}

pub async fn clone(url: &str) -> Result<(), OxenError> {
    let dst = std::env::current_dir()?;
    command::clone(url, &dst).await?;
    Ok(())
}

pub async fn create_remote(namespace: &str, name: &str, host: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    let remote_repo = command::create_remote(&repo, namespace, name, host).await?;
    println!(
        "Remote created for {}\n\noxen remote add origin {}",
        name, remote_repo.remote.url
    );

    Ok(())
}

pub fn add_remote(name: &str, url: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let mut repo = LocalRepository::from_dir(&repo_dir)?;

    command::add_remote(&mut repo, name, url)?;

    Ok(())
}

pub fn remove_remote(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let mut repo = LocalRepository::from_dir(&repo_dir)?;

    command::remove_remote(&mut repo, name)?;

    Ok(())
}

pub fn list_remotes() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    for remote in repo.remotes.iter() {
        println!("{}", remote.name);
    }

    Ok(())
}

pub fn list_remotes_verbose() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    for remote in repo.remotes.iter() {
        println!("{}\t{}", remote.name, remote.url);
    }

    Ok(())
}

pub fn set_auth_token(host: &str, token: &str) -> Result<(), OxenError> {
    let mut config = UserConfig::default_or_create()?;
    config.add_host_auth_token(host, token);
    config.save_default()?;
    println!("Authentication token set for host: {}", host);
    Ok(())
}

pub fn set_user_name(name: &str) -> Result<(), OxenError> {
    let mut config = UserConfig::default_or_create()?;
    config.name = String::from(name);
    config.save_default()?;
    Ok(())
}

pub fn set_user_email(email: &str) -> Result<(), OxenError> {
    let mut config = UserConfig::default_or_create()?;
    config.email = String::from(email);
    config.save_default()?;
    Ok(())
}

pub fn add(path: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::add(&repository, Path::new(path))?;

    Ok(())
}

pub fn restore(opts: RestoreOpts) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::restore(&repository, opts)?;

    Ok(())
}

pub fn add_tabular(path: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::add_tabular(&repository, Path::new(path))?;

    Ok(())
}

pub async fn push(remote: &str, branch: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::push_remote_branch(&repository, remote, branch).await?;
    Ok(())
}

pub async fn pull(remote: &str, branch: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::pull_remote_branch(&repository, remote, branch).await?;
    Ok(())
}

pub fn diff(commit_id: Option<&str>, path: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let path = Path::new(path);

    let result = command::diff(&repository, commit_id, path)?;
    println!("{result}");
    Ok(())
}

pub fn merge(branch: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::merge(&repository, branch)?;
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
        println!(
            "Date:   {}\n",
            commit.date.format(util::oxen_date_format::FORMAT)
        );
        println!("    {}\n", commit.message);
    }

    Ok(())
}

pub fn status(skip: usize, limit: usize, print_all: bool) -> Result<(), OxenError> {
    // Should we let user call this from any directory and look up for parent?
    let current_dir = env::current_dir().unwrap();
    let repo_dir = util::fs::get_repo_root(&current_dir).expect(error::NO_REPO_FOUND);

    let repository = LocalRepository::from_dir(&repo_dir)?;
    let repo_status = command::status_from_dir(&repository, &current_dir)?;

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

    repo_status.print_stdout_with_params(skip, limit, print_all);

    Ok(())
}

pub fn df<P: AsRef<Path>>(input: P, opts: DFOpts) -> Result<(), OxenError> {
    command::df(input, opts)?;
    Ok(())
}

pub fn df_schema<P: AsRef<Path>>(input: P) -> Result<(), OxenError> {
    let result = command::df_schema(input)?;
    println!("{}", result);
    Ok(())
}

pub fn schema_show(
    val: &str,
    verbose: bool,
) -> Result<(LocalRepository, Option<schema::Schema>), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;
    let schema = command::schema_show(&repo, None, val)?;
    if let Some(schema) = schema {
        if let Some(name) = &schema.name {
            if verbose {
                println!("{}\n{}", name, schema);
            }
            Ok((repo, Some(schema)))
        } else {
            if verbose {
                println!(
                    "Schema has no name, to name run:\n\n  oxen schemas name {} \"my_schema\"\n\n{}\n",
                    schema.hash, schema
                );
            }
            Ok((repo, None))
        }
    } else {
        eprintln!("Could not find schema {}", val);
        Ok((repo, None))
    }
}

pub fn schema_name(schema_ref: &str, val: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    let schema = command::schema_name(&repository, schema_ref, val)?;
    println!("{}", schema.unwrap());

    Ok(())
}

pub fn schema_create_index(schema_ref: &str, field: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::schema_create_index(&repository, schema_ref, field)
}

pub fn schema_list_indices(schema_ref: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    let fields = command::schema_list_indices(&repository, schema_ref)?;
    for field in fields {
        println!("{}", field.name);
    }

    Ok(())
}

pub fn schema_query_index(schema_ref: &str, field: &str, query: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    let df = command::schema_query_index(&repository, schema_ref, field, query)?;
    println!("{}", df);

    Ok(())
}

pub fn schema_list() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let schemas = command::schema_list(&repository, None)?;
    if schemas.is_empty() {
        eprintln!("{}", OxenError::no_schemas_found());
    } else {
        let result = schema::Schema::schemas_to_string(&schemas);
        println!("{}", result);
    }
    Ok(())
}

pub fn schema_list_commit_id(commit_id: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let schemas = command::schema_list(&repository, Some(commit_id))?;
    if schemas.is_empty() {
        eprintln!("{}", OxenError::no_schemas_found());
    } else {
        let result = schema::Schema::schemas_to_string(&schemas);
        println!("{}", result);
    }
    Ok(())
}

pub fn create_branch(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::create_branch_from_head(&repository, name)?;
    Ok(())
}

pub fn delete_branch(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::delete_branch(&repository, name)?;
    Ok(())
}

pub async fn delete_remote_branch(remote_name: &str, branch_name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::delete_remote_branch(&repository, remote_name, branch_name).await?;
    Ok(())
}

pub fn force_delete_branch(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::force_delete_branch(&repository, name)?;
    Ok(())
}

pub fn checkout(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::checkout(&repository, name)?;
    Ok(())
}

pub fn checkout_theirs(path: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::checkout_theirs(&repository, path)?;
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
            println!("  {}", branch.name)
        }
    }

    Ok(())
}

pub async fn list_remote_branches(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let remotes = command::list_remote_branches(&repository, name).await?;

    for branch in remotes.iter() {
        println!("{}\t{}", branch.remote, branch.branch);
    }
    Ok(())
}

pub async fn list_all_branches() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    list_branches()?;

    for remote in repository.remotes.iter() {
        list_remote_branches(&remote.name).await?;
    }

    Ok(())
}

pub fn show_current_branch() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    if let Some(current_branch) = command::current_branch(&repository)? {
        println!("{}", current_branch.name);
    }

    Ok(())
}

pub fn inspect(path: &Path) -> Result<(), OxenError> {
    command::inspect(path)
}
