use liboxen::api;
use liboxen::command;
use liboxen::config::UserConfig;
use liboxen::constants;
use liboxen::error;
use liboxen::error::OxenError;
use liboxen::model::schema;
use liboxen::model::EntryDataType;
use liboxen::model::{staged_data::StagedDataOpts, LocalRepository};
use liboxen::opts::AddOpts;
use liboxen::opts::CloneOpts;
use liboxen::opts::DFOpts;
use liboxen::opts::DownloadOpts;
use liboxen::opts::InfoOpts;
use liboxen::opts::ListOpts;
use liboxen::opts::LogOpts;
use liboxen::opts::PaginateOpts;
use liboxen::opts::RestoreOpts;
use liboxen::opts::RmOpts;
use liboxen::util;

use colored::Colorize;
use liboxen::view::PaginatedDirEntries;
use minus::Pager;
use std::env;
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use time::format_description;

fn get_host_or_default() -> Result<String, OxenError> {
    let config = UserConfig::get_or_create()?;
    let mut default_host = constants::DEFAULT_HOST.to_string();
    if let Some(host) = config.default_host {
        if !host.is_empty() {
            default_host = host;
        }
    }
    Ok(default_host)
}

fn get_host_from_repo(repo: &LocalRepository) -> Result<String, OxenError> {
    if let Some(remote) = repo.remote() {
        let host = api::remote::client::get_host_from_url(remote.url)?;
        return Ok(host);
    }
    get_host_or_default()
}

pub async fn check_remote_version(host: impl AsRef<str>) -> Result<(), OxenError> {
    // Do the version check in the dispatch because it's only really the CLI that needs to do it
    match api::remote::version::get_remote_version(host.as_ref()).await {
        Ok(remote_version) => {
            let local_version: &str = constants::OXEN_VERSION;

            if remote_version != local_version {
                let warning = format!("Warning: 🐂 Oxen remote version mismatch. Expected {local_version} but got {remote_version}\n\nPlease visit https://docs.oxen.ai/getting-started/install for installation instructions.\n").yellow();
                eprintln!("{warning}");
            }
        }
        Err(err) => {
            eprintln!("Err checking remote version: {err}")
        }
    }

    Ok(())
}

pub async fn init(path: &str) -> Result<(), OxenError> {
    let directory = dunce::canonicalize(PathBuf::from(&path))?;

    let host = get_host_or_default()?;
    check_remote_version(host).await?;

    command::init(&directory)?;
    println!("🐂 repository initialized at: {directory:?}");
    Ok(())
}

pub async fn clone(opts: &CloneOpts) -> Result<(), OxenError> {
    let host = api::remote::client::get_host_from_url(&opts.url)?;
    check_remote_version(host).await?;

    command::clone(opts).await?;
    Ok(())
}

pub async fn create_remote(namespace: &str, name: &str, host: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    let remote_repo = api::remote::repositories::create(&repo, namespace, name, host).await?;
    println!(
        "Remote created for {}\n\noxen config --set-remote {} {}",
        name, name, remote_repo.remote.url
    );

    Ok(())
}

pub fn set_remote(name: &str, url: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let mut repo = LocalRepository::from_dir(&repo_dir)?;

    command::config::set_remote(&mut repo, name, url)?;

    Ok(())
}

pub fn delete_remote(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let mut repo = LocalRepository::from_dir(&repo_dir)?;

    command::config::delete_remote(&mut repo, name)?;

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
    let mut config = UserConfig::get_or_create()?;
    config.add_host_auth_token(host, token);
    config.save_default()?;
    println!("Authentication token set for host: {host}");
    Ok(())
}

pub fn set_user_name(name: &str) -> Result<(), OxenError> {
    let mut config = UserConfig::get_or_create()?;
    config.name = String::from(name);
    config.save_default()?;
    Ok(())
}

pub fn set_user_email(email: &str) -> Result<(), OxenError> {
    let mut config = UserConfig::get_or_create()?;
    config.email = String::from(email);
    config.save_default()?;
    Ok(())
}

pub fn set_default_host(host: &str) -> Result<(), OxenError> {
    let mut config = UserConfig::get_or_create()?;
    if host.is_empty() {
        config.default_host = None;
    } else {
        config.default_host = Some(String::from(host));
    }
    config.save_default()?;
    Ok(())
}

pub async fn remote_delete_row(path: impl AsRef<Path>, uuid: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let path = path.as_ref();

    command::remote::df::delete_staged_row(&repository, path, uuid).await?;

    Ok(())
}

pub async fn remote_download(opts: DownloadOpts) -> Result<(), OxenError> {
    let paths = &opts.paths;
    if paths.is_empty() {
        return Err(OxenError::basic_str("Must supply a path to download."));
    }

    // Check if the first path is a valid remote repo
    let name = paths[0].to_string_lossy();
    if let Some(remote_repo) =
        api::remote::repositories::get_by_host_remote_name(&opts.host, &opts.remote, name).await?
    {
        // Download from the remote without having to have a local repo directory
        let remote_paths = paths[1..].to_vec();
        let commit_id = opts.remote_commit_id(&remote_repo).await?;
        for path in remote_paths {
            command::remote::download(&remote_repo, &path, &opts.dst, &commit_id).await?;
        }
    } else {
        // We have a --shallow clone, and are just downloading into this directory
        let repo_dir = env::current_dir().unwrap();
        let local_repo = LocalRepository::from_dir(&repo_dir)?;

        let head_commit = api::local::commits::head_commit(&local_repo)?;
        let remote_repo = api::remote::repositories::get_default_remote(&local_repo).await?;
        let dst_path = local_repo.path.join(opts.dst);

        for remote_path in paths {
            command::remote::download(&remote_repo, remote_path, &dst_path, &head_commit.id)
                .await?;
        }
    }

    Ok(())
}

pub async fn remote_metadata_list_dir(path: impl AsRef<Path>) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let local_repo = LocalRepository::from_dir(&repo_dir)?;
    let path = path.as_ref();

    let head_commit = api::local::commits::head_commit(&local_repo)?;
    let remote_repo = api::remote::repositories::get_default_remote(&local_repo).await?;

    let response = api::remote::metadata::list_dir(&remote_repo, &head_commit.id, path).await?;
    let df = response.df.to_df();

    println!("{}\t{:?}\n{:?}", head_commit.id, path, df);

    Ok(())
}

pub async fn remote_metadata_aggregate_dir(
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let local_repo = LocalRepository::from_dir(&repo_dir)?;
    let path = path.as_ref();

    let head_commit = api::local::commits::head_commit(&local_repo)?;
    let remote_repo = api::remote::repositories::get_default_remote(&local_repo).await?;

    let response =
        api::remote::metadata::agg_dir(&remote_repo, &head_commit.id, path, column).await?;
    let df = response.df.to_df();

    println!("{}\t{:?}\n{:?}", head_commit.id, path, df);

    Ok(())
}

pub async fn remote_metadata_list_image(path: impl AsRef<Path>) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let local_repo = LocalRepository::from_dir(&repo_dir)?;
    let path = path.as_ref();

    let head_commit = api::local::commits::head_commit(&local_repo)?;
    let remote_repo = api::remote::repositories::get_default_remote(&local_repo).await?;

    let response = api::remote::metadata::list_dir(&remote_repo, &head_commit.id, path).await?;
    let df = response.df.to_df();

    println!("{}\t{:?}\n{:?}", head_commit.id, path, df);

    Ok(())
}

pub async fn add(opts: AddOpts) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    for path in &opts.paths {
        if opts.is_remote {
            command::remote::add(&repository, path, &opts).await?;
        } else {
            command::add(&repository, path)?;
        }
    }

    Ok(())
}

pub async fn rm(paths: Vec<PathBuf>, opts: &RmOpts) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    for path in paths {
        let path_opts = RmOpts::from_path_opts(&path, opts);
        command::rm(&repository, &path_opts).await?;
    }

    Ok(())
}

pub async fn restore(opts: RestoreOpts) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    if opts.is_remote {
        command::remote::restore(&repository, opts).await?;
    } else {
        command::restore(&repository, opts)?;
    }

    Ok(())
}

pub async fn push(remote: &str, branch: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    let host = get_host_from_repo(&repository)?;
    check_remote_version(host).await?;

    command::push_remote_branch(&repository, remote, branch).await?;
    Ok(())
}

pub async fn pull(remote: &str, branch: &str, all: bool) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    let host = get_host_from_repo(&repository)?;
    check_remote_version(host).await?;

    command::pull_remote_branch(&repository, remote, branch, all).await?;
    Ok(())
}

pub async fn diff(commit_id: Option<&str>, path: &str, remote: bool) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let path = Path::new(path);

    let result = if remote {
        command::remote::diff(&repository, commit_id, path).await?
    } else {
        command::diff(&repository, commit_id, path)?
    };
    println!("{result}");
    Ok(())
}

pub fn merge(branch: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::merge(&repository, branch)?;
    Ok(())
}

pub async fn commit(message: &str, is_remote: bool) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    if is_remote {
        println!("Committing to remote with message: {message}");
        command::remote::commit(&repo, message).await?;
    } else {
        println!("Committing with message: {message}");
        command::commit(&repo, message)?;
    }

    Ok(())
}

fn write_to_pager(output: &mut Pager, text: &str) -> Result<(), OxenError> {
    match writeln!(output, "{}", text) {
        Ok(_) => Ok(()),
        Err(_) => Err(OxenError::basic_str("Could not write to pager")),
    }
}

pub async fn fetch() -> Result<(), OxenError> {
    // Look up from the current dir for .oxen directory
    let current_dir = env::current_dir().unwrap();
    let repo_dir = util::fs::get_repo_root(&current_dir).expect(error::NO_REPO_FOUND);

    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::fetch(&repository).await?;
    Ok(())
}

pub async fn log_commits(opts: LogOpts) -> Result<(), OxenError> {
    // Look up from the current dir for .oxen directory
    let current_dir = env::current_dir().unwrap();
    let repo_dir = util::fs::get_repo_root(&current_dir).expect(error::NO_REPO_FOUND);
    let repository = LocalRepository::from_dir(&repo_dir)?;

    let commits = api::local::commits::list_with_opts(&repository, &opts).await?;

    // Fri, 21 Oct 2022 16:08:39 -0700
    let format = format_description::parse(
        "[weekday], [day] [month repr:long] [year] [hour]:[minute]:[second] [offset_hour sign:mandatory]",
    ).unwrap();

    let mut output = Pager::new();

    for commit in commits {
        let commit_id_str = format!("commit {}", commit.id).yellow();
        write_to_pager(&mut output, &format!("{}\n", commit_id_str))?;
        write_to_pager(&mut output, &format!("Author: {}", commit.author))?;
        write_to_pager(
            &mut output,
            &format!("Date:   {}\n", commit.timestamp.format(&format).unwrap()),
        )?;
        write_to_pager(&mut output, &format!("    {}\n", commit.message))?;
    }

    match minus::page_all(output) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Error while paging: {}", e);
        }
    }

    Ok(())
}

pub async fn status(directory: Option<PathBuf>, opts: &StagedDataOpts) -> Result<(), OxenError> {
    if opts.is_remote {
        return remote_status(directory, opts).await;
    }

    // Look up from the current dir for .oxen directory
    let current_dir = env::current_dir().unwrap();
    let repo_dir = util::fs::get_repo_root(&current_dir).expect(error::NO_REPO_FOUND);

    let directory = directory.unwrap_or(current_dir);
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let repo_status = command::status_from_dir(&repository, &directory)?;

    if let Some(current_branch) = api::local::branches::current_branch(&repository)? {
        println!(
            "On branch {} -> {}\n",
            current_branch.name, current_branch.commit_id
        );
    } else {
        let head = api::local::commits::head_commit(&repository)?;
        println!(
            "You are in 'detached HEAD' state.\nHEAD is now at {} {}\n",
            head.id, head.message
        );
    }

    repo_status.print_stdout_with_params(opts);

    Ok(())
}

pub fn info(opts: InfoOpts) -> Result<(), OxenError> {
    // Look up from the current dir for .oxen directory
    let current_dir = env::current_dir().unwrap();
    let repo_dir = util::fs::get_repo_root(&current_dir).expect(error::NO_REPO_FOUND);
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let metadata = command::info(&repository, opts.to_owned())?;

    if opts.output_as_json {
        let json = serde_json::to_string(&metadata)?;
        println!("{}", json);
    } else {
        /*
        hash size data_type mime_type extension last_updated_commit_id
        */
        if opts.verbose {
            println!("hash\tsize\tdata_type\tmime_type\textension\tlast_updated_commit_id");
        }

        let mut last_updated_commit_id = String::from("None");
        if let Some(commit) = metadata.last_updated {
            last_updated_commit_id = commit.id;
        }

        println!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            metadata.hash,
            metadata.size,
            metadata.data_type,
            metadata.mime_type,
            metadata.extension,
            last_updated_commit_id
        );
    }

    Ok(())
}

async fn remote_status(directory: Option<PathBuf>, opts: &StagedDataOpts) -> Result<(), OxenError> {
    // Look up from the current dir for .oxen directory
    let current_dir = env::current_dir().unwrap();
    let repo_dir = util::fs::get_repo_root(&current_dir).expect(error::NO_REPO_FOUND);

    let repository = LocalRepository::from_dir(&repo_dir)?;
    let host = get_host_from_repo(&repository)?;
    check_remote_version(host).await?;

    let directory = directory.unwrap_or(PathBuf::from("."));

    if let Some(current_branch) = api::local::branches::current_branch(&repository)? {
        let remote_repo = api::remote::repositories::get_default_remote(&repository).await?;
        let repo_status =
            command::remote::status(&remote_repo, &current_branch, &directory, opts).await?;
        if let Some(remote_branch) =
            api::remote::branches::get_by_name(&remote_repo, &current_branch.name).await?
        {
            println!(
                "Checking remote branch {} -> {}\n",
                remote_branch.name, remote_branch.commit_id
            );
            repo_status.print_stdout_with_params(opts);
        } else {
            println!("Remote branch '{}' not found", current_branch.name);
        }
    } else {
        let head = api::local::commits::head_commit(&repository)?;
        println!(
            "You are in 'detached HEAD' state.\nHEAD is now at {} {}\nYou cannot query remote status unless you are on a branch.",
            head.id, head.message
        );
    }

    Ok(())
}

pub async fn remote_ls(opts: &ListOpts) -> Result<(), OxenError> {
    let paths = &opts.paths;
    if paths.is_empty() {
        return Err(OxenError::basic_str("Must supply a path to download."));
    }

    let page_opts = PaginateOpts {
        page_num: opts.page_num,
        page_size: opts.page_size,
    };

    // Check if the first path is a valid remote repo
    let name = paths[0].to_string_lossy();
    let entries = if let Some(remote_repo) =
        api::remote::repositories::get_by_host_remote_name(&opts.host, &opts.remote, name).await?
    {
        let branch = api::remote::branches::get_by_name(&remote_repo, &opts.branch_name)
            .await?
            .ok_or_else(OxenError::must_be_on_valid_branch)?;
        let directory = if paths.len() > 1 {
            paths[1].clone()
        } else {
            PathBuf::from("")
        };
        command::remote::ls(&remote_repo, &branch, &directory, &page_opts).await?
    } else {
        // Look up from the current dir for .oxen directory
        let current_dir = env::current_dir().unwrap();
        let repo_dir = util::fs::get_repo_root(&current_dir).expect(error::NO_REPO_FOUND);

        let repository = LocalRepository::from_dir(&repo_dir)?;

        let host = get_host_from_repo(&repository)?;
        check_remote_version(host).await?;

        let directory = paths[0].clone();
        let remote_repo = api::remote::repositories::get_default_remote(&repository).await?;
        let branch = api::local::branches::current_branch(&repository)?
            .ok_or_else(OxenError::must_be_on_valid_branch)?;
        command::remote::ls(&remote_repo, &branch, &directory, &page_opts).await?
    };

    let num_displaying = if opts.page_size > entries.total_entries {
        entries.total_entries
    } else {
        opts.page_size
    };
    println!(
        "Displaying {}/{} total entries\n",
        num_displaying, entries.total_entries
    );

    maybe_display_types(&entries);

    for entry in entries.entries {
        if entry.is_dir {
            println!("  {}/", entry.filename);
        } else {
            println!("  {}", entry.filename);
        }
    }
    println!();

    Ok(())
}

fn maybe_display_types(entries: &PaginatedDirEntries) {
    // unwrap entries.metadata or exit function
    let entries_metadata = match &entries.metadata {
        Some(entries_metadata) => entries_metadata,
        None => return,
    };

    // parse data_type_counts or exit function
    let data_type_counts = &entries_metadata.dir.data_types;

    if !data_type_counts.is_empty() {
        println!();
        for data_type_count in data_type_counts {
            if let Ok(edt) = EntryDataType::from_str(&data_type_count.data_type) {
                let emoji = edt.to_emoji();
                print!(
                    "{} {} ({})\t",
                    emoji, data_type_count.data_type, data_type_count.count
                );
            } else {
                print!(
                    "{} ({})\t",
                    data_type_count.data_type, data_type_count.count
                );
            }
        }
        print!("\n\n");
    }
}

pub fn df<P: AsRef<Path>>(input: P, opts: DFOpts) -> Result<(), OxenError> {
    command::df(input, opts)?;
    Ok(())
}

pub async fn remote_df<P: AsRef<Path>>(input: P, opts: DFOpts) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    let host = get_host_from_repo(&repo)?;
    check_remote_version(host).await?;

    command::remote::df(&repo, input, opts).await?;
    Ok(())
}

pub fn df_schema<P: AsRef<Path>>(input: P, flatten: bool, opts: DFOpts) -> Result<(), OxenError> {
    let result = command::df::schema(input, flatten, opts)?;
    println!("{result}");
    Ok(())
}

pub fn schema_show(val: &str, staged: bool, verbose: bool) -> Result<String, OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    let val = command::schemas::show(&repo, val, staged, verbose)?;
    println!("{val}");
    Ok(val)
}

pub fn schema_name(schema_ref: &str, val: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::schemas::set_name(&repository, schema_ref, val)?;
    let schema = schema_show(schema_ref, true, false)?;
    println!("{schema}");

    Ok(())
}

pub fn schema_list(staged: bool) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let schemas = if staged {
        command::schemas::list_staged(&repository)?
    } else {
        command::schemas::list(&repository, None)?
    };

    if schemas.is_empty() && staged {
        eprintln!("{}", OxenError::no_schemas_staged());
    } else if schemas.is_empty() {
        eprintln!("{}", OxenError::no_schemas_committed());
    } else {
        let result = schema::Schema::schemas_to_string(schemas);
        println!("{result}");
    }

    Ok(())
}

pub fn schema_list_commit_id(commit_id: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let schemas = command::schemas::list(&repository, Some(commit_id))?;
    if schemas.is_empty() {
        eprintln!("{}", OxenError::no_schemas_committed());
    } else {
        let result = schema::Schema::schemas_to_string(schemas);
        println!("{result}");
    }
    Ok(())
}

pub fn schema_rm(schema_ref: impl AsRef<str>, staged: bool) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    command::schemas::rm(&repository, schema_ref, staged)?;

    Ok(())
}

pub fn schema_add_column_metadata(
    schema_ref: impl AsRef<str>,
    column: impl AsRef<str>,
    metadata: impl AsRef<str>,
) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    // make sure metadata is valid json, return oxen error if not
    let metadata: serde_json::Value = serde_json::from_str(metadata.as_ref()).map_err(|e| {
        OxenError::basic_str(format!(
            "Metadata must be valid JSON: '{}'\n{}",
            metadata.as_ref(),
            e
        ))
    })?;

    for (path, schema) in
        command::schemas::add_column_metadata(&repository, schema_ref, column, &metadata)?
    {
        println!("{:?}\n{}", path, schema.verbose_str());
    }

    Ok(())
}

pub fn schema_add_metadata(
    schema_ref: impl AsRef<str>,
    metadata: impl AsRef<str>,
) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    let metadata: serde_json::Value = serde_json::from_str(metadata.as_ref()).map_err(|e| {
        OxenError::basic_str(format!(
            "Metadata must be valid JSON: '{}'\n{}",
            metadata.as_ref(),
            e
        ))
    })?;

    for (path, schema) in command::schemas::add_schema_metadata(&repository, schema_ref, &metadata)?
    {
        println!("{:?}\n{}", path, schema.verbose_str());
    }

    Ok(())
}

pub fn create_branch(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    api::local::branches::create_from_head(&repository, name)?;
    Ok(())
}

pub fn delete_branch(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    api::local::branches::delete(&repository, name)?;
    Ok(())
}

pub async fn delete_remote_branch(remote_name: &str, branch_name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    let host = get_host_from_repo(&repository)?;
    check_remote_version(host).await?;

    api::remote::branches::delete_remote(&repository, remote_name, branch_name).await?;
    Ok(())
}

pub fn force_delete_branch(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    api::local::branches::force_delete(&repository, name)?;
    Ok(())
}

pub fn rename_current_branch(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    api::local::branches::rename_current_branch(&repository, name)?;
    Ok(())
}

pub async fn checkout(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::checkout(&repository, name).await?;
    Ok(())
}

pub fn checkout_theirs(path: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::checkout_theirs(&repository, path)?;
    Ok(())
}

pub fn checkout_ours(path: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::checkout_ours(&repository, path)?;
    Ok(())
}

pub fn create_checkout_branch(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    api::local::branches::create_checkout(&repository, name)?;
    Ok(())
}

pub fn list_branches() -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let branches = api::local::branches::list(&repository)?;

    for branch in branches.iter() {
        if branch.is_head {
            let branch_str = format!("* {}", branch.name).green();
            println!("{branch_str}")
        } else {
            println!("  {}", branch.name)
        }
    }

    Ok(())
}

pub async fn list_remote_branches(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    let host = get_host_from_repo(&repo)?;
    check_remote_version(host).await?;

    let remote = repo
        .get_remote(name)
        .ok_or(OxenError::remote_not_set(name))?;
    let remote_repo = api::remote::repositories::get_by_remote(&remote)
        .await?
        .ok_or(OxenError::remote_not_found(remote.clone()))?;

    let branches = api::remote::branches::list(&remote_repo).await?;
    for branch in branches.iter() {
        println!("{}\t{}", &remote.name, branch.name);
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

    if let Some(current_branch) = api::local::branches::current_branch(&repository)? {
        println!("{}", current_branch.name);
    }

    Ok(())
}

pub fn inspect(path: &Path) -> Result<(), OxenError> {
    command::db_inspect::inspect(path)
}
