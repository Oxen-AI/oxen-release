use liboxen::api;
use liboxen::command;
use liboxen::config::UserConfig;
use liboxen::error;
use liboxen::error::OxenError;
use liboxen::model::file::FileNew;
use liboxen::model::schema;
use liboxen::model::EntryDataType;
use liboxen::model::LocalRepository;
use liboxen::model::RepoNew;
use liboxen::opts::AddOpts;
use liboxen::opts::DFOpts;
use liboxen::opts::DownloadOpts;
use liboxen::opts::InfoOpts;
use liboxen::opts::ListOpts;
use liboxen::opts::PaginateOpts;
use liboxen::opts::UploadOpts;
use liboxen::util;
use liboxen::view::PaginatedDirEntries;

use std::env;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::helpers::{
    check_remote_version, check_remote_version_blocking, check_repo_migration_needed,
    get_host_from_repo,
};

pub async fn create_remote(
    namespace: impl AsRef<str>,
    name: impl AsRef<str>,
    host: impl AsRef<str>,
    scheme: impl AsRef<str>,
    empty: bool,
    is_public: bool,
) -> Result<(), OxenError> {
    let namespace = namespace.as_ref();
    let name = name.as_ref();
    let host = host.as_ref();
    let scheme = scheme.as_ref();
    if empty {
        let mut repo_new = RepoNew::from_namespace_name(namespace, name);
        repo_new.host = Some(String::from(host));
        repo_new.is_public = Some(is_public);
        repo_new.scheme = Some(String::from(scheme));
        let remote_repo = api::remote::repositories::create_empty(repo_new).await?;
        println!(
            "🎉 Remote successfully created for '{}/{}' if this is a brand new repository:\n\n  oxen clone {}\n\nTo push an existing local repository to a new remote:\n\n  oxen config --set-remote origin {}\n",
            namespace, name, remote_repo.remote.url, remote_repo.remote.url
        );
    } else {
        // Creating a remote with an initial commit and a README
        let config = UserConfig::get()?;
        let user = config.to_user();
        let readme_body = format!(
            "
Welcome to Oxen.ai 🐂 🌾

## Getting Started

Clone the repository to your local machine:

```bash
oxen clone https://{}/{}/{}
```

## Adding Data

You can add files to it with

```
oxen add <path>
```

Then commit them with

```
oxen commit -m <message>
```

## Pushing Data

Push your changes to the remote with

```
oxen push origin main
```

## Learn More

For the complete developer documentation, visit https://docs.oxen.ai/

Happy Mooooooving of data 🐂
",
            host, namespace, name
        );

        let files: Vec<FileNew> = vec![FileNew {
            path: PathBuf::from("README.md"),
            contents: format!("# {}\n{}", name, readme_body),
            user,
        }];
        let mut repo = RepoNew::from_files(namespace, name, files);
        repo.host = Some(String::from(host));
        repo.is_public = Some(is_public);
        repo.scheme = Some(String::from(scheme));

        let remote_repo = api::remote::repositories::create(repo).await?;
        println!(
            "Created {}/{}\n\nClone to repository to your local:\n\n  oxen clone {}\n",
            namespace, name, remote_repo.remote.url
        );
    }

    Ok(())
}

pub async fn remote_delete_row(path: impl AsRef<Path>, uuid: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let path = path.as_ref();

    command::remote::df::delete_row(&repository, path, uuid).await?;

    Ok(())
}

pub async fn remote_index_dataset(path: impl AsRef<Path>) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let path = path.as_ref();

    command::remote::df::index_dataset(&repository, path).await?;
    Ok(())
}

/// Download allows the user to download a file or files without cloning the repo
pub async fn download(opts: DownloadOpts) -> Result<(), OxenError> {
    let paths = &opts.paths;
    if paths.is_empty() {
        return Err(OxenError::basic_str("Must supply a path to download."));
    }

    check_remote_version_blocking(opts.clone().host).await?;

    // Check if the first path is a valid remote repo
    let name = paths[0].to_string_lossy();
    if let Some(remote_repo) =
        api::remote::repositories::get_by_name_host_and_remote(&name, &opts.host, &opts.remote)
            .await?
    {
        // Download from the remote without having to have a local repo directory
        let remote_paths = paths[1..].to_vec();
        let commit_id = opts.remote_commit_id(&remote_repo).await?;
        for path in remote_paths {
            command::remote::download(&remote_repo, &path, &opts.dst, &commit_id).await?;
        }
    } else {
        eprintln!("Repository does not exist {}", name);
    }

    Ok(())
}

pub async fn remote_download(opts: DownloadOpts) -> Result<(), OxenError> {
    let paths = &opts.paths;
    if paths.is_empty() {
        return Err(OxenError::basic_str("Must supply a path to download."));
    }

    check_remote_version_blocking(opts.clone().host).await?;
    // Check if the first path is a valid remote repo
    let name = paths[0].to_string_lossy();
    if let Some(remote_repo) =
        api::remote::repositories::get_by_name_host_and_remote(name, &opts.host, &opts.remote)
            .await?
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
    let df = response.data_frame.view.to_df();

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
    let df = response.data_frame.view.to_df();

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
    let df = response.data_frame.view.to_df();

    println!("{}\t{:?}\n{:?}", head_commit.id, path, df);

    Ok(())
}

pub async fn add(opts: AddOpts) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    check_repo_migration_needed(&repository)?;

    for path in &opts.paths {
        if opts.is_remote {
            command::remote::add(&repository, path, &opts).await?;
        } else {
            command::add(&repository, path)?;
        }
    }

    Ok(())
}

pub async fn push(remote: &str, branch: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    let host = get_host_from_repo(&repository)?;

    check_repo_migration_needed(&repository)?;
    check_remote_version_blocking(host.clone()).await?;
    check_remote_version(host).await?;

    command::push_remote_branch(&repository, remote, branch).await?;
    Ok(())
}

pub async fn pull(remote: &str, branch: &str, all: bool) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;

    let host = get_host_from_repo(&repository)?;
    check_repo_migration_needed(&repository)?;
    check_remote_version_blocking(host.clone()).await?;
    check_remote_version(host).await?;

    command::pull_remote_branch(&repository, remote, branch, all).await?;
    Ok(())
}

pub async fn unlock_branch(remote: &str, branch: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    command::unlock(&repository, remote, branch).await?;
    Ok(())
}

pub fn merge(branch: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repository = LocalRepository::from_dir(&repo_dir)?;
    check_repo_migration_needed(&repository)?;

    command::merge(&repository, branch)?;
    Ok(())
}

pub async fn fetch() -> Result<(), OxenError> {
    // Look up from the current dir for .oxen directory
    let current_dir = env::current_dir().unwrap();
    let repo_dir =
        util::fs::get_repo_root(&current_dir).ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;

    let repository = LocalRepository::from_dir(&repo_dir)?;
    let host = get_host_from_repo(&repository)?;

    check_repo_migration_needed(&repository)?;
    check_remote_version_blocking(host.clone()).await?;
    command::fetch(&repository).await?;
    Ok(())
}

pub fn info(opts: InfoOpts) -> Result<(), OxenError> {
    // Look up from the current dir for .oxen directory
    let current_dir = env::current_dir().unwrap();
    let repo_dir =
        util::fs::get_repo_root(&current_dir).ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;
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
        api::remote::repositories::get_by_name_host_and_remote(name, &opts.host, &opts.remote)
            .await?
    {
        let branch = api::remote::branches::get_by_name(&remote_repo, &opts.revision)
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
        let repo_dir = util::fs::get_repo_root(&current_dir)
            .ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;

        let repository = LocalRepository::from_dir(&repo_dir)?;

        let host = get_host_from_repo(&repository)?;
        check_remote_version_blocking(host.clone()).await?;
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

pub async fn remote_df<P: AsRef<Path>>(input: P, opts: DFOpts) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    let host = get_host_from_repo(&repo)?;
    check_remote_version(host).await?;

    command::remote::staged_df(&repo, input, opts).await?;

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

pub async fn list_remote_branches(name: &str) -> Result<(), OxenError> {
    let repo_dir = env::current_dir().unwrap();
    let repo = LocalRepository::from_dir(&repo_dir)?;

    let host = get_host_from_repo(&repo)?;
    check_remote_version_blocking(host.clone()).await?;
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

pub fn load(src_path: &Path, dest_path: &Path, no_working_dir: bool) -> Result<(), OxenError> {
    command::load(src_path, dest_path, no_working_dir)?;
    Ok(())
}
