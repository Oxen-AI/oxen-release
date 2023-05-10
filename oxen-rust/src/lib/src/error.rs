//! Errors for the oxen library
//!
//! Enumeration for all errors that can occur in the oxen library
//!

use derive_more::{Display, Error};
use std::fmt::Debug;
use std::io;
use std::path::{Path, PathBuf};

use crate::model::Schema;
use crate::model::{Commit, ParsedResource};
use crate::model::{Remote, RepositoryNew};

pub mod path_buf_error;
pub mod string_error;

pub use crate::error::path_buf_error::PathBufError;
pub use crate::error::string_error::StringError;

pub const NO_REPO_FOUND: &str = "No oxen repository exists, looking for directory: .oxen";

pub const HEAD_NOT_FOUND: &str = "HEAD not found";

pub const EMAIL_AND_NAME_NOT_FOUND: &str =
    "oxen not configured, set email and name with:\n\noxen config --name YOUR_NAME --email YOUR_EMAIL\n";

pub const AUTH_TOKEN_NOT_FOUND: &str =
    "oxen authentication token not found, obtain one from your administrator and configure with:\n\noxen config --auth <HOST> <TOKEN>\n";

#[derive(Debug, Display, Error)]
pub enum OxenError {
    /// Internal Oxen Errors
    // User
    UserConfigNotFound(Box<StringError>),

    // Repo
    RepoNotFound(Box<RepositoryNew>),
    RepoAlreadyExists(Box<RepositoryNew>),

    // Remotes
    RemoteRepoNotFound(Box<Remote>),

    // Branches/Commits
    BranchNotFound(Box<StringError>),
    CommittishNotFound(Box<StringError>),
    RootCommitDoesNotMatch(Box<Commit>),
    NothingToCommit(StringError),

    // Resources (paths, uris, etc.)
    ResourceNotFound(StringError),
    PathDoesNotExist(Box<PathBufError>),
    ParsedResourceNotFound(Box<PathBufError>),

    // Schema
    InvalidSchema(Box<Schema>),

    // Generic
    ParsingError(Box<StringError>),

    // External Library Errors
    IO(io::Error),
    Authentication(StringError),
    TomlSer(toml::ser::Error),
    TomlDe(toml::de::Error),
    URI(http::uri::InvalidUri),
    URL(url::ParseError),
    JSON(serde_json::Error),
    HTTP(reqwest::Error),
    Encoding(std::str::Utf8Error),
    DB(rocksdb::Error),
    ENV(std::env::VarError),

    // Fallback
    Basic(StringError),
}

impl OxenError {
    pub fn basic_str<T: AsRef<str>>(s: T) -> Self {
        OxenError::Basic(StringError::from(s.as_ref()))
    }

    pub fn authentication<T: AsRef<str>>(s: T) -> Self {
        OxenError::Authentication(StringError::from(s.as_ref()))
    }

    pub fn user_config_not_found(value: StringError) -> Self {
        OxenError::UserConfigNotFound(Box::new(value))
    }

    pub fn repo_not_found(repo: RepositoryNew) -> Self {
        OxenError::RepoNotFound(Box::new(repo))
    }

    pub fn remote_not_set(name: &str) -> Self {
        OxenError::basic_str(
            format!("Remote not set, you can set a remote by running:\n\noxen config --set-remote origin {} <url>\n", name)
        )
    }

    pub fn remote_not_found(remote: Remote) -> Self {
        OxenError::RemoteRepoNotFound(Box::new(remote))
    }

    pub fn resource_not_found(value: impl AsRef<str>) -> Self {
        OxenError::ResourceNotFound(StringError::from(value.as_ref()))
    }

    pub fn path_does_not_exist(path: PathBuf) -> Self {
        OxenError::PathDoesNotExist(Box::new(path.into()))
    }

    pub fn parsed_resource_not_found(resource: ParsedResource) -> Self {
        OxenError::ParsedResourceNotFound(Box::new(resource.resource.into()))
    }

    pub fn repo_already_exists(repo: RepositoryNew) -> Self {
        OxenError::RepoAlreadyExists(Box::new(repo))
    }

    pub fn committish_not_found(value: StringError) -> Self {
        OxenError::CommittishNotFound(Box::new(value))
    }

    pub fn root_commit_does_not_match(commit: Commit) -> Self {
        OxenError::RootCommitDoesNotMatch(Box::new(commit))
    }

    pub fn local_repo_not_found() -> OxenError {
        OxenError::basic_str(NO_REPO_FOUND)
    }

    pub fn email_and_name_not_set() -> OxenError {
        OxenError::user_config_not_found(EMAIL_AND_NAME_NOT_FOUND.to_string().into())
    }

    pub fn auth_token_not_set() -> OxenError {
        OxenError::basic_str(AUTH_TOKEN_NOT_FOUND)
    }

    pub fn remote_repo_not_found<T: AsRef<str>>(url: T) -> OxenError {
        let err = format!("Remote repository does not exist {}", url.as_ref());
        OxenError::basic_str(err)
    }

    pub fn head_not_found() -> OxenError {
        OxenError::basic_str(HEAD_NOT_FOUND)
    }

    pub fn home_dir_not_found() -> OxenError {
        OxenError::basic_str("Home directory not found")
    }

    pub fn must_be_on_valid_branch() -> OxenError {
        OxenError::basic_str("Repository is in a detached HEAD state, checkout a valid branch to continue.\n\n  oxen checkout <branch>\n")
    }

    pub fn no_schemas_found() -> OxenError {
        OxenError::basic_str(
            "No schemas found\n\nAdd and commit a tabular data file with:\n\n  oxen add path/to/file.csv\n  oxen commit -m \"adding data\"\n",
        )
    }

    pub fn schema_does_not_exist_for_file<P: AsRef<Path>>(path: P) -> OxenError {
        let err = format!("Schema does not exist for file {:?}", path.as_ref());
        OxenError::basic_str(err)
    }

    pub fn schema_does_not_exist<S: AsRef<str>>(schema_ref: S) -> OxenError {
        let err = format!("Schema does not exist {:?}", schema_ref.as_ref());
        OxenError::basic_str(err)
    }

    pub fn schema_does_not_have_field<S: AsRef<str>>(field: S) -> OxenError {
        let err = format!("Schema does not have field {:?}", field.as_ref());
        OxenError::basic_str(err)
    }

    pub fn schema_has_changed(old_schema: Schema, current_schema: Schema) -> OxenError {
        let err =
            format!("\nSchema has changed\n\nOld\n{old_schema}\n\nCurrent\n{current_schema}\n");
        OxenError::basic_str(err)
    }

    pub fn remote_branch_not_found<T: AsRef<str>>(name: T) -> OxenError {
        let err = format!("Remote branch '{}' not found", name.as_ref());
        OxenError::basic_str(err)
    }

    pub fn local_branch_not_found<T: AsRef<str>>(name: T) -> OxenError {
        let err = format!("Local branch '{}' not found", name.as_ref());
        OxenError::basic_str(err)
    }

    pub fn commit_db_corrupted<T: AsRef<str>>(commit_id: T) -> OxenError {
        let err = format!(
            "Commit db corrupted, could not find commit: {}",
            commit_id.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn commit_id_does_not_exist<T: AsRef<str>>(commit_id: T) -> OxenError {
        let err = format!("Could not find commit: {}", commit_id.as_ref());
        OxenError::basic_str(err)
    }

    pub fn local_parent_link_broken<T: AsRef<str>>(commit_id: T) -> OxenError {
        let err = format!("Broken link to parent commit: {}", commit_id.as_ref());
        OxenError::basic_str(err)
    }

    pub fn entry_does_not_exist<T: AsRef<Path>>(path: T) -> OxenError {
        let err = format!("Entry does not exist: {:?}", path.as_ref());
        OxenError::basic_str(err)
    }

    pub fn file_error<T: AsRef<Path>>(path: T, error: std::io::Error) -> OxenError {
        let err = format!("File does not exist: {:?} error {:?}", path.as_ref(), error);
        OxenError::basic_str(err)
    }

    pub fn file_create_error<T: AsRef<Path>>(path: T, error: std::io::Error) -> OxenError {
        let err = format!(
            "Could not create file: {:?} error {:?}",
            path.as_ref(),
            error
        );
        OxenError::basic_str(err)
    }

    pub fn file_metadata_error<T: AsRef<Path>>(path: T, error: std::io::Error) -> OxenError {
        let err = format!(
            "Could not get file metadata: {:?} error {:?}",
            path.as_ref(),
            error
        );
        OxenError::basic_str(err)
    }

    pub fn file_copy_error(
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        err: impl Debug,
    ) -> OxenError {
        let err = format!(
            "File copy error: {err:?}\nCould not copy from `{:?}` to `{:?}`",
            src.as_ref(),
            dst.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn remote_add_file_not_in_repo(path: impl AsRef<Path>) -> OxenError {
        let err = format!(
            "File is outside of the repo {:?}\n\nYou must specify a path you would like to add the file at with the -p flag.\n\n  oxen remote add /path/to/file.png -p my-images/\n",
            path.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn file_does_not_exist_in_commit<P: AsRef<Path>, S: AsRef<str>>(
        path: P,
        commit_id: S,
    ) -> OxenError {
        let err = format!(
            "File {:?} does not exist in commit {}",
            path.as_ref(),
            commit_id.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn file_has_no_parent<T: AsRef<Path>>(path: T) -> OxenError {
        let err = format!("File has no parent: {:?}", path.as_ref());
        OxenError::basic_str(err)
    }

    pub fn file_has_no_file_name<T: AsRef<Path>>(path: T) -> OxenError {
        let err = format!("File has no file_name: {:?}", path.as_ref());
        OxenError::basic_str(err)
    }

    pub fn could_not_convert_path_to_str<T: AsRef<Path>>(path: T) -> OxenError {
        let err = format!("File has no name: {:?}", path.as_ref());
        OxenError::basic_str(err)
    }

    pub fn local_commit_or_branch_not_found<T: AsRef<str>>(name: T) -> OxenError {
        let err = format!(
            "Local branch or commit reference `{}` not found",
            name.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn could_not_find_merge_conflict<P: AsRef<Path>>(path: P) -> OxenError {
        let err = format!(
            "Could not find merge conflict for path: {:?}",
            path.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn could_not_decode_value_for_key_error<S: AsRef<str>>(key: S) -> OxenError {
        let err = format!("Could not decode value for key: {:?}", key.as_ref());
        OxenError::basic_str(err)
    }

    pub fn invalid_agg_query<S: AsRef<str>>(query: S) -> OxenError {
        let err = format!("Invalid aggregate opt: {:?}", query.as_ref());
        OxenError::basic_str(err)
    }

    pub fn invalid_set_remote_url<S: AsRef<str>>(url: S) -> OxenError {
        let err = format!("\nRemote invalid, must be fully qualified URL, got: {:?}\n\n  oxen config --set-remote origin https://hub.oxen.ai/<namespace>/<reponame>\n", url.as_ref());
        OxenError::basic_str(err)
    }

    pub fn parse_error<S: AsRef<str>>(value: S) -> OxenError {
        let err = format!("Parse error: {:?}", value.as_ref());
        OxenError::basic_str(err)
    }

    pub fn unknown_agg_fn<S: AsRef<str>>(name: S) -> OxenError {
        let err = format!("Unknown aggregation function: {:?}", name.as_ref());
        OxenError::basic_str(err)
    }

    pub fn repo_is_shallow() -> OxenError {
        let err = r"
Repo is in a shallow clone state. You can only perform operations remotely.

To fetch data from the remote, run:

    oxen pull origin main

Or you can interact with the remote directly with the `oxen remote` subcommand:

    oxen remote status
    oxen remote add path/to/image.jpg
    oxen remote commit -m 'Committing data to remote without ever pulling it locally'
";
        OxenError::basic_str(err)
    }
}

// if you do not want to call .map_err, implement the std::convert::From trait
impl From<io::Error> for OxenError {
    fn from(error: io::Error) -> Self {
        OxenError::IO(error)
    }
}

impl From<String> for OxenError {
    fn from(error: String) -> Self {
        OxenError::Basic(StringError::from(error))
    }
}

impl From<toml::ser::Error> for OxenError {
    fn from(error: toml::ser::Error) -> Self {
        OxenError::TomlSer(error)
    }
}

impl From<toml::de::Error> for OxenError {
    fn from(error: toml::de::Error) -> Self {
        OxenError::TomlDe(error)
    }
}

impl From<http::uri::InvalidUri> for OxenError {
    fn from(error: http::uri::InvalidUri) -> Self {
        OxenError::URI(error)
    }
}

impl From<url::ParseError> for OxenError {
    fn from(error: url::ParseError) -> Self {
        OxenError::URL(error)
    }
}

impl From<serde_json::Error> for OxenError {
    fn from(error: serde_json::Error) -> Self {
        OxenError::JSON(error)
    }
}

impl From<std::str::Utf8Error> for OxenError {
    fn from(error: std::str::Utf8Error) -> Self {
        OxenError::Encoding(error)
    }
}

impl From<reqwest::Error> for OxenError {
    fn from(error: reqwest::Error) -> Self {
        OxenError::HTTP(error)
    }
}

impl From<rocksdb::Error> for OxenError {
    fn from(error: rocksdb::Error) -> Self {
        OxenError::DB(error)
    }
}

impl From<std::env::VarError> for OxenError {
    fn from(error: std::env::VarError) -> Self {
        OxenError::ENV(error)
    }
}
