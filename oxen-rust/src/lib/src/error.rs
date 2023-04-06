use std::error;
use std::fmt;
use std::fmt::Debug;
use std::io;
use std::path::Path;

use crate::model::Schema;

pub const NO_REPO_FOUND: &str = "No oxen repository exists, looking for directory: .oxen";

pub const HEAD_NOT_FOUND: &str = "HEAD not found";

pub const EMAIL_AND_NAME_NOT_FOUND: &str =
    "Err: oxen not configured, set email and name with:\n\noxen config --name YOUR_NAME --email YOUR_EMAIL\n";

pub const AUTH_TOKEN_NOT_FOUND: &str =
    "Err: oxen authentication token not found, obtain one from your administrator and configure with:\n\noxen config --auth <HOST> <TOKEN>\n";

#[derive(Debug)]
pub enum OxenError {
    IO(io::Error),
    Basic(String),
    Authentication(String),
    TomlSer(toml::ser::Error),
    TomlDe(toml::de::Error),
    URI(http::uri::InvalidUri),
    URL(url::ParseError),
    JSON(serde_json::Error),
    HTTP(reqwest::Error),
    Encoding(std::str::Utf8Error),
    DB(rocksdb::Error),
    ENV(std::env::VarError),
}

impl OxenError {
    pub fn basic_str<T: AsRef<str>>(s: T) -> Self {
        OxenError::Basic(String::from(s.as_ref()))
    }

    pub fn authentication<T: AsRef<str>>(s: T) -> Self {
        OxenError::Authentication(String::from(s.as_ref()))
    }

    pub fn local_repo_not_found() -> OxenError {
        OxenError::basic_str(NO_REPO_FOUND)
    }

    pub fn email_and_name_not_set() -> OxenError {
        OxenError::basic_str(EMAIL_AND_NAME_NOT_FOUND)
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

    pub fn must_be_on_valid_branch() -> OxenError {
        OxenError::basic_str("Repository is in a detached HEAD state, checkout a valid branch to continue.\n\n  oxen checkout <branch>\n")
    }

    pub fn remote_not_set() -> OxenError {
        OxenError::basic_str(
            "Remote not set, you can set a remote by running:\n\noxen config --set-remote origin <name> <url>\n",
        )
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

    pub fn file_does_not_exist<T: AsRef<Path>>(path: T) -> OxenError {
        let err = format!("File does not exist: {:?}", path.as_ref());
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

impl fmt::Display for OxenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let OxenError::Basic(err) = self {
            write!(f, "{err}")
        } else {
            write!(f, "{self:?}")
        }
    }
}

// Defers to default method impls, compiler will fill in the blanks
impl error::Error for OxenError {}

// if you do not want to call .map_err, implement the std::convert::From trait
impl From<io::Error> for OxenError {
    fn from(error: io::Error) -> Self {
        OxenError::IO(error)
    }
}

impl From<String> for OxenError {
    fn from(error: String) -> Self {
        OxenError::Basic(error)
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
