use std::error;
use std::fmt;
use std::io;
use std::path::Path;

pub const NO_REPO_FOUND: &str = "No oxen repository exists, looking for directory: .oxen";

pub const EMAIL_AND_NAME_NOT_FOUND: &str =
    "Err: oxen not configured, set email and name with:\n\noxen config --name <NAME> --email <EMAIL>\n";

pub const AUTH_TOKEN_NOT_FOUND: &str =
    "Err: oxen authentication token not found, obtain one from your administrator and configure with:\n\noxen config --auth-token <TOKEN>\n";

#[derive(Debug)]
pub enum OxenError {
    IO(io::Error),
    Basic(String),
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
        OxenError::basic_str(&err)
    }

    pub fn head_not_found() -> OxenError {
        OxenError::basic_str("Err: HEAD not found")
    }

    pub fn remote_not_set() -> OxenError {
        OxenError::basic_str("Err: Remote not set, you can set a remote by running:\n\noxen remote add <name> <url>\n")
    }

    pub fn remote_branch_not_found<T: AsRef<str>>(name: T) -> OxenError {
        let err = format!("Remote branch `{}` not found", name.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn local_branch_not_found<T: AsRef<str>>(name: T) -> OxenError {
        let err = format!("Local branch `{}` not found", name.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn commit_db_corrupted<T: AsRef<str>>(commit_id: T) -> OxenError {
        let err = format!(
            "Commit db currupted, could not find commit: {}",
            commit_id.as_ref()
        );
        OxenError::basic_str(&err)
    }

    pub fn commit_id_does_not_exist<T: AsRef<str>>(commit_id: T) -> OxenError {
        let err = format!("Error: could not find commit: {}", commit_id.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn local_parent_link_broken<T: AsRef<str>>(commit_id: T) -> OxenError {
        let err = format!("Broken link to parent commit: {}", commit_id.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn file_does_not_exist<T: AsRef<Path>>(path: T) -> OxenError {
        let err = format!("File does not exist: {:?}", path.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn file_has_no_parent<T: AsRef<Path>>(path: T) -> OxenError {
        let err = format!("File has no parent: {:?}", path.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn could_not_convert_path_to_str<T: AsRef<Path>>(path: T) -> OxenError {
        let err = format!("File has no name: {:?}", path.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn local_commit_or_branch_not_found<T: AsRef<str>>(name: T) -> OxenError {
        let err = format!(
            "Local branch or commit reference `{}` not found",
            name.as_ref()
        );
        OxenError::basic_str(&err)
    }
}

impl fmt::Display for OxenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let OxenError::Basic(err) = self {
            write!(f, "{}", err)
        } else {
            write!(f, "{:?}", self)
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
