use std::error;
use std::fmt;
use std::io;
use std::path::Path;

#[derive(Debug)]
pub enum OxenError {
    IO(io::Error),
    Basic(String),
    TomlSer(toml::ser::Error),
    TomlDe(toml::de::Error),
    URI(http::uri::InvalidUri),
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
        OxenError::basic_str("No oxen repository exists, looking for directory: .oxen")
    }

    pub fn remote_repo_not_found<T: AsRef<str>>(url: T) -> OxenError {
        let err = format!("Remote repository does not exist {}", url.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn head_not_found() -> OxenError {
        OxenError::basic_str("Error: HEAD not found.")
    }

    pub fn remote_not_set() -> OxenError {
        OxenError::basic_str("Remote not set. `oxen set-remote <remote-name> <url>`")
    }

    pub fn remote_branch_not_found<T: AsRef<str>>(name: T) -> OxenError {
        let err = format!("Remote branch `{}` not found", name.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn commit_db_corrupted<T: AsRef<str>>(commit_id: T) -> OxenError {
        let err = format!("Commit db currupted, could not find commit: {}", commit_id.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn local_parent_link_broken<T: AsRef<str>>(commit_id: T) -> OxenError {
        let err = format!("Broken link to parent commit: {}", commit_id.as_ref());
        OxenError::basic_str(&err)
    }

    pub fn local_file_not_found<T: AsRef<Path>>(path: T) -> OxenError {
        let err = format!("Could not find local file: {:?}", path.as_ref());
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
