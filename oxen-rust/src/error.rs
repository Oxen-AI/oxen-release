use std::error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum OxenError {
    IO(io::Error),
    Basic(String),
    TOML(toml::ser::Error),
    URI(http::uri::InvalidUri)
}

impl OxenError {
    pub fn basic_str(s: &str) -> Self {
        OxenError::Basic(String::from(s))
    }
}

impl fmt::Display for OxenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let OxenError::Basic(err) = self {
            write!(f, "{:?}", err)
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
        OxenError::TOML(error)
    }
}

impl From<http::uri::InvalidUri> for OxenError {
    fn from(error: http::uri::InvalidUri) -> Self {
        OxenError::URI(error)
    }
}
