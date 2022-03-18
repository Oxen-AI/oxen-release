use std::error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum OxenError {
    IO(io::Error),
    Basic(String),
    TOML(toml::ser::Error),
}

impl fmt::Display for OxenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
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
