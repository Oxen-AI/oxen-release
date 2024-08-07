//! Enumeration of supported Oxen Versions
//!

use std::fmt::Display;

use crate::error::OxenError;

pub enum MinOxenVersion {
    V0_10_0,
    V0_19_0,
}

impl MinOxenVersion {
    /// Should default to latest if none is supplied in most cases
    pub fn or_latest(s: Option<String>) -> Result<MinOxenVersion, OxenError> {
        if let Some(version) = s {
            MinOxenVersion::from_string(version)
        } else {
            Ok(MinOxenVersion::V0_19_0)
        }
    }

    /// Only use this if we have no version specified in an .oxen/config.toml file
    pub fn or_earliest(s: Option<String>) -> Result<MinOxenVersion, OxenError> {
        if let Some(version) = s {
            MinOxenVersion::from_string(version)
        } else {
            Ok(MinOxenVersion::V0_10_0)
        }
    }

    pub fn from_string(s: impl AsRef<str>) -> Result<MinOxenVersion, OxenError> {
        match s.as_ref() {
            "v0.10.0" => Ok(MinOxenVersion::V0_10_0),
            "v0.19.0" => Ok(MinOxenVersion::V0_19_0),
            _ => Err(OxenError::invalid_version(s.as_ref())),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            MinOxenVersion::V0_10_0 => "v0.10.0",
            MinOxenVersion::V0_19_0 => "v0.19.0",
        }
    }
}

impl Display for MinOxenVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
