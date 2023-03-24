use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::model::ContentType;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum ModType {
    Append,
    Delete,
    Modify,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ModEntry {
    pub uuid: String,
    pub modification_type: ModType, // append, delete, modify
    pub content_type: ContentType,  // text, json, csv
    pub data: String,
    pub path: PathBuf,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
}
