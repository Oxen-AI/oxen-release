use serde::{Deserialize, Serialize};

use crate::api;
use crate::model::metadata::metadata_image::{ImgColorSpace, MetadataImage};
use crate::model::{Commit, CommitEntry, EntryDataType, LocalRepository};
use crate::view::entry::ResourceVersion;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataText {
    pub num_lines: usize,
    // pub num_words: usize,
    // pub num_chars: usize,
    // pub num_whitespace: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataVideo {
    pub num_seconds: f64,
    pub width: usize,
    pub height: usize,
    pub color_space: ImgColorSpace, // RGB, RGBA, etc.
    pub format: String,             // mp4, etc.
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataAudio {
    pub num_seconds: f64,
    pub format: String, // mp3, etc.
    pub num_channels: usize,
    pub sample_rate: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataTabular {
    pub width: usize,
    pub height: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataItem {
    pub text: Option<MetadataText>,
    pub image: Option<MetadataImage>,
    pub video: Option<MetadataVideo>,
    pub audio: Option<MetadataAudio>,
    pub tabular: Option<MetadataTabular>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CLIMetadataEntry {
    pub filename: String,
    pub last_updated: Option<Commit>,
    // Hash of the file
    pub hash: String,
    // size of the file in bytes
    pub size: u64,
    // high level type of "image", "text", "video", "audio", "tabular"
    pub data_type: EntryDataType,
    // auto detected mime type of the file (e.g. "image/png")
    pub mime_type: String,
    // auto detected extension of the file
    pub extension: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataEntry {
    pub filename: String,
    pub is_dir: bool,
    pub latest_commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
    // size of the file in bytes
    pub size: u64,
    // high level type of "image", "text", "video", "audio", "tabular"
    pub data_type: EntryDataType,
    // auto detected mime type of the file (e.g. "image/png")
    pub mime_type: String,
    // auto detected extension of the file
    pub extension: String,
}

impl MetadataEntry {
    pub fn from_commit_entry(
        repo: &LocalRepository,
        entry: Option<&CommitEntry>,
    ) -> Option<MetadataEntry> {
        entry?;
        match api::local::metadata::from_commit_entry(repo, entry.unwrap()) {
            Ok(metadata) => Some(metadata),
            Err(_) => None,
        }
    }
}
