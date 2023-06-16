use serde::{Deserialize, Serialize};

use crate::model::{Commit, EntryDataType};
use crate::view::entry::ResourceVersion;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum ImgColorSpace {
    // 8-bit
    RGB,
    RGBA,
    Grayscale,
    GrayscaleAlpha,

    // 16-bit
    Rgb16,
    Rgba16,
    Grayscale16,
    GrayscaleAlpha16,

    // 32-bit float
    Rgb32F,
    Rgba32F,

    Unknown,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetaDataText {
    pub num_lines: usize,
    // pub num_words: usize,
    // pub num_chars: usize,
    // pub num_whitespace: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetaDataImage {
    pub width: usize,
    pub height: usize,
    pub color_space: ImgColorSpace, // RGB, RGBA, etc.
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetaDataVideo {
    pub num_seconds: f64,
    pub width: usize,
    pub height: usize,
    pub color_space: ImgColorSpace, // RGB, RGBA, etc.
    pub format: String,             // mp4, etc.
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetaDataAudio {
    pub num_seconds: f64,
    pub format: String, // mp3, etc.
    pub num_channels: usize,
    pub sample_rate: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetaDataTabular {
    pub width: usize,
    pub height: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetaData {
    pub text: Option<MetaDataText>,
    pub image: Option<MetaDataImage>,
    pub video: Option<MetaDataVideo>,
    pub audio: Option<MetaDataAudio>,
    pub tabular: Option<MetaDataTabular>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetaDataEntry {
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
    // type specific meta data
    pub meta: MetaData,
}
