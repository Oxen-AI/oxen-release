use serde::{Deserialize, Serialize};

use super::metadata_image::ImgColorSpace;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataVideo {
    pub num_seconds: f64,
    pub width: usize,
    pub height: usize,
    pub color_space: ImgColorSpace, // RGB, RGBA, etc.
    pub format: String,             // mp4, etc.
}
