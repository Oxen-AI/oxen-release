use serde::{Deserialize, Serialize};

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
pub struct MetadataImage {
    pub width: usize,
    pub height: usize,
    pub color_space: ImgColorSpace, // RGB, RGBA, etc.
}
