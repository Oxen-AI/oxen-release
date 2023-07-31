use serde::{Deserialize, Serialize};

use crate::model::metadata::metadata_image::MetadataImage;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CountChange {
    pub added: usize,
    pub removed: usize,
    pub modified: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SizeChange {
    pub delta: usize,
    pub base: usize,
    pub head: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ImageDiff {
    pub base: MetadataImage,
    pub head: MetadataImage,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DiffEntryChanges {
    pub size: SizeChange,
    pub file_counts: CountChange,
    // pub image: Option<ImageDiff>
}
