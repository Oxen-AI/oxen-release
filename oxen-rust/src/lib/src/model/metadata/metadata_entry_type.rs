use serde::{Deserialize, Serialize};

use crate::model::metadata::{
    MetadataAudio, MetadataDir, MetadataImage, MetadataTabular, MetadataText, MetadataVideo,
};

#[derive(Deserialize, Serialize, Debug, Clone)]

pub struct EntryTypeMetadata {
    pub dir: Option<MetadataDir>,
    pub text: Option<MetadataText>,
    pub image: Option<MetadataImage>,
    pub video: Option<MetadataVideo>,
    pub audio: Option<MetadataAudio>,
    pub tabular: Option<MetadataTabular>,
}
