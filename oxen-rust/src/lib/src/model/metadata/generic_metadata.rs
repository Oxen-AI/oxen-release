use serde::{Deserialize, Serialize};

use crate::model::metadata::{
    MetadataAudio, MetadataDir, MetadataImage, MetadataTabular, MetadataText, MetadataVideo,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum GenericMetadata {
    MetadataDir(MetadataDir),
    MetadataText(MetadataText),
    MetadataImage(MetadataImage),
    MetadataVideo(MetadataVideo),
    MetadataAudio(MetadataAudio),
    MetadataTabular(MetadataTabular),
}
