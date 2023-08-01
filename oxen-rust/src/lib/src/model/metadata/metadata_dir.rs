use serde::{Deserialize, Serialize};

use crate::view::JsonDataFrame;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataDir {
    pub data_types: JsonDataFrame,
    pub mime_types: JsonDataFrame,
}
