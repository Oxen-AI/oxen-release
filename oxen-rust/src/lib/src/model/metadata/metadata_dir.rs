use serde::{Deserialize, Serialize};

use crate::view::DataTypeCount;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataDir {
    pub dir: MetadataDirImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataDirImpl {
    pub data_types: Vec<DataTypeCount>,
}

impl MetadataDir {
    pub fn new(data_types: Vec<DataTypeCount>) -> Self {
        Self {
            dir: MetadataDirImpl { data_types },
        }
    }
}
