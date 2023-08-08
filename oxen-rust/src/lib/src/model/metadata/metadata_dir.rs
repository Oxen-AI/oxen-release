use serde::{Deserialize, Serialize};

use crate::view::DataTypeCount;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataDir {
    pub data_types: Vec<DataTypeCount>,
}
