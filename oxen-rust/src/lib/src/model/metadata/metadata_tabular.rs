use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataTabular {
    pub tabular: MetadataTabularImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataTabularImpl {
    pub width: usize,
    pub height: usize,
}

impl MetadataTabular {
    pub fn new(width: usize, height: usize) -> Self {
        Self {
            tabular: MetadataTabularImpl { width, height },
        }
    }
}
