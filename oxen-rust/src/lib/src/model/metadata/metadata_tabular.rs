use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataTabular {
    pub width: usize,
    pub height: usize,
}
