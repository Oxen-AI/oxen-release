use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataVideo {
    pub num_seconds: f64,
    pub width: usize,
    pub height: usize,
}
