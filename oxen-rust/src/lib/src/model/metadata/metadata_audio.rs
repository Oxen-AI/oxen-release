use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataAudio {
    pub num_seconds: f64,
    pub num_channels: usize,
    pub sample_rate: usize,
}
