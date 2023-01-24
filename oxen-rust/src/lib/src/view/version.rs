use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VersionResponse {
    pub status: String,
    pub status_message: String,
    pub oxen_version: String,
}
