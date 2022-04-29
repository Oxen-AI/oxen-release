use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RemoteEntry {
    pub id: String,
    pub data_type: String,
    pub url: String,
    pub filename: String,
    pub hash: String,
}
