use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Namespace {
    pub name: String,
    pub storage_usage_gb: f64,
}
