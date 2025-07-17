use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Namespace {
    pub name: String,
    pub storage_usage_gb: f64,
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({} GB)", self.name, self.storage_usage_gb)
    }
}

impl std::error::Error for Namespace {}
