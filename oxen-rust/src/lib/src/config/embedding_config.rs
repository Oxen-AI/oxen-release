use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const EMBEDDING_CONFIG_FILENAME: &str = "embeddings.toml";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EmbeddingColumn {
    pub name: String,
    pub vector_length: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EmbeddingConfig {
    // Map of column name to embedding vector length
    pub columns: HashMap<String, EmbeddingColumn>,
}
