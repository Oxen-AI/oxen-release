use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Remote {
    pub name: String,
    pub url: String,
}
