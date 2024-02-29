use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AddRemoveModifyCounts {
    pub added: usize,
    pub removed: usize,
    pub modified: usize,
}
