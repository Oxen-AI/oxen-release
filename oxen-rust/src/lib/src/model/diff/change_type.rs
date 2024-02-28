use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum ChangeType {
    Added,
    Removed,
    Modified,
    Unchanged,
}
