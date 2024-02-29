use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum ChangeType {
    Added,
    Removed,
    Modified,
    Unchanged,
}
