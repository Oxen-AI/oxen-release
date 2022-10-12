use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub enum EntryType {
    Regular, // any old file
    Tabular, // file we want to track row level changes
}
