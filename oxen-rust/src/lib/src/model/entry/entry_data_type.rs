use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Eq, Hash, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EntryDataType {
    Dir,
    Text,
    Image,
    Video,
    Audio,
    Tabular,
    Binary,
}
