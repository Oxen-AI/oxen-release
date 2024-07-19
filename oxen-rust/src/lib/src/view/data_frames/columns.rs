use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct NewColumn {
    pub name: String,
    pub data_type: String,
}
