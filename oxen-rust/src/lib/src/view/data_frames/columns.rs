use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct NewColumn {
    pub name: String,
    pub data_type: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ColumnToDelete {
    pub name: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ColumnToRestore {
    pub name: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ColumnToUpdate {
    pub name: String,
    pub new_name: Option<String>,
    pub new_data_type: Option<String>,
}
