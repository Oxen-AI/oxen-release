use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct DataTypeCount {
    pub count: usize,
    pub data_type: String,
}
