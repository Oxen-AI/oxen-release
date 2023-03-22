#[derive(Clone, Debug)]
pub enum AppendDataType {
    Json,
    Csv,
}

#[derive(Clone, Debug)]
pub struct AppendOpts {
    pub data_type: AppendDataType,
    pub remote: bool,
}
