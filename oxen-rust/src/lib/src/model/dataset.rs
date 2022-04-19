use serde::Deserialize;

#[derive(Deserialize, Debug, Clone, PartialEq, Hash)]
pub struct Dataset {
    pub id: String,
    pub name: String,
}

impl Eq for Dataset {}

#[derive(Deserialize, Debug)]
pub struct DatasetResponse {
    pub dataset: Dataset,
}

#[derive(Deserialize, Debug)]
pub struct ListDatasetsResponse {
    pub datasets: Vec<Dataset>,
}
