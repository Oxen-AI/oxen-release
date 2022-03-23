use serde::Deserialize;

#[derive(Deserialize, Debug, Clone, Hash)]
pub struct Dataset {
    pub id: String,
    pub name: String,
}

impl PartialEq for Dataset {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
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
