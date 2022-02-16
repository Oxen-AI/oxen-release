
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Dataset {
  pub id: String,
  pub name: String,
}

#[derive(Deserialize, Debug)]
pub struct DatasetResponse {
  pub dataset: Dataset,
}

#[derive(Deserialize, Debug)]
pub struct ListDatasetsResponse {
  pub datasets: Vec<Dataset>,
}

