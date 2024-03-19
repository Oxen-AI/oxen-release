use serde::{Serialize, Deserialize};
use crate::model::RemoteDataset;
use crate::view::StatusMessage;

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoteDatasetResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub dataset: RemoteDataset,
}