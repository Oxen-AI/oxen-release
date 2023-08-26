use crate::model::{EntryDataType, RemoteRepository};
use serde::{Deserialize, Serialize};

use super::{DataTypeCount, StatusMessage};
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RepositoryView {
    pub namespace: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RepositoryDataTypesView {
    pub namespace: String,
    pub name: String,
    pub size: u64,
    pub data_types: Vec<DataTypeCount>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RepositoryResponse {
    pub status: String,
    pub status_message: String,
    pub repository: RepositoryView,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RepositoryDataTypesResponse {
    pub status: String,
    pub status_message: String,
    pub repository: RepositoryDataTypesView,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListRepositoryResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub repositories: Vec<RepositoryView>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RepositoryResolveResponse {
    pub status: String,
    pub status_message: String,
    pub repository_api_url: String,
}

impl RepositoryView {
    pub fn from_remote(repository: RemoteRepository) -> RepositoryView {
        RepositoryView {
            namespace: repository.namespace.clone(),
            name: repository.name,
        }
    }
}

impl RepositoryDataTypesView {
    pub fn total_files(&self) -> usize {
        self.data_types.iter().map(|dt| dt.count).sum()
    }

    pub fn data_types_str(data_type_counts: &Vec<DataTypeCount>) -> String {
        let mut data_types_str = String::new();
        for data_type_count in data_type_counts {
            if data_type_count.count == 0 {
                continue;
            }
            if let Ok(edt) = EntryDataType::from_str(&data_type_count.data_type) {
                let emoji = edt.to_emoji();
                let data = format!(
                    "{} {} ({})\t",
                    emoji, data_type_count.data_type, data_type_count.count
                );
                data_types_str.push_str(&data);
            } else {
                let data = format!(
                    "{} ({})\t",
                    data_type_count.data_type, data_type_count.count
                );
                data_types_str.push_str(&data);
            }
        }
        data_types_str
    }
}
