use crate::model::Namespace;
use serde::{Deserialize, Serialize};

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug)]
pub struct NamespaceView {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListNamespacesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub namespaces: Vec<NamespaceView>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NamespaceResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub namespace: Namespace,
}
