use crate::model::Namespace;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct NamespaceView {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListNamespacesResponse {
    pub status: String,
    pub status_message: String,
    pub namespaces: Vec<NamespaceView>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NamespaceResponse {
    pub status: String,
    pub status_message: String,
    pub namespace: Namespace,
}
