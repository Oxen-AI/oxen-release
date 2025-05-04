use serde::{Deserialize, Serialize};

use super::StatusMessage;

#[derive(Clone, Debug, Serialize)]
pub struct NotebookRequest {
    pub notebook_base_image_id: String,
    pub run_as_script: bool,
    pub script_args: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Notebook {
    pub id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NotebookResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub notebook: Notebook,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NotebookBaseImage {
    pub id: String,
    pub image_definition: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NotebookBaseImagesResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub notebook_base_images: Vec<NotebookBaseImage>,
}
