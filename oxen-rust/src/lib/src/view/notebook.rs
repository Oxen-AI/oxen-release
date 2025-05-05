use serde::{Deserialize, Serialize};

use super::StatusMessage;
use crate::opts::NotebookOpts;

#[derive(Clone, Debug, Serialize)]
pub struct NotebookRequest {
    pub notebook_base_image_id: String,
    pub run_as_script: bool,
    pub script_args: Option<String>,
    pub gpu_model: Option<String>,
    pub cpu_cores: u32,
    pub memory_mb: u32,
    pub timeout_secs: u32,
    pub build_script: Option<String>,
}

impl NotebookRequest {
    pub fn new(opts: &NotebookOpts) -> Self {
        Self {
            notebook_base_image_id: opts.base_image.clone(),
            run_as_script: &opts.mode == "script",
            script_args: opts.script_args.clone(),
            gpu_model: opts.gpu_model.clone(),
            cpu_cores: opts.cpu_cores,
            memory_mb: opts.memory_mb,
            timeout_secs: opts.timeout_secs,
            build_script: opts.build_script.clone(),
        }
    }
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
