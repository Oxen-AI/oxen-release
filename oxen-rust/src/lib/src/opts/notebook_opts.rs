use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_NOTEBOOK_BASE_IMAGE};

#[derive(Clone, Debug)]
pub struct NotebookOpts {
    pub notebook: String, // path or id
    pub branch: String,
    pub base_image: String,
    pub mode: String, // "edit", "script"
    pub gpu_model: Option<String>,
    pub cpu_cores: u32,
    pub memory_mb: u32,
    pub timeout_secs: u32,
    pub notebook_base_image_id: Option<String>,
    pub build_script: Option<String>,
    pub script_args: Option<String>,
}

impl Default for NotebookOpts {
    fn default() -> Self {
        Self {
            notebook: "".to_string(),
            branch: DEFAULT_BRANCH_NAME.to_string(),
            base_image: DEFAULT_NOTEBOOK_BASE_IMAGE.to_string(),
            mode: "edit".to_string(),
            gpu_model: None,
            cpu_cores: 1,
            memory_mb: 1024,
            timeout_secs: 3600,
            notebook_base_image_id: None,
            build_script: None,
            script_args: None,
        }
    }
}
