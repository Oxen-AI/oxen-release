use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct ForkRequest {
    pub namespace: String,
    pub new_repo_name: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ForkStatus {
    Counting(u32),
    InProgress(f32),
    Complete,
    Failed(String),
}

#[derive(Serialize, Deserialize)]
pub struct ForkStatusFile {
    pub status: String,
    pub progress: Option<f32>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ForkStartResponse {
    pub repository: String,
    pub fork_status: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ForkStatusResponse {
    pub repository: String,
    pub status: String,
    pub progress: Option<f32>,
    pub error: Option<String>,
}

impl From<ForkStatus> for ForkStatusFile {
    fn from(status: ForkStatus) -> Self {
        match status {
            ForkStatus::Counting(c) => ForkStatusFile {
                status: "counting".to_string(),
                progress: Some(c as f32),
                error: None,
            },
            ForkStatus::InProgress(p) => ForkStatusFile {
                status: "in_progress".to_string(),
                progress: Some(p),
                error: None,
            },
            ForkStatus::Complete => ForkStatusFile {
                status: "complete".to_string(),
                progress: None,
                error: None,
            },
            ForkStatus::Failed(e) => ForkStatusFile {
                status: "failed".to_string(),
                progress: None,
                error: Some(e),
            },
        }
    }
}
