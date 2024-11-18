use serde::{Deserialize, Serialize};
use std::str::FromStr;

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
    pub status: ForkStatus,
    pub progress: Option<f32>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ForkStartResponse {
    pub repository: String,
    pub fork_status: ForkStatus,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ForkStatusResponse {
    pub repository: String,
    pub status: ForkStatus,
    pub progress: Option<f32>,
    pub error: Option<String>,
}

impl From<ForkStatus> for ForkStatusFile {
    fn from(status: ForkStatus) -> Self {
        match status {
            ForkStatus::Counting(c) => ForkStatusFile {
                status: ForkStatus::Counting(c),
                progress: Some(c as f32),
                error: None,
            },
            ForkStatus::InProgress(p) => ForkStatusFile {
                status: ForkStatus::InProgress(p),
                progress: Some(p),
                error: None,
            },
            ForkStatus::Complete => ForkStatusFile {
                status: ForkStatus::Complete,
                progress: None,
                error: None,
            },
            ForkStatus::Failed(e) => ForkStatusFile {
                status: ForkStatus::Failed(e.clone()),
                progress: None,
                error: Some(e),
            },
        }
    }
}

impl FromStr for ForkStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "counting" => Ok(ForkStatus::Counting(0)),
            "in_progress" => Ok(ForkStatus::InProgress(0.0)),
            "complete" => Ok(ForkStatus::Complete),
            "failed" => Ok(ForkStatus::Failed(String::new())),
            _ => Err(format!("Invalid status: {}", s)),
        }
    }
}
