use crate::error::OxenError;

pub enum StagedRowStatus {
    Added,
    Modified,
    Removed,
    Unchanged,
}

impl StagedRowStatus {
    pub fn to_string(&self) -> String {
        match self {
            StagedRowStatus::Added => "added".to_string(),
            StagedRowStatus::Modified => "modified".to_string(),
            StagedRowStatus::Removed => "removed".to_string(),
            StagedRowStatus::Unchanged => "unchanged".to_string(),
        }
    }

    pub fn from_string(s: &str) -> Result<StagedRowStatus, OxenError> {
        match s {
            "added" => Ok(StagedRowStatus::Added),
            "modified" => Ok(StagedRowStatus::Modified),
            "removed" => Ok(StagedRowStatus::Removed),
            "unchanged" => Ok(StagedRowStatus::Unchanged),
            _ => Err(OxenError::basic_str("Invalid row status")),
        }
    }
}
