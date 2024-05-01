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
}
