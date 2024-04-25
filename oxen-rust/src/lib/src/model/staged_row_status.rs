pub enum StagedRowStatus {
    Added,
    Modified,
    Deleted,
    Unchanged,
}

impl StagedRowStatus {
    pub fn to_string(&self) -> String {
        match self {
            StagedRowStatus::Added => "added".to_string(),
            StagedRowStatus::Modified => "modified".to_string(),
            StagedRowStatus::Deleted => "deleted".to_string(),
            StagedRowStatus::Unchanged => "unchanged".to_string(),
        }
    }
}
