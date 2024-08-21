use crate::core::merge::merge_conflict_reader::MergeConflictReader;
use crate::error::OxenError;
use crate::model::{LocalRepository, MergeConflict};

pub fn list_conflicts(repo: &LocalRepository) -> Result<Vec<MergeConflict>, OxenError> {
    let merger = MergeConflictReader::new(repo)?;
    let conflicts = merger.list_conflicts()?;
    Ok(conflicts)
}
