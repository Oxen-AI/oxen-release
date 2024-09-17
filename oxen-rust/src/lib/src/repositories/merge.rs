use crate::core;
use crate::core::merge::merge_conflict_reader::MergeConflictReader;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository, MergeConflict};

pub fn list_conflicts(repo: &LocalRepository) -> Result<Vec<MergeConflict>, OxenError> {
    let merger = MergeConflictReader::new(repo)?;
    let conflicts = merger.list_conflicts()?;
    Ok(conflicts)
}

pub fn has_conflicts(
    repo: &LocalRepository,
    base_branch: &Branch,
    merge_branch: &Branch,
) -> Result<bool, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            merger.has_conflicts(base_branch, merge_branch)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::index::merger::has_conflicts(repo, base_branch, merge_branch)
        }
    }
}
