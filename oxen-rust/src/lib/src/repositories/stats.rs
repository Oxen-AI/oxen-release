use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::RepoStats;
use crate::core::versions::MinOxenVersion;
use crate::core;

pub fn get_stats(repo: &LocalRepository) -> Result<RepoStats, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => Ok(core::v0_10_0::stats::get_stats(repo)),
        _ => core::v_latest::stats::get_stats(repo),
    }
}