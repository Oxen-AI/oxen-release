use std::path::{Path, PathBuf};

use crate::core;
use crate::core::merge::merge_conflict_reader::MergeConflictReader;
use crate::core::merge::node_merge_conflict_reader::NodeMergeConflictReader;
use crate::core::v0_10_0::index::CommitReader;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::Commit;
use crate::model::{merge_conflict::NodeMergeConflict, Branch, LocalRepository, MergeConflict};

pub struct MergeCommits {
    pub lca: Commit,
    pub base: Commit,
    pub merge: Commit,
}

impl MergeCommits {
    pub fn is_fast_forward_merge(&self) -> bool {
        self.lca.id == self.base.id
    }
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
            core::v0_19_0::merge::has_conflicts(repo, base_branch, merge_branch)
        }
    }
}

pub fn can_merge_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<bool, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            let reader = &CommitReader::new(repo)?;
            merger.can_merge_commits(reader, base_commit, merge_commit)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::merge::can_merge_commits(repo, base_commit, merge_commit)
        }
    }
}

pub fn list_conflicts_between_branches(
    repo: &LocalRepository,
    base_branch: &Branch,
    merge_branch: &Branch,
) -> Result<Vec<PathBuf>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            let reader = &CommitReader::new(repo)?;

            merger.list_conflicts_between_branches(reader, base_branch, merge_branch)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::merge::list_conflicts_between_branches(repo, base_branch, merge_branch)
        }
    }
}

pub fn list_commits_between_branches(
    repo: &LocalRepository,
    base_branch: &Branch,
    head_branch: &Branch,
) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            let reader = &CommitReader::new(repo)?;
            merger.list_commits_between_branches(reader, base_branch, head_branch)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::merge::list_commits_between_branches(repo, base_branch, head_branch)
        }
    }
}

pub fn list_commits_between_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            let reader = &CommitReader::new(repo)?;
            merger.list_commits_between_commits(reader, base_commit, head_commit)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::merge::list_commits_between_commits(repo, base_commit, head_commit)
        }
    }
}

pub fn list_conflicts_between_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Vec<PathBuf>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            let reader = &CommitReader::new(repo)?;
            merger.list_conflicts_between_commits(reader, base_commit, merge_commit)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::merge::list_conflicts_between_commits(repo, base_commit, merge_commit)
        }
    }
}

pub fn merge_into_base(
    repo: &LocalRepository,
    merge_branch: &Branch,
    base_branch: &Branch,
) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            merger.merge_into_base(merge_branch, base_branch)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::merge::merge_into_base(repo, merge_branch, base_branch)
        }
    }
}

pub fn merge(
    repo: &LocalRepository,
    branch_name: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            merger.merge(branch_name)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::merge::merge(repo, branch_name),
    }
}

pub fn merge_commit_into_base(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            merger.merge_commit_into_base(merge_commit, base_commit)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::merge::merge_commit_into_base(repo, merge_commit, base_commit)
        }
    }
}

pub fn merge_commit_into_base_on_branch(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
    branch: &Branch,
) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            merger.merge_commit_into_base_on_branch(merge_commit, base_commit, branch)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::merge::merge_commit_into_base_on_branch(
            repo,
            merge_commit,
            base_commit,
            branch,
        ),
    }
}

pub fn has_file(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            merger.has_file(path)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::merge::has_file(repo, path),
    }
}

pub fn remove_conflict_path(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            merger.remove_conflict_path(path)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::merge::remove_conflict_path(repo, path),
    }
}

pub fn find_merge_commits<S: AsRef<str>>(
    repo: &LocalRepository,
    branch_name: S,
) -> Result<MergeCommits, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            merger.find_merge_commits(branch_name)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::merge::find_merge_commits(repo, branch_name),
    }
}

pub fn lowest_common_ancestor_from_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Commit, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let merger = core::v0_10_0::index::merger::Merger::new(repo)?;
            let reader = &CommitReader::new(repo)?;
            merger.lowest_common_ancestor_from_commits(reader, base_commit, merge_commit)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::merge::lowest_common_ancestor_from_commits(
            repo,
            base_commit,
            merge_commit,
        ),
    }
}
