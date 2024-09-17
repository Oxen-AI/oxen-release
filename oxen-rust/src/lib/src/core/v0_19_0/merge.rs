use crate::config::UserConfig;
use crate::core::db;
use crate::core::merge::db_path;
pub use crate::core::merge::merge_conflict_db_reader::MergeConflictDBReader;
use crate::core::merge::merge_conflict_writer;
use crate::core::oxenignore;
use crate::core::refs::{RefReader, RefWriter};
use crate::core::v0_10_0::index::{
    CommitEntryReader, CommitEntryWriter, CommitReader, CommitWriter, SchemaReader, Stager,
};
use crate::error::OxenError;
use crate::model::merge_conflict::NodeMergeConflict;
use crate::model::merkle_tree::node::FileNode;
use crate::model::{Branch, Commit, CommitEntry, LocalRepository, MergeConflict};
use crate::repositories;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded, DB};
use std::path::{Path, PathBuf};
use std::str;

use super::index::{self, CommitMerkleTree};
use super::restore;

pub struct MergeCommits {
    lca: Commit,
    base: Commit,
    merge: Commit,
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
    let base_commit =
        repositories::commits::get_commit_or_head(&repo, Some(base_branch.commit_id))?;
    let merge_commit =
        repositories::commits::get_commit_or_head(&repo, Some(merge_branch.commit_id))?;

    Ok(can_merge_commits(&repo, &base_commit, &merge_commit)?)
}

/// Check if there are conflicts between the merge commit and the base commit
/// Returns true if there are no conflicts, false if there are conflicts
pub fn can_merge_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<bool, OxenError> {
    let lca = lowest_common_ancestor_from_commits(repo, base_commit, merge_commit)?;
    let merge_commits = MergeCommits {
        lca,
        base: base_commit.clone(),
        merge: merge_commit.clone(),
    };

    if merge_commits.is_fast_forward_merge() {
        // If it is fast forward merge, there are no merge conflicts
        return Ok(true);
    }

    let write_to_disk = false;
    let conflicts = find_merge_conflicts(&repo, &merge_commits, write_to_disk)?;
    Ok(conflicts.is_empty())
}

pub fn lowest_common_ancestor_from_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Commit, OxenError> {
    // Traverse the base commit back to start, keeping map of Commit -> Depth(int)
    let commit_depths_from_head =
        repositories::commits::list_from_with_depth(repo, base_commit.id.as_str())?;

    // Traverse the merge commit back
    //   check at each step if ID is in the HEAD commit history
    //   The lowest Depth Commit in HEAD should be the LCA
    let commit_depths_from_merge =
        repositories::commits::list_from_with_depth(repo, merge_commit.id.as_str())?;

    let mut min_depth = usize::MAX;
    let mut lca: Commit = commit_depths_from_head.keys().next().unwrap().clone();
    for (commit, _) in commit_depths_from_merge.iter() {
        if let Some(depth) = commit_depths_from_head.get(commit) {
            if depth < &min_depth {
                min_depth = *depth;
                log::debug!("setting new lca, {:?}", commit);
                lca = commit.clone();
            }
        }
    }

    Ok(lca)
}

/// Will try a three way merge and return conflicts if there are any to indicate that the merge was unsuccessful
fn find_merge_conflicts(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
    write_to_disk: bool,
) -> Result<Vec<NodeMergeConflict>, OxenError> {
    log::debug!("finding merge conflicts");
    /*
    https://en.wikipedia.org/wiki/Merge_(version_control)#Three-way_merge

    C = LCA
    A = Base
    B = Merge
    D = Resulting merge commit

    C - A - D
      \   /
        B

    The three-way merge looks for sections which are the same in only two of the three files.
    In this case, there are two versions of the section,
        and the version which is in the common ancestor "C" is discarded,
        while the version that differs is preserved in the output.
    If "A" and "B" agree, that is what appears in the output.
    A section that is the same in "A" and "C" outputs the changed version in "B",
    and likewise a section that is the same in "B" and "C" outputs the version in "A".

    Sections that are different in all three files are marked as a conflict situation and left for the user to resolve.
    */

    // We will return conflicts if there are any
    let mut conflicts: Vec<NodeMergeConflict> = vec![];

    // Read all the entries from each commit into sets we can compare to one another
    let lca_commit_node = CommitMerkleTree::from_commit(repo, &merge_commits.lca)?;
    let base_commit_node = CommitMerkleTree::from_commit(repo, &merge_commits.base)?;
    let merge_commit_node = CommitMerkleTree::from_commit(repo, &merge_commits.merge)?;

    let lca_entries =
        CommitMerkleTree::dir_entries_with_paths(&lca_commit_node.root, &PathBuf::from("/"))?;
    let base_entries =
        CommitMerkleTree::dir_entries_with_paths(&base_commit_node.root, &PathBuf::from("/"))?;
    let merge_entries =
        CommitMerkleTree::dir_entries_with_paths(&merge_commit_node.root, &PathBuf::from("/"))?;

    log::debug!("lca_entries.len() {}", lca_entries.len());
    log::debug!("base_entries.len() {}", base_entries.len());
    log::debug!("merge_entries.len() {}", merge_entries.len());

    // Check all the entries in the candidate merge
    for merge_entry in merge_entries.iter() {
        // log::debug!("Considering entry {}", merge_entries.len());
        // Check if the entry exists in all 3 commits
        if let Some(base_entry) = base_entries.get(&merge_entry) {
            if let Some(lca_entry) = lca_entries.get(merge_entry) {
                // If Base and LCA are the same but Merge is different, take merge
                // log::debug!(
                //     "Comparing hashes merge_entry {:?} BASE {} LCA {} MERGE {}",
                //     merge_entry.path,
                //     base_entry.hash,
                //     lca_entry.hash,
                //     merge_entry.hash
                // );
                if base_entry.0.hash == lca_entry.0.hash
                    && base_entry.0.hash != merge_entry.0.hash
                    && write_to_disk
                {
                    log::debug!("top update entry");
                    update_entry(repo, merge_entry)?;
                }

                // If all three are different, mark as conflict
                if base_entry.0.hash != lca_entry.0.hash
                    && lca_entry.0.hash != merge_entry.0.hash
                    && base_entry.0.hash != merge_entry.0.hash
                {
                    conflicts.push(NodeMergeConflict {
                        lca_entry: lca_entry.to_owned(),
                        base_entry: base_entry.to_owned(),
                        merge_entry: merge_entry.to_owned(),
                    });
                }
            } else {
                // merge entry doesn't exist in LCA, so just check if it's different from base
                if base_entry.0.hash != merge_entry.0.hash {
                    conflicts.push(NodeMergeConflict {
                        lca_entry: base_entry.to_owned(),
                        base_entry: base_entry.to_owned(),
                        merge_entry: merge_entry.to_owned(),
                    });
                }
            }
        } else if write_to_disk {
            // merge entry does not exist in base, so create it
            log::debug!("bottom update entry");
            update_entry(repo, merge_entry)?;
        }
    }
    log::debug!("three_way_merge conflicts.len() {}", conflicts.len());

    Ok(conflicts)
}

fn update_entry(
    repo: &LocalRepository,
    merge_entry: &(FileNode, PathBuf),
) -> Result<(), OxenError> {
    index::restore::restore_file(repo, &merge_entry.0, &merge_entry.1)?;
    Ok(())
}
