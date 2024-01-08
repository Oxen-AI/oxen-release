use crate::api;
use crate::constants::MERGE_DIR;
use crate::core::db;
use crate::core::index::{
    oxenignore, CommitEntryReader, CommitReader, CommitWriter, MergeConflictDBReader, RefReader,
    RefWriter, SchemaReader, Stager,
};
use crate::error::OxenError;
use crate::model::{commit, Branch, Commit, CommitEntry, LocalRepository, MergeConflict};

use crate::util;

use rocksdb::DB;
use std::path::{Path, PathBuf};
use std::str;

use super::{merge_conflict_writer, restore};

pub fn db_path(repo: &LocalRepository) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path).join(Path::new(MERGE_DIR))
}

// This is a struct to find the commits we want to merge
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

pub struct Merger {
    repository: LocalRepository,
    merge_db: DB,
}

impl Merger {
    /// Create a new merger
    pub fn new(repo: &LocalRepository) -> Result<Merger, OxenError> {
        let db_path = db_path(repo);
        log::debug!("Merger::new() DB {:?}", db_path);
        let opts = db::opts::default();
        Ok(Merger {
            repository: repo.to_owned(),
            merge_db: DB::open(&opts, dunce::simplified(&db_path))?,
        })
    }

    /// Check if there are conflicts between the branch you are trying to merge and the base branch
    /// Returns true if there are conflicts, false if there are not
    pub fn has_conflicts(
        &self,
        base_branch: &Branch,
        merge_branch: &Branch,
    ) -> Result<bool, OxenError> {
        let commit_reader = CommitReader::new(&self.repository)?;
        let base_commit = Commit::from_branch(&commit_reader, base_branch)?;
        let merge_commit = Commit::from_branch(&commit_reader, merge_branch)?;

        Ok(!self.can_merge_commits(&commit_reader, &base_commit, &merge_commit)?)
    }

    /// Check if there are conflicts between the merge commit and the base commit
    /// Returns true if there are no conflicts, false if there are conflicts
    pub fn can_merge_commits(
        &self,
        commit_reader: &CommitReader,
        base_commit: &Commit,
        merge_commit: &Commit,
    ) -> Result<bool, OxenError> {
        let lca =
            self.lowest_common_ancestor_from_commits(commit_reader, base_commit, merge_commit)?;
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
        let conflicts = self.find_merge_conflicts(&merge_commits, write_to_disk)?;
        Ok(conflicts.is_empty())
    }

    pub fn list_conflicts_between_branches(
        &self,
        commit_reader: &CommitReader,
        base_branch: &Branch,
        merge_branch: &Branch,
    ) -> Result<Vec<PathBuf>, OxenError> {
        let base_commit = Commit::from_branch(commit_reader, base_branch)?;
        let merge_commit = Commit::from_branch(commit_reader, merge_branch)?;

        self.list_conflicts_between_commits(commit_reader, &base_commit, &merge_commit)
    }

    pub fn list_commits_between_branches(
        &self,
        reader: &CommitReader,
        base_branch: &Branch,
        head_branch: &Branch,
    ) -> Result<Vec<Commit>, OxenError> {
        log::debug!(
            "list_commits_between_branches() base: {:?} head: {:?}",
            base_branch,
            head_branch
        );
        let base_commit = Commit::from_branch(reader, base_branch)?;
        let head_commit = Commit::from_branch(reader, head_branch)?;

        let lca = self.lowest_common_ancestor_from_commits(reader, &base_commit, &head_commit)?;

        reader.history_from_base_to_head(&lca.id, &head_commit.id)
    }

    pub fn list_commits_between_commits(
        &self,
        reader: &CommitReader,
        base_commit: &Commit,
        head_commit: &Commit,
    ) -> Result<Vec<Commit>, OxenError> {
        log::debug!(
            "list_commits_between_commits() base: {:?} head: {:?}",
            base_commit,
            head_commit
        );

        let lca = self.lowest_common_ancestor_from_commits(reader, base_commit, head_commit)?;

        log::debug!(
            "For commits {:?} -> {:?} found lca {:?}",
            base_commit,
            head_commit,
            lca
        );

        log::debug!("Reading history from lca to head");
        reader.history_from_base_to_head(&lca.id, &head_commit.id)
    }

    pub fn list_conflicts_between_commits(
        &self,
        commit_reader: &CommitReader,
        base_commit: &Commit,
        merge_commit: &Commit,
    ) -> Result<Vec<PathBuf>, OxenError> {
        let lca =
            self.lowest_common_ancestor_from_commits(commit_reader, base_commit, merge_commit)?;
        let merge_commits = MergeCommits {
            lca,
            base: base_commit.clone(),
            merge: merge_commit.clone(),
        };
        let write_to_disk = false;
        let conflicts = self.find_merge_conflicts(&merge_commits, write_to_disk)?;
        Ok(conflicts
            .iter()
            .map(|c| c.base_entry.path.to_owned())
            .collect())
    }

    /// Merge into the current branch, returns the merge commit if successful, and None if there is conflicts
    pub fn merge(&self, branch_name: impl AsRef<str>) -> Result<Option<Commit>, OxenError> {
        let branch_name = branch_name.as_ref();
        let commit_reader = CommitReader::new(&self.repository)?;

        let merge_branch = api::local::branches::get_by_name(&self.repository, branch_name)?
            .ok_or(OxenError::local_branch_not_found(branch_name))?;

        let base_commit = commit_reader.head_commit()?;
        let merge_commit = Commit::from_branch(&commit_reader, &merge_branch)?;

        let lca =
            self.lowest_common_ancestor_from_commits(&commit_reader, &base_commit, &merge_commit)?;
        let merge_commits = MergeCommits {
            lca,
            base: base_commit,
            merge: merge_commit,
        };

        self.merge_commits(&merge_commits)
    }

    /// Merge a branch into a base branch, returns the merge commit if successful, and None if there is conflicts
    pub fn merge_into_base(
        &self,
        merge_branch: &Branch,
        base_branch: &Branch,
    ) -> Result<Option<Commit>, OxenError> {
        println!(
            "merge_into_base merge {} into {}",
            merge_branch, base_branch
        );

        if merge_branch.commit_id == base_branch.commit_id {
            // If the merge branch is the same as the base branch, there is nothing to merge
            return Ok(None);
        }

        let commit_reader = CommitReader::new(&self.repository)?;
        let base_commit = Commit::from_branch(&commit_reader, base_branch)?;
        let merge_commit = Commit::from_branch(&commit_reader, merge_branch)?;

        let lca =
            self.lowest_common_ancestor_from_commits(&commit_reader, &base_commit, &merge_commit)?;
        let merge_commits = MergeCommits {
            lca,
            base: base_commit,
            merge: merge_commit,
        };

        self.merge_commits(&merge_commits)
    }

    pub fn merge_commit_into_base(
        &self,
        merge_commit: &Commit,
        base_commit: &Commit,
    ) -> Result<Option<Commit>, OxenError> {
        let commit_reader = CommitReader::new(&self.repository)?;

        let lca =
            self.lowest_common_ancestor_from_commits(&commit_reader, base_commit, merge_commit)?;
        log::debug!(
            "merge_commit_into_base has lca {:?} for merge commit {:?} and base {:?}",
            lca,
            merge_commit,
            base_commit
        );
        let merge_commits = MergeCommits {
            lca,
            base: base_commit.to_owned(),
            merge: merge_commit.to_owned(),
        };

        self.merge_commits(&merge_commits)
    }

    fn merge_commits(&self, merge_commits: &MergeCommits) -> Result<Option<Commit>, OxenError> {
        // User output
        println!(
            "Updating {} -> {}",
            merge_commits.base.id, merge_commits.merge.id
        );

        log::debug!(
            "FOUND MERGE COMMITS:\nLCA: {} -> {}\nBASE: {} -> {}\nMerge: {} -> {}",
            merge_commits.lca.id,
            merge_commits.lca.message,
            merge_commits.base.id,
            merge_commits.base.message,
            merge_commits.merge.id,
            merge_commits.merge.message,
        );

        // Check which type of merge we need to do
        if merge_commits.is_fast_forward_merge() {
            // User output
            println!("Fast-forward");
            let commit = self.fast_forward_merge(&merge_commits.base, &merge_commits.merge)?;
            Ok(Some(commit))
        } else {
            log::debug!(
                "Three way merge! {} -> {}",
                merge_commits.base.id,
                merge_commits.merge.id
            );

            let write_to_disk = true;
            let conflicts = self.find_merge_conflicts(merge_commits, write_to_disk)?;
            log::debug!("Got {} conflicts", conflicts.len());

            if conflicts.is_empty() {
                let commit = self.create_merge_commit(merge_commits)?;
                Ok(Some(commit))
            } else {
                merge_conflict_writer::write_conflicts_to_disk(
                    &self.repository,
                    &self.merge_db,
                    &merge_commits.merge,
                    &merge_commits.base,
                    &conflicts,
                )?;
                Ok(None)
            }
        }
    }

    pub fn has_file(&self, path: &Path) -> Result<bool, OxenError> {
        MergeConflictDBReader::has_file(&self.merge_db, path)
    }

    pub fn remove_conflict_path(&self, path: &Path) -> Result<(), OxenError> {
        let path_str = path.to_str().unwrap();
        let key = path_str.as_bytes();
        self.merge_db.delete(key)?;
        Ok(())
    }

    fn create_merge_commit(&self, merge_commits: &MergeCommits) -> Result<Commit, OxenError> {
        let repo = &self.repository;

        // Stage changes
        let stager = Stager::new(repo)?;
        let commit = api::local::commits::head_commit(repo)?;
        let reader = CommitEntryReader::new(repo, &commit)?;
        let schema_reader = SchemaReader::new(repo, &commit.id)?;
        let ignore = oxenignore::create(repo);
        stager.add(&repo.path, &reader, &schema_reader, &ignore)?;

        let commit_msg = format!(
            "Merge commit {} into {}",
            merge_commits.merge.id, merge_commits.base.id
        );

        log::debug!("create_merge_commit {}", commit_msg);

        // Create a commit with both parents
        let reader = CommitEntryReader::new_from_head(repo)?;
        let status = stager.status(&reader)?;
        let commit_writer = CommitWriter::new(repo)?;
        let parent_ids: Vec<String> = vec![
            merge_commits.base.id.to_owned(),
            merge_commits.merge.id.to_owned(),
        ];
        let commit = commit_writer.commit_with_parent_ids(&status, parent_ids, &commit_msg)?;
        stager.unstage()?;

        Ok(commit)
    }

    // This will try to find the least common ancestor, and if the least common ancestor is HEAD, then we just
    // fast forward, otherwise we need to three way merge
    pub fn find_merge_commits<S: AsRef<str>>(
        &self,
        branch_name: S,
    ) -> Result<MergeCommits, OxenError> {
        let branch_name = branch_name.as_ref();
        let ref_reader = RefReader::new(&self.repository)?;
        let head_commit_id = ref_reader
            .head_commit_id()?
            .ok_or_else(OxenError::head_not_found)?;
        let merge_commit_id = ref_reader
            .get_commit_id_for_branch(branch_name)?
            .ok_or_else(|| OxenError::commit_db_corrupted(branch_name))?;

        let commit_reader = CommitReader::new(&self.repository)?;
        let base = commit_reader
            .get_commit_by_id(&head_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&head_commit_id))?;
        let merge = commit_reader
            .get_commit_by_id(&merge_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&merge_commit_id))?;

        let lca = self.lowest_common_ancestor_from_commits(&commit_reader, &base, &merge)?;

        Ok(MergeCommits { lca, base, merge })
    }

    /// It is a fast forward merge if we cannot traverse cleanly back from merge to HEAD
    fn fast_forward_merge(
        &self,
        base_commit: &Commit,
        merge_commit: &Commit,
    ) -> Result<Commit, OxenError> {
        log::debug!("FF merge!");
        let base_commit_entry_reader = CommitEntryReader::new(&self.repository, base_commit)?;
        let merge_commit_entry_reader = CommitEntryReader::new(&self.repository, merge_commit)?;

        let base_entries = base_commit_entry_reader.list_entries_set()?;
        let merge_entries = merge_commit_entry_reader.list_entries_set()?;

        // Can just copy over all new versions since it is fast forward
        for merge_entry in merge_entries.iter() {
            log::debug!("Merge entry: {:?}", merge_entry.path);
            // Only copy over if hash is different or it doesn't exist for performance
            if let Some(base_entry) = base_entries.get(merge_entry) {
                if base_entry.hash != merge_entry.hash {
                    log::debug!("Merge entry has changed, restore: {:?}", merge_entry.path);
                    self.update_entry(merge_entry)?;
                }
            } else {
                log::debug!("Merge entry is new, restore: {:?}", merge_entry.path);
                self.update_entry(merge_entry)?;
            }
        }

        // Remove all entries that are in HEAD but not in merge entries
        for base_entry in base_entries.iter() {
            log::debug!("Base entry: {:?}", base_entry.path);
            if !merge_entries.contains(base_entry) {
                log::debug!("Removing Base Entry: {:?}", base_entry.path);

                let path = self.repository.path.join(&base_entry.path);
                if path.exists() {
                    util::fs::remove_file(path)?;
                }
            }
        }

        // Move the HEAD forward to this commit
        let ref_writer = RefWriter::new(&self.repository)?;
        ref_writer.set_head_commit_id(&merge_commit.id)?;

        Ok(merge_commit.clone())
    }

    /// Check if HEAD is in the direct parent chain of the merge commit. If it is a direct parent, we can just fast forward
    pub fn lowest_common_ancestor<S: AsRef<str>>(
        &self,
        branch_name: S,
    ) -> Result<Commit, OxenError> {
        let branch_name = branch_name.as_ref();
        let ref_reader = RefReader::new(&self.repository)?;
        let base_commit_id = ref_reader
            .head_commit_id()?
            .ok_or_else(OxenError::head_not_found)?;
        let merge_commit_id = ref_reader
            .get_commit_id_for_branch(branch_name)?
            .ok_or_else(|| OxenError::commit_db_corrupted(branch_name))?;

        let commit_reader = CommitReader::new(&self.repository)?;
        let base_commit = commit_reader
            .get_commit_by_id(&base_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&base_commit_id))?;
        let merge_commit = commit_reader
            .get_commit_by_id(&merge_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&merge_commit_id))?;

        self.lowest_common_ancestor_from_commits(&commit_reader, &base_commit, &merge_commit)
    }

    pub fn lowest_common_ancestor_from_commits(
        &self,
        commit_reader: &CommitReader,
        base_commit: &Commit,
        merge_commit: &Commit,
    ) -> Result<Commit, OxenError> {
        // Traverse the base commit back to start, keeping map of Commit -> Depth(int)
        let commit_depths_from_head = commit_reader.history_with_depth_from_commit(base_commit)?;

        // Traverse the merge commit back
        //   check at each step if ID is in the HEAD commit history
        //   The lowest Depth Commit in HEAD should be the LCA
        let commit_depths_from_merge =
            commit_reader.history_with_depth_from_commit(merge_commit)?;

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
        &self,
        merge_commits: &MergeCommits,
        write_to_disk: bool,
    ) -> Result<Vec<MergeConflict>, OxenError> {
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
        let mut conflicts: Vec<MergeConflict> = vec![];

        // Read all the entries from each commit into sets we can compare to one another
        let lca_entry_reader = CommitEntryReader::new(&self.repository, &merge_commits.lca)?;
        let base_entry_reader = CommitEntryReader::new(&self.repository, &merge_commits.base)?;
        let merge_entry_reader = CommitEntryReader::new(&self.repository, &merge_commits.merge)?;

        let lca_entries = lca_entry_reader.list_entries_set()?;
        let base_entries = base_entry_reader.list_entries_set()?;
        let merge_entries = merge_entry_reader.list_entries_set()?;

        log::debug!("lca_entries.len() {}", lca_entries.len());
        log::debug!("base_entries.len() {}", base_entries.len());
        log::debug!("merge_entries.len() {}", merge_entries.len());

        // Check all the entries in the candidate merge
        for merge_entry in merge_entries.iter() {
            // log::debug!("Considering entry {}", merge_entries.len());

            // Check if the entry exists in all 3 commits
            if let Some(base_entry) = base_entries.get(merge_entry) {
                if let Some(lca_entry) = lca_entries.get(merge_entry) {
                    // If Base and LCA are the same but Merge is different, take merge
                    // log::debug!(
                    //     "Comparing hashes merge_entry {:?} BASE {} LCA {} MERGE {}",
                    //     merge_entry.path,
                    //     base_entry.hash,
                    //     lca_entry.hash,
                    //     merge_entry.hash
                    // );
                    if base_entry.hash == lca_entry.hash && write_to_disk {
                        self.update_entry(merge_entry)?;
                    }

                    // If all three are different, mark as conflict
                    if base_entry.hash != lca_entry.hash
                        && lca_entry.hash != merge_entry.hash
                        && base_entry.hash != merge_entry.hash
                    {
                        conflicts.push(MergeConflict {
                            lca_entry: lca_entry.to_owned(),
                            base_entry: base_entry.to_owned(),
                            merge_entry: merge_entry.to_owned(),
                        });
                    }
                } else {
                    // merge entry doesn't exist in LCA, so just check if it's different from base
                    if base_entry.hash != merge_entry.hash {
                        conflicts.push(MergeConflict {
                            lca_entry: base_entry.to_owned(),
                            base_entry: base_entry.to_owned(),
                            merge_entry: merge_entry.to_owned(),
                        });
                    }
                }
            } else if write_to_disk {
                // merge entry does not exist in base, so create it
                self.update_entry(merge_entry)?;
            }
        }
        log::debug!("three_way_merge conflicts.len() {}", conflicts.len());

        Ok(conflicts)
    }

    fn update_entry(&self, merge_entry: &CommitEntry) -> Result<(), OxenError> {
        restore::restore_file(
            &self.repository,
            &merge_entry.path,
            &merge_entry.commit_id,
            merge_entry,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::core::index::{CommitReader, MergeConflictReader, Merger};
    use crate::error::OxenError;
    use crate::model::{Commit, LocalRepository};
    use crate::test;
    use crate::util;

    async fn populate_threeway_merge_repo(
        repo: &LocalRepository,
        merge_branch_name: &str,
    ) -> Result<Commit, OxenError> {
        // Need to have main branch get ahead of branch so that you can traverse to directory to it, but they
        // have a common ancestor
        // Ex) We want to merge E into D to create F
        // A - C - D - F
        //    \      /
        //     B - E

        let a_branch = api::local::branches::current_branch(repo)?.unwrap();
        let a_path = repo.path.join("a.txt");
        util::fs::write_to_path(&a_path, "a")?;
        command::add(repo, a_path)?;
        // Return the lowest common ancestor for the tests
        let lca = command::commit(repo, "Committing a.txt file")?;

        // Make changes on B
        api::local::branches::create_checkout(repo, merge_branch_name)?;
        let b_path = repo.path.join("b.txt");
        util::fs::write_to_path(&b_path, "b")?;
        command::add(repo, b_path)?;
        command::commit(repo, "Committing b.txt file")?;

        // Checkout A again to make another change
        command::checkout(repo, &a_branch.name).await?;
        let c_path = repo.path.join("c.txt");
        util::fs::write_to_path(&c_path, "c")?;
        command::add(repo, c_path)?;
        command::commit(repo, "Committing c.txt file")?;

        let d_path = repo.path.join("d.txt");
        util::fs::write_to_path(&d_path, "d")?;
        command::add(repo, d_path)?;
        command::commit(repo, "Committing d.txt file")?;

        // Checkout merge branch (B) to make another change
        command::checkout(repo, merge_branch_name).await?;
        let e_path = repo.path.join("e.txt");
        util::fs::write_to_path(&e_path, "e")?;
        command::add(repo, e_path)?;
        command::commit(repo, "Committing e.txt file")?;

        // Checkout the OG branch again so that we can merge into it
        command::checkout(repo, &a_branch.name).await?;

        Ok(lca)
    }

    #[tokio::test]
    async fn test_merge_one_commit_add_fast_forward() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write and commit hello file to main branch
            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            command::add(&repo, hello_file)?;
            command::commit(&repo, "Adding hello file")?;

            // Branch to add world
            let branch_name = "add-world";
            api::local::branches::create_checkout(&repo, branch_name)?;

            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;
            command::add(&repo, &world_file)?;
            command::commit(&repo, "Adding world file")?;
            // Fetch the branch again to get the latest commit
            let merge_branch = api::local::branches::current_branch(&repo)?.unwrap();

            // Checkout and merge additions
            let og_branch = command::checkout(&repo, &og_branch.name).await?.unwrap();

            // Make sure world file doesn't exist until we merge it in
            assert!(!world_file.exists());

            // Merge it
            let merger = Merger::new(&repo)?;
            let commit = merger.merge_into_base(&merge_branch, &og_branch)?.unwrap();

            // Now that we've merged in, world file should exist
            assert!(world_file.exists());

            // Check that HEAD has updated to the merge commit
            let head_commit = api::local::commits::head_commit(&repo)?;
            assert_eq!(head_commit.id, commit.id);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_one_commit_remove_fast_forward() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write and add hello file
            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            command::add(&repo, hello_file)?;

            // Write and add world file
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;
            command::add(&repo, &world_file)?;

            // Commit two files
            command::commit(&repo, "Adding hello & world files")?;

            // Branch to remove world
            let branch_name = "remove-world";
            let merge_branch = api::local::branches::create_checkout(&repo, branch_name)?;

            // Remove the file
            let world_file = repo.path.join("world.txt");
            util::fs::remove_file(&world_file)?;

            // Commit the removal
            command::add(&repo, &world_file)?;
            command::commit(&repo, "Removing world file")?;

            // Checkout and merge additions
            command::checkout(&repo, &og_branch.name).await?;

            // Make sure world file exists until we merge the removal in
            assert!(world_file.exists());

            let merger = Merger::new(&repo)?;
            merger.merge(&merge_branch.name)?.unwrap();

            // Now that we've merged in, world file should not exist
            assert!(!world_file.exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_one_commit_modified_fast_forward() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write and add hello file
            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            command::add(&repo, hello_file)?;

            // Write and add world file
            let world_file = repo.path.join("world.txt");
            let og_contents = "World";
            util::fs::write_to_path(&world_file, og_contents)?;
            command::add(&repo, &world_file)?;

            // Commit two files
            command::commit(&repo, "Adding hello & world files")?;

            // Branch to remove world
            let branch_name = "modify-world";
            api::local::branches::create_checkout(&repo, branch_name)?;

            // Modify the file
            let new_contents = "Around the world";
            let world_file = test::modify_txt_file(world_file, new_contents)?;

            // Commit the removal
            command::add(&repo, &world_file)?;
            command::commit(&repo, "Modifying world file")?;

            // Checkout and merge additions
            command::checkout(&repo, &og_branch.name).await?;

            // Make sure world file exists in it's original form
            let contents = util::fs::read_from_path(&world_file)?;
            assert_eq!(contents, og_contents);

            let merger = Merger::new(&repo)?;
            merger.merge(branch_name)?.unwrap();

            // Now that we've merged in, world file should be new content
            let contents = util::fs::read_from_path(&world_file)?;
            assert_eq!(contents, new_contents);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_is_three_way_merge() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let merge_branch_name = "B"; // see populate function
            populate_threeway_merge_repo(&repo, merge_branch_name).await?;

            // Make sure the merger can detect the three way merge
            let merger = Merger::new(&repo)?;
            let merge_commits = merger.find_merge_commits(merge_branch_name)?;
            let is_fast_forward = merge_commits.is_fast_forward_merge();
            assert!(!is_fast_forward);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_get_lowest_common_ancestor() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let merge_branch_name = "B"; // see populate function
            let lca = populate_threeway_merge_repo(&repo, merge_branch_name).await?;

            // Make sure the merger can detect the three way merge
            let merger = Merger::new(&repo)?;
            let guess = merger.lowest_common_ancestor(merge_branch_name)?;
            assert_eq!(lca.id, guess.id);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_no_conflict_three_way_merge() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let merge_branch_name = "B";
            // this will checkout main again so we can try to merge
            populate_threeway_merge_repo(&repo, merge_branch_name).await?;

            {
                // Make sure the merger can detect the three way merge
                let merger = Merger::new(&repo)?;
                let merge_commit = merger.merge(merge_branch_name)?.unwrap();

                // Two way merge should have two parent IDs so we know where the merge came from
                assert_eq!(merge_commit.parent_ids.len(), 2);

                // There should be 5 files: [a.txt, b.txt, c.txt, d.txt e.txt]
                let file_prefixes = ["a", "b", "c", "d", "e"];
                for prefix in file_prefixes.iter() {
                    let filename = format!("{prefix}.txt");
                    let filepath = repo.path.join(filename);
                    println!(
                        "test_merge_no_conflict_three_way_merge checking file exists {filepath:?}"
                    );
                    assert!(filepath.exists());
                }
            }

            // Make sure we added the merge commit
            let commit_reader = CommitReader::new(&repo)?;
            let post_merge_history = commit_reader.history_from_head()?;

            // We should have the merge commit + the branch commits here
            assert_eq!(7, post_merge_history.len());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_conflict_three_way_merge() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // This test has a conflict where user on the main line, and user on the branch, both modify a.txt

            // Ex) We want to merge E into D to create F
            // A - C - D - F
            //    \      /
            //     B - E

            let a_branch = api::local::branches::current_branch(&repo)?.unwrap();
            let a_path = repo.path.join("a.txt");
            util::fs::write_to_path(&a_path, "a")?;
            command::add(&repo, &a_path)?;
            // Return the lowest common ancestor for the tests
            command::commit(&repo, "Committing a.txt file")?;

            // Make changes on B
            let merge_branch_name = "B";
            api::local::branches::create_checkout(&repo, merge_branch_name)?;

            // Add a text new text file
            let b_path = repo.path.join("b.txt");
            util::fs::write_to_path(&b_path, "b")?;
            command::add(&repo, &b_path)?;

            // Modify the text file a.txt
            test::modify_txt_file(&a_path, "a modified from branch")?;
            command::add(&repo, &a_path)?;

            // Commit changes
            command::commit(&repo, "Committing b.txt file")?;

            // Checkout main branch again to make another change
            command::checkout(&repo, &a_branch.name).await?;

            // Add new file c.txt on main branch
            let c_path = repo.path.join("c.txt");
            util::fs::write_to_path(&c_path, "c")?;
            command::add(&repo, &c_path)?;

            // Modify a.txt from main branch
            test::modify_txt_file(&a_path, "a modified from main line")?;
            command::add(&repo, &a_path)?;

            // Commit changes to main branch
            command::commit(&repo, "Committing c.txt file")?;

            // Commit some more changes to main branch
            let d_path = repo.path.join("d.txt");
            util::fs::write_to_path(&d_path, "d")?;
            command::add(&repo, &d_path)?;
            command::commit(&repo, "Committing d.txt file")?;

            // Checkout merge branch (B) to make another change
            command::checkout(&repo, merge_branch_name).await?;

            // Add another branch
            let e_path = repo.path.join("e.txt");
            util::fs::write_to_path(&e_path, "e")?;
            command::add(&repo, &e_path)?;
            command::commit(&repo, "Committing e.txt file")?;

            // Checkout the OG branch again so that we can merge into it
            command::checkout(&repo, &a_branch.name).await?;

            // Make sure the merger can detect the three way merge
            {
                let merger = Merger::new(&repo)?;
                merger.merge(merge_branch_name)?;
            }

            let conflict_reader = MergeConflictReader::new(&repo)?;
            let has_conflicts = conflict_reader.has_conflicts()?;
            let conflicts = conflict_reader.list_conflicts()?;

            assert!(has_conflicts);
            assert_eq!(conflicts.len(), 1);

            let local_a_path = util::fs::path_relative_to_dir(&a_path, &repo.path)?;
            assert_eq!(conflicts[0].base_entry.path, local_a_path);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_conflict_three_way_merge_post_merge_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // This case for a three way merge was failing, if one branch gets fast forwarded, then the next
            // should have a conflict from the LCA

            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "cat\ndog")?;
            command::add(&repo, &labels_path)?;
            // Return the lowest common ancestor for the tests
            command::commit(&repo, "Add initial labels.txt file with cat and dog")?;

            // Add a fish label to the file on a branch
            let fish_branch_name = "add-fish-label";
            api::local::branches::create_checkout(&repo, fish_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nfish")?;
            command::add(&repo, &labels_path)?;
            command::commit(&repo, "Adding fish to labels.txt file")?;

            // Checkout main, and branch from it to another branch to add a human label
            command::checkout(&repo, &og_branch.name).await?;
            let human_branch_name = "add-human-label";
            api::local::branches::create_checkout(&repo, human_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nhuman")?;
            command::add(&repo, labels_path)?;
            command::commit(&repo, "Adding human to labels.txt file")?;

            // Checkout main again
            command::checkout(&repo, &og_branch.name).await?;

            // Merge in a scope so that it closes the db
            {
                let merger = Merger::new(&repo)?;
                merger.merge(fish_branch_name)?;
            }

            // Checkout main again, merge again
            command::checkout(&repo, &og_branch.name).await?;
            {
                let merger = Merger::new(&repo)?;
                merger.merge(human_branch_name)?;
            }

            let conflict_reader = MergeConflictReader::new(&repo)?;
            let has_conflicts = conflict_reader.has_conflicts()?;
            let conflicts = conflict_reader.list_conflicts()?;

            assert!(has_conflicts);
            assert_eq!(conflicts.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merger_has_merge_conflicts_without_merging() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // This case for a three way merge was failing, if one branch gets fast forwarded, then the next
            // should have a conflict from the LCA

            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "cat\ndog")?;
            command::add(&repo, &labels_path)?;
            // Return the lowest common ancestor for the tests
            command::commit(&repo, "Add initial labels.txt file with cat and dog")?;

            // Add a fish label to the file on a branch
            let fish_branch_name = "add-fish-label";
            api::local::branches::create_checkout(&repo, fish_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nfish")?;
            command::add(&repo, &labels_path)?;
            command::commit(&repo, "Adding fish to labels.txt file")?;

            // Checkout main, and branch from it to another branch to add a human label
            command::checkout(&repo, &og_branch.name).await?;
            let human_branch_name = "add-human-label";
            api::local::branches::create_checkout(&repo, human_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nhuman")?;
            command::add(&repo, labels_path)?;
            command::commit(&repo, "Adding human to labels.txt file")?;

            // Checkout main again
            command::checkout(&repo, &og_branch.name).await?;

            // Merge the fish branch in, and then the human branch should have conflicts
            let merger = Merger::new(&repo)?;
            // Should merge cleanly
            let result = merger.merge(fish_branch_name)?;
            assert!(result.is_some());

            // But now there should be conflicts when trying to merge in the human branch
            let base_branch = api::local::branches::get_by_name(&repo, &og_branch.name)?.unwrap();
            let merge_branch =
                api::local::branches::get_by_name(&repo, human_branch_name)?.unwrap();

            // Check if there are conflicts
            let has_conflicts = merger.has_conflicts(&base_branch, &merge_branch)?;
            assert!(has_conflicts);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_merge_conflicts_without_merging() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // This case for a three way merge was failing, if one branch gets fast forwarded, then the next
            // should have a conflict from the LCA

            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "cat\ndog")?;
            command::add(&repo, &labels_path)?;
            // Return the lowest common ancestor for the tests
            command::commit(&repo, "Add initial labels.txt file with cat and dog")?;

            // Add a fish label to the file on a branch
            let fish_branch_name = "add-fish-label";
            api::local::branches::create_checkout(&repo, fish_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nfish")?;
            command::add(&repo, &labels_path)?;
            command::commit(&repo, "Adding fish to labels.txt file")?;

            // Checkout main, and branch from it to another branch to add a human label
            command::checkout(&repo, &og_branch.name).await?;
            let human_branch_name = "add-human-label";
            api::local::branches::create_checkout(&repo, human_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nhuman")?;
            command::add(&repo, labels_path)?;
            let human_commit = command::commit(&repo, "Adding human to labels.txt file")?;

            // Checkout main again
            command::checkout(&repo, &og_branch.name).await?;

            // Merge the fish branch in, and then the human branch should have conflicts
            let merger = Merger::new(&repo)?;
            // Should merge cleanly
            let result_commit = merger.merge(fish_branch_name)?;
            assert!(result_commit.is_some());

            // There should be one file that is in conflict
            let commit_reader = CommitReader::new(&repo)?;
            let base_commit = result_commit.unwrap();
            let conflicts = merger.list_conflicts_between_commits(
                &commit_reader,
                &base_commit,
                &human_commit,
            )?;
            assert_eq!(conflicts.len(), 1);

            Ok(())
        })
        .await
    }
}
