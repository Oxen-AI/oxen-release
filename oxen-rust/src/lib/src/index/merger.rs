use crate::command;
use crate::constants::{MERGE_DIR, MERGE_HEAD_FILE, ORIG_HEAD_FILE};
use crate::db;
use crate::error::OxenError;
use crate::index::{
    CommitDirReader, CommitReader, CommitWriter, MergeConflictDBReader, RefReader, RefWriter,
    Stager,
};
use crate::model::{Commit, CommitEntry, LocalRepository, MergeConflict};

use crate::util;

use rocksdb::DB;
use std::path::Path;
use std::str;

use super::restore;

// This is a struct to find the commits we want to merge
struct MergeCommits {
    lca: Commit,
    head: Commit,
    merge: Commit,
}

impl MergeCommits {
    pub fn is_fast_forward_merge(&self) -> bool {
        self.lca.id == self.head.id
    }
}

pub struct Merger {
    repository: LocalRepository,
    merge_db: DB,
}

impl Merger {
    pub fn new(repo: &LocalRepository) -> Result<Merger, OxenError> {
        let db_path = util::fs::oxen_hidden_dir(&repo.path).join(Path::new(MERGE_DIR));
        log::debug!("Merger::new() DB {:?}", db_path);
        let opts = db::opts::default();
        Ok(Merger {
            repository: repo.to_owned(),
            merge_db: DB::open(&opts, &db_path)?,
        })
    }

    /// Merge a branch name into the current checked out branch, returns the HEAD commit if successful,
    /// and None if there were conflicts. Conflicts get written to disk so we can return to them to fix.
    pub fn merge<S: AsRef<str>>(&self, branch_name: S) -> Result<Option<Commit>, OxenError> {
        let branch_name = branch_name.as_ref();
        // This returns HEAD, LCA, and the Merge commits we can work with
        let merge_commits = self.find_merge_commits(branch_name)?;

        // User output
        println!(
            "Updating {} -> {}",
            merge_commits.head.id, merge_commits.merge.id
        );

        log::debug!(
            "FOUND MERGE COMMITS:\nLCA: {} -> {}\nHEAD: {} -> {}\nMerge: {} -> {}",
            merge_commits.lca.id,
            merge_commits.lca.message,
            merge_commits.head.id,
            merge_commits.head.message,
            merge_commits.merge.id,
            merge_commits.merge.message,
        );

        // Check which type of merge we need to do
        if merge_commits.is_fast_forward_merge() {
            // User output
            println!("Fast-forward");
            let commit = self.fast_forward_merge(merge_commits.head, merge_commits.merge)?;
            Ok(Some(commit))
        } else {
            log::debug!("Three way merge! {}", branch_name);

            let conflicts = self.three_way_merge(&merge_commits)?;
            if conflicts.is_empty() {
                let commit = self.create_merge_commit(branch_name, &merge_commits)?;
                Ok(Some(commit))
            } else {
                self.write_conflicts_to_disk(&merge_commits, &conflicts)?;
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

    fn write_conflicts_to_disk(
        &self,
        merge_commits: &MergeCommits,
        conflicts: &[MergeConflict],
    ) -> Result<(), OxenError> {
        // Write two files which are the merge commit and head commit so that we can make these parents later
        let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
        let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
        let orig_head_path = hidden_dir.join(ORIG_HEAD_FILE);
        util::fs::write_to_path(&merge_head_path, &merge_commits.merge.id);
        util::fs::write_to_path(&orig_head_path, &merge_commits.head.id);

        for conflict in conflicts.iter() {
            let key = conflict.head_entry.path.to_str().unwrap();
            let key_bytes = key.as_bytes();
            let val_json = serde_json::to_string(&conflict)?;

            self.merge_db.put(key_bytes, val_json.as_bytes())?;
        }

        Ok(())
    }

    fn create_merge_commit<S: AsRef<str>>(
        &self,
        branch_name: S,
        merge_commits: &MergeCommits,
    ) -> Result<Commit, OxenError> {
        let repo = &self.repository;

        // Stage changes
        let stager = Stager::new(repo)?;
        let commit = command::head_commit(repo)?;
        let reader = CommitDirReader::new(repo, &commit)?;
        stager.add(&repo.path, &reader)?;

        let commit_msg = format!("Merge branch '{}'", branch_name.as_ref());

        log::debug!("create_merge_commit {}", commit_msg);

        // Create a commit with both parents
        let reader = CommitDirReader::new_from_head(repo)?;
        let status = stager.status(&reader)?;
        let commit_writer = CommitWriter::new(repo)?;
        let parent_ids: Vec<String> = vec![
            merge_commits.head.id.to_owned(),
            merge_commits.merge.id.to_owned(),
        ];
        let commit = commit_writer.commit_with_parent_ids(&status, parent_ids, &commit_msg)?;
        stager.unstage()?;

        Ok(commit)
    }

    // This will try to find the least common ancestor, and if the least common ancestor is HEAD, then we just
    // fast forward, otherwise we need to three way merge
    fn find_merge_commits<S: AsRef<str>>(&self, branch_name: S) -> Result<MergeCommits, OxenError> {
        let branch_name = branch_name.as_ref();
        let ref_reader = RefReader::new(&self.repository)?;
        let head_commit_id = ref_reader
            .head_commit_id()?
            .ok_or_else(OxenError::head_not_found)?;
        let merge_commit_id = ref_reader
            .get_commit_id_for_branch(branch_name)?
            .ok_or_else(|| OxenError::commit_db_corrupted(branch_name))?;

        let commit_reader = CommitReader::new(&self.repository)?;
        let head = commit_reader
            .get_commit_by_id(&head_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&head_commit_id))?;
        let merge = commit_reader
            .get_commit_by_id(&merge_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&merge_commit_id))?;

        let lca = self.p_lowest_common_ancestor(&commit_reader, &head, &merge)?;

        Ok(MergeCommits { lca, head, merge })
    }

    /// It is a fast forward merge if we cannot traverse cleanly back from merge to HEAD
    fn fast_forward_merge(
        &self,
        head_commit: Commit,
        merge_commit: Commit,
    ) -> Result<Commit, OxenError> {
        let head_commit_entry_reader = CommitDirReader::new(&self.repository, &head_commit)?;
        let merge_commit_entry_reader = CommitDirReader::new(&self.repository, &merge_commit)?;

        let head_entries = head_commit_entry_reader.list_entries_set()?;
        let merge_entries = merge_commit_entry_reader.list_entries_set()?;

        // Can just copy over all new versions since it is fast forward
        for merge_entry in merge_entries.iter() {
            // Only copy over if hash is different or it doesn't exist for performace
            if let Some(head_entry) = head_entries.get(merge_entry) {
                if head_entry.hash != merge_entry.hash {
                    self.update_entry(merge_entry)?;
                }
            } else {
                self.update_entry(merge_entry)?;
            }
        }

        // Remove all entries that are in HEAD but not in merge entries
        for head_entry in head_entries.iter() {
            if !merge_entries.contains(head_entry) {
                let path = self.repository.path.join(&head_entry.path);
                std::fs::remove_file(path)?;
            }
        }

        // Move the HEAD forward to this commit
        let ref_writer = RefWriter::new(&self.repository)?;
        ref_writer.set_head_commit_id(&merge_commit.id)?;

        Ok(merge_commit)
    }

    /// Check if HEAD is in the direct parent chain of the merge commit. If it is a direct parent, we can just fast forward
    pub fn lowest_common_ancestor<S: AsRef<str>>(
        &self,
        branch_name: S,
    ) -> Result<Commit, OxenError> {
        let branch_name = branch_name.as_ref();
        let ref_reader = RefReader::new(&self.repository)?;
        let head_commit_id = ref_reader
            .head_commit_id()?
            .ok_or_else(OxenError::head_not_found)?;
        let merge_commit_id = ref_reader
            .get_commit_id_for_branch(branch_name)?
            .ok_or_else(|| OxenError::commit_db_corrupted(branch_name))?;

        let commit_reader = CommitReader::new(&self.repository)?;
        let head_commit = commit_reader
            .get_commit_by_id(&head_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&head_commit_id))?;
        let merge_commit = commit_reader
            .get_commit_by_id(&merge_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&merge_commit_id))?;

        self.p_lowest_common_ancestor(&commit_reader, &head_commit, &merge_commit)
    }

    fn p_lowest_common_ancestor(
        &self,
        commit_reader: &CommitReader,
        head_commit: &Commit,
        merge_commit: &Commit,
    ) -> Result<Commit, OxenError> {
        // Traverse the HEAD commit back to start, keeping map of Commit -> Depth(int)
        let commit_depths_from_head = commit_reader.history_with_depth_from_commit(head_commit)?;

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
                    lca = commit.clone();
                }
            }
        }

        Ok(lca)
    }

    /// Will return conflicts if there are any to indicate that the merge was unsuccessful
    fn three_way_merge(
        &self,
        merge_commits: &MergeCommits,
    ) -> Result<Vec<MergeConflict>, OxenError> {
        /*
        https://en.wikipedia.org/wiki/Merge_(version_control)#Three-way_merge

        C = LCA
        A = HEAD
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
        let lca_entry_reader = CommitDirReader::new(&self.repository, &merge_commits.lca)?;
        let head_entry_reader = CommitDirReader::new(&self.repository, &merge_commits.head)?;
        let merge_entry_reader = CommitDirReader::new(&self.repository, &merge_commits.merge)?;

        let lca_entries = lca_entry_reader.list_entries_set()?;
        let head_entries = head_entry_reader.list_entries_set()?;
        let merge_entries = merge_entry_reader.list_entries_set()?;

        log::debug!("lca_entries.len() {}", lca_entries.len());
        log::debug!("head_entries.len() {}", head_entries.len());
        log::debug!("merge_entries.len() {}", merge_entries.len());

        // Check all the entries in the candidate merge
        for merge_entry in merge_entries.iter() {
            // Check if the entry exists in all 3 commits
            if let Some(head_entry) = head_entries.get(merge_entry) {
                if let Some(lca_entry) = lca_entries.get(merge_entry) {
                    // If HEAD and LCA are the same but Merge is different, take merge
                    log::debug!(
                        "Comparing hashes merge_entry {:?} HEAD {} LCA {} MERGE {}",
                        merge_entry.path,
                        head_entry.hash,
                        lca_entry.hash,
                        merge_entry.hash
                    );
                    if head_entry.hash == lca_entry.hash {
                        self.update_entry(merge_entry)?;
                    }

                    // TODO: IS THIS CORRECT? Feels like we need to do something...
                    // If Merge and LCA are the same, but HEAD is different, take HEAD
                    // Since we are already on HEAD, this means do nothing

                    // If all three are different, mark as conflict
                    if head_entry.hash != lca_entry.hash
                        && lca_entry.hash != merge_entry.hash
                        && head_entry.hash != merge_entry.hash
                    {
                        conflicts.push(MergeConflict {
                            lca_entry: lca_entry.to_owned(),
                            head_entry: head_entry.to_owned(),
                            merge_entry: merge_entry.to_owned(),
                        });
                    }
                } // merge entry doesn't exist in LCA, which is fine, we will catch it in HEAD
            } else {
                // merge entry does not exist in HEAD, so create it
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
    use crate::command;
    use crate::error::OxenError;
    use crate::index::{CommitReader, MergeConflictReader, Merger};
    use crate::model::{Commit, LocalRepository};
    use crate::test;
    use crate::util;

    fn populate_threeway_merge_repo(
        repo: &LocalRepository,
        merge_branch_name: &str,
    ) -> Result<Commit, OxenError> {
        // Need to have main branch get ahead of branch so that you can traverse to directory to it, but they
        // have a common ancestor
        // Ex) We want to merge E into D to create F
        // A - C - D - F
        //    \      /
        //     B - E

        let a_branch = command::current_branch(repo)?.unwrap();
        let a_path = repo.path.join("a.txt");
        util::fs::write_to_path(&a_path, "a");
        command::add(repo, a_path)?;
        // Return the lowest common ancestor for the tests
        let lca = command::commit(repo, "Committing a.txt file")?;

        // Make changes on B
        command::create_checkout_branch(repo, merge_branch_name)?;
        let b_path = repo.path.join("b.txt");
        util::fs::write_to_path(&b_path, "b");
        command::add(repo, b_path)?;
        command::commit(repo, "Committing b.txt file")?;

        // Checkout A again to make another change
        command::checkout(repo, &a_branch.name)?;
        let c_path = repo.path.join("c.txt");
        util::fs::write_to_path(&c_path, "c");
        command::add(repo, c_path)?;
        command::commit(repo, "Committing c.txt file")?;

        let d_path = repo.path.join("d.txt");
        util::fs::write_to_path(&d_path, "d");
        command::add(repo, d_path)?;
        command::commit(repo, "Committing d.txt file")?;

        // Checkout merge branch (B) to make another change
        command::checkout(repo, merge_branch_name)?;
        let e_path = repo.path.join("e.txt");
        util::fs::write_to_path(&e_path, "e");
        command::add(repo, e_path)?;
        command::commit(repo, "Committing e.txt file")?;

        // Checkout the OG branch again so that we can merge into it
        command::checkout(repo, &a_branch.name)?;

        Ok(lca.unwrap())
    }

    #[test]
    fn test_merge_one_commit_add_fast_forward() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write and commit hello file to main branch
            let og_branch = command::current_branch(&repo)?.unwrap();
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello");
            command::add(&repo, hello_file)?;
            command::commit(&repo, "Adding hello file")?;

            // Branch to add world
            let branch_name = "add-world";
            command::create_checkout_branch(&repo, branch_name)?;

            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World");
            command::add(&repo, &world_file)?;
            command::commit(&repo, "Adding world file")?;

            // Checkout and merge additions
            command::checkout(&repo, og_branch.name)?;

            // Make sure world file doesn't exist until we merge it in
            assert!(!world_file.exists());

            // Merge it
            let merger = Merger::new(&repo)?;
            let commit = merger.merge(branch_name)?.unwrap();

            // Now that we've merged in, world file should exist
            assert!(world_file.exists());

            // Check that HEAD has updated to the merge commit
            let head_commit = command::head_commit(&repo)?;
            assert_eq!(head_commit.id, commit.id);

            Ok(())
        })
    }

    #[test]
    fn test_merge_one_commit_remove_fast_forward() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write and add hello file
            let og_branch = command::current_branch(&repo)?.unwrap();
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello");
            command::add(&repo, hello_file)?;

            // Write and add world file
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World");
            command::add(&repo, &world_file)?;

            // Commit two files
            command::commit(&repo, "Adding hello & world files")?;

            // Branch to remove world
            let branch_name = "remove-world";
            command::create_checkout_branch(&repo, branch_name)?;

            // Remove the file
            let world_file = repo.path.join("world.txt");
            std::fs::remove_file(&world_file)?;

            // Commit the removal
            command::add(&repo, &world_file)?;
            command::commit(&repo, "Removing world file")?;

            // Checkout and merge additions
            command::checkout(&repo, og_branch.name)?;

            // Make sure world file exists until we merge the removal in
            assert!(world_file.exists());

            let merger = Merger::new(&repo)?;
            merger.merge(branch_name)?;

            // Now that we've merged in, world file should not exist
            assert!(!world_file.exists());

            Ok(())
        })
    }

    #[test]
    fn test_merge_one_commit_modified_fast_forward() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write and add hello file
            let og_branch = command::current_branch(&repo)?.unwrap();
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello");
            command::add(&repo, hello_file)?;

            // Write and add world file
            let world_file = repo.path.join("world.txt");
            let og_contents = "World";
            util::fs::write_to_path(&world_file, og_contents);
            command::add(&repo, &world_file)?;

            // Commit two files
            command::commit(&repo, "Adding hello & world files")?;

            // Branch to remove world
            let branch_name = "modify-world";
            command::create_checkout_branch(&repo, branch_name)?;

            // Modify the file
            let new_contents = "Around the world";
            let world_file = test::modify_txt_file(world_file, new_contents)?;

            // Commit the removal
            command::add(&repo, &world_file)?;
            command::commit(&repo, "Modifying world file")?;

            // Checkout and merge additions
            command::checkout(&repo, og_branch.name)?;

            // Make sure world file exists in it's original form
            let contents = util::fs::read_from_path(&world_file)?;
            assert_eq!(contents, og_contents);

            let merger = Merger::new(&repo)?;
            merger.merge(branch_name)?;

            // Now that we've merged in, world file should be new content
            let contents = util::fs::read_from_path(&world_file)?;
            assert_eq!(contents, new_contents);

            Ok(())
        })
    }

    #[test]
    fn test_merge_is_three_way_merge() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let merge_branch_name = "B"; // see populate function
            populate_threeway_merge_repo(&repo, merge_branch_name)?;

            // Make sure the merger can detect the three way merge
            let merger = Merger::new(&repo)?;
            let merge_commits = merger.find_merge_commits(merge_branch_name)?;
            let is_fast_forward = merge_commits.is_fast_forward_merge();
            assert!(!is_fast_forward);

            Ok(())
        })
    }

    #[test]
    fn test_merge_get_lowest_common_ancestor() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let merge_branch_name = "B"; // see populate function
            let lca = populate_threeway_merge_repo(&repo, merge_branch_name)?;

            // Make sure the merger can detect the three way merge
            let merger = Merger::new(&repo)?;
            let guess = merger.lowest_common_ancestor(merge_branch_name)?;
            assert_eq!(lca.id, guess.id);

            Ok(())
        })
    }

    #[test]
    fn test_merge_no_conflict_three_way_merge() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let merge_branch_name = "B";
            // this will checkout main again so we can try to merge
            populate_threeway_merge_repo(&repo, merge_branch_name)?;

            {
                // Make sure the merger can detect the three way merge
                let merger = Merger::new(&repo)?;
                let merge_commit = merger.merge(merge_branch_name)?.unwrap();

                // Two way merge should have two parent IDs so we know where the merge came from
                assert_eq!(merge_commit.parent_ids.len(), 2);

                // There should be 5 files: [a.txt, b.txt, c.txt, d.txt e.txt]
                let file_prefixes = vec!["a", "b", "c", "d", "e"];
                for prefix in file_prefixes.iter() {
                    let filename = format!("{}.txt", prefix);
                    let filepath = repo.path.join(filename);
                    println!(
                        "test_merge_no_conflict_three_way_merge checking file exists {:?}",
                        filepath
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
    }

    #[test]
    fn test_merge_conflict_three_way_merge() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // This test has a conflict where user on the main line, and user on the branch, both modify a.txt

            // Ex) We want to merge E into D to create F
            // A - C - D - F
            //    \      /
            //     B - E

            let a_branch = command::current_branch(&repo)?.unwrap();
            let a_path = repo.path.join("a.txt");
            util::fs::write_to_path(&a_path, "a");
            command::add(&repo, &a_path)?;
            // Return the lowest common ancestor for the tests
            command::commit(&repo, "Committing a.txt file")?;

            // Make changes on B
            let merge_branch_name = "B";
            command::create_checkout_branch(&repo, merge_branch_name)?;

            // Add a text new text file
            let b_path = repo.path.join("b.txt");
            util::fs::write_to_path(&b_path, "b");
            command::add(&repo, &b_path)?;

            // Modify the text file a.txt
            test::modify_txt_file(&a_path, "a modified from branch")?;
            command::add(&repo, &a_path)?;

            // Commit changes
            command::commit(&repo, "Committing b.txt file")?;

            // Checkout main branch again to make another change
            command::checkout(&repo, &a_branch.name)?;

            // Add new file c.txt on main branch
            let c_path = repo.path.join("c.txt");
            util::fs::write_to_path(&c_path, "c");
            command::add(&repo, &c_path)?;

            // Modify a.txt from main branch
            test::modify_txt_file(&a_path, "a modified from main line")?;
            command::add(&repo, &a_path)?;

            // Commit changes to main branch
            command::commit(&repo, "Committing c.txt file")?;

            // Commit some more changes to main branch
            let d_path = repo.path.join("d.txt");
            util::fs::write_to_path(&d_path, "d");
            command::add(&repo, &d_path)?;
            command::commit(&repo, "Committing d.txt file")?;

            // Checkout merge branch (B) to make another change
            command::checkout(&repo, merge_branch_name)?;

            // Add another branch
            let e_path = repo.path.join("e.txt");
            util::fs::write_to_path(&e_path, "e");
            command::add(&repo, &e_path)?;
            command::commit(&repo, "Committing e.txt file")?;

            // Checkout the OG branch again so that we can merge into it
            command::checkout(&repo, &a_branch.name)?;

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
            assert_eq!(conflicts[0].head_entry.path, local_a_path);

            Ok(())
        })
    }

    #[test]
    fn test_merge_conflict_three_way_merge_post_merge_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // This case for a three way merge was failing, if one branch gets fast forwarded, then the next
            // should have a conflict from the LCA

            let og_branch = command::current_branch(&repo)?.unwrap();
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "cat\ndog");
            command::add(&repo, &labels_path)?;
            // Return the lowest common ancestor for the tests
            command::commit(&repo, "Add initial labels.txt file with cat and dog")?;

            // Add a fish label to the file on a branch
            let fish_branch_name = "add-fish-label";
            command::create_checkout_branch(&repo, fish_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nfish")?;
            command::add(&repo, &labels_path)?;
            command::commit(&repo, "Adding fish to labels.txt file")?;

            // Checkout main, and branch from it to another branch to add a human label
            command::checkout(&repo, &og_branch.name)?;
            let human_branch_name = "add-human-label";
            command::create_checkout_branch(&repo, human_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nhuman")?;
            command::add(&repo, labels_path)?;
            command::commit(&repo, "Adding human to labels.txt file")?;

            // Checkout main again
            command::checkout(&repo, &og_branch.name)?;

            // Merge in a scope so that it closes the db
            {
                let merger = Merger::new(&repo)?;
                merger.merge(fish_branch_name)?;
            }

            // Checkout main again, merge again
            command::checkout(&repo, &og_branch.name)?;
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
    }
}
