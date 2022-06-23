use crate::command;
use crate::error::OxenError;
use crate::index::{RefReader, CommitReader, CommitEntryReader};
use crate::model::{Commit, CommitEntry, MergeConflict, LocalRepository};
use crate::util;

// This is a struct to find the commits we want to merge
struct MergeCommits {
    lca: Commit,
    head: Commit,
    merge: Commit
}

impl MergeCommits {
    pub fn is_fast_forward_merge(&self) -> bool {
        self.lca.id == self.head.id
    }
}

pub struct Merger {
    repository: LocalRepository
}

impl Merger {
    pub fn new(repo: &LocalRepository) -> Merger {
        Merger {
            repository: repo.to_owned()
        }
    }

    /// Merge a branch name into the current checked out branch, returns the HEAD commit if successful,
    /// and None if there were conflicts. Conflicts get written to disk so we can return to them to fix.
    pub fn merge<S: AsRef<str>>(&self, branch_name: S) -> Result<Option<Commit>, OxenError> {
        // This returns HEAD, LCA, and the Merge commits we can work with
        let merge_commits = self.find_merge_commits(&branch_name)?;

        log::debug!("FOUND MERGE COMMITS:\nLCA: {} -> {}\nHEAD: {} -> {}\nMerge: {} -> {}",
            merge_commits.lca.id, merge_commits.lca.message,
            merge_commits.head.id, merge_commits.head.message,
            merge_commits.merge.id, merge_commits.merge.message,
        );

        // Check which type of merge we need to do
        if merge_commits.is_fast_forward_merge() {
            let commit = self.fast_forward_merge(merge_commits.head, merge_commits.merge)?;
            Ok(Some(commit))
        } else {
            let conflicts = self.three_way_merge(&merge_commits)?;
            if conflicts.is_empty() {
                let commit = self.create_merge_commit(&branch_name, &merge_commits)?;
                Ok(Some(commit))
            } else {
                // TODO: write conflicts to disk
                Ok(None)
            }
        }
    }

    fn create_merge_commit<S: AsRef<str>>(&self, branch_name: S, merge_commits: &MergeCommits) -> Result<Commit, OxenError> {
        command::add(&self.repository, &self.repository.path)?;
        let commit_msg = format!("Merge branch '{}'", branch_name.as_ref());
        command::commit(&self.repository, &commit_msg)?;
        Ok(merge_commits.head.to_owned())
    } 

    // This will try to find the least common ancestor, and if the least common ancestor is HEAD, then we just
    // fast forward, otherwise we need to three way merge
    fn find_merge_commits<S: AsRef<str>>(&self, branch_name: S) -> Result<MergeCommits, OxenError> {
        let branch_name = branch_name.as_ref();
        let ref_reader = RefReader::new(&self.repository)?;
        let head_commit_id = ref_reader.head_commit_id()?;
        let merge_commit_id = ref_reader.get_commit_id_for_branch(branch_name)?.ok_or_else(|| OxenError::commit_db_corrupted(branch_name))?;
        
        let commit_reader = CommitReader::new(&self.repository)?;
        let head = commit_reader.get_commit_by_id(&head_commit_id)?
                            .ok_or_else(|| OxenError::commit_db_corrupted(&head_commit_id))?;
        let merge = commit_reader.get_commit_by_id(&merge_commit_id)?
                            .ok_or_else(|| OxenError::commit_db_corrupted(&merge_commit_id))?;

        let lca = self.p_lowest_common_ancestor(&commit_reader, &head, &merge)?;

        Ok(MergeCommits {
            lca: lca.to_owned(),
            head: head.to_owned(),
            merge: merge.to_owned(),
        })
    }

    /// It is a fast forward merge if we cannot traverse cleanly back from merge to HEAD
    fn fast_forward_merge(&self, head_commit: Commit, merge_commit: Commit) -> Result<Commit, OxenError> {
        let head_commit_entry_reader = CommitEntryReader::new(&self.repository, &head_commit)?;
        let merge_commit_entry_reader = CommitEntryReader::new(&self.repository, &merge_commit)?;

        let head_entries = head_commit_entry_reader.list_entries_set()?;
        let merge_entries = merge_commit_entry_reader.list_entries_set()?;

        // Can just copy over all new versions since it is fast forward
        for merge_entry in merge_entries.iter() {
            // Only copy over if hash is different or it doesn't exist for performace
            if let Some(head_entry) = head_entries.get(merge_entry) {
                if head_entry.hash != merge_entry.hash {
                    self.update_entry(&merge_entry)?;
                }
            } else {
                self.update_entry(&merge_entry)?;
            }
        }

        // Remove all entries that are in HEAD but not in merge entries
        for head_entry in head_entries.iter() {
            if !merge_entries.contains(&head_entry) {
                let path = self.repository.path.join(&head_entry.path);
                std::fs::remove_file(path)?;
            }
        }
        
        Ok(merge_commit)
    }

    /// Check if HEAD is in the direct parent chain of the merge commit. If it is a direct parent, we can just fast forward
    pub fn lowest_common_ancestor<S: AsRef<str>>(&self, branch_name: S) -> Result<Commit, OxenError> {
        let branch_name = branch_name.as_ref();
        let ref_reader = RefReader::new(&self.repository)?;
        let head_commit_id = ref_reader.head_commit_id()?;
        let merge_commit_id = ref_reader.get_commit_id_for_branch(branch_name)?
                                .ok_or_else(|| OxenError::commit_db_corrupted(branch_name))?;

        let commit_reader = CommitReader::new(&self.repository)?;
        let head_commit = commit_reader.get_commit_by_id(&head_commit_id)?
                            .ok_or_else(|| OxenError::commit_db_corrupted(&head_commit_id))?;
        let merge_commit = commit_reader.get_commit_by_id(&merge_commit_id)?
                            .ok_or_else(|| OxenError::commit_db_corrupted(&merge_commit_id))?;

        self.p_lowest_common_ancestor(&commit_reader, &head_commit, &merge_commit)
    }

    fn p_lowest_common_ancestor(&self, commit_reader: &CommitReader, head_commit: &Commit, merge_commit: &Commit) -> Result<Commit, OxenError> {
        // Traverse the HEAD commit back to start, keeping map of Commit -> Depth(int)
        let commit_depths_from_head = commit_reader.history_with_depth_from_commit(head_commit)?;

        // Traverse the merge commit back
        //   check at each step if ID is in the HEAD commit history
        //   The lowest Depth Commit in HEAD should be the LCA
        let commit_depths_from_merge = commit_reader.history_with_depth_from_commit(merge_commit)?;
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
        let lca_entry_reader = CommitEntryReader::new(&self.repository, &merge_commits.lca)?;
        let head_entry_reader = CommitEntryReader::new(&self.repository, &merge_commits.head)?;
        let merge_entry_reader = CommitEntryReader::new(&self.repository, &merge_commits.merge)?;

        let lca_entries = lca_entry_reader.list_entries_set()?;
        let head_entries = head_entry_reader.list_entries_set()?;
        let merge_entries = merge_entry_reader.list_entries_set()?;

        // Check all the entries in the candidate merge
        for merge_entry in merge_entries.iter() {
            // Check if the entry exists in all 3 commits
            if let Some(head_entry) = head_entries.get(merge_entry) {
                if let Some(lca_entry) = lca_entries.get(merge_entry) {
                    // If HEAD and LCA are the same but Merge is different, take merge
                    if head_entry.hash == lca_entry.hash {
                        self.update_entry(merge_entry)?;
                    }

                    // TODO: IS THIS CORRECT? Feels like we need to do something...
                    // If Merge and LCA are the same, but HEAD is different, take HEAD
                    // Since we are already on HEAD, this means do nothing

                    // If all three are different, mark as conflict
                    if head_entry.hash != lca_entry.hash &&
                       lca_entry.hash != merge_entry.hash &&
                       head_entry.hash != merge_entry.hash {
                        conflicts.push(MergeConflict {
                            lca_entry: lca_entry.to_owned(),
                            head_entry: head_entry.to_owned(),
                            merge_entry: merge_entry.to_owned()
                        });
                    }
                } // merge entry doesn't exist in LCA, which is fine, we will catch it in HEAD
            } else {
                // merge entry does not exist in HEAD, so create it
                self.update_entry(merge_entry)?;
            }
        }

        Ok(conflicts)
    }

    // TODO: might want to move this into a util to restore from version path (in case of compression or other transforms)
    fn update_entry(&self, merge_entry: &CommitEntry) -> Result<(), OxenError> {
        let version_file = util::fs::version_path(&self.repository, &merge_entry);
        let dst_path = self.repository.path.join(&merge_entry.path);
        std::fs::copy(version_file, dst_path)?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::Merger;
    use crate::model::{Commit, LocalRepository};
    use crate::util;
    use crate::test;

    fn populate_threeway_merge_repo(repo: &LocalRepository, merge_branch_name: &str) -> Result<Commit, OxenError> {
        // Need to have main branch get ahead of branch so that you can traverse to directory to it, but they
        // have a common ancestor
        // Ex) We want to merge E into D
        // A - C - D
        //    \
        //     B - E

        let a_branch = command::current_branch(&repo)?.unwrap();
        let a_path = repo.path.join("a.txt");
        util::fs::write_to_path(&a_path, "a");
        command::add(&repo, a_path)?;
        // Return the lowest common ancestor for the tests
        let lca = command::commit(&repo, "Committing a.txt file")?;

        // Make changes on B
        command::create_checkout_branch(&repo, merge_branch_name)?;
        let b_path = repo.path.join("b.txt");
        util::fs::write_to_path(&b_path, "b");
        command::add(&repo, b_path)?;
        command::commit(&repo, "Committing b.txt file")?;

        // Checkout A again to make another change
        command::checkout(&repo, &a_branch.name)?;
        let c_path = repo.path.join("c.txt");
        util::fs::write_to_path(&c_path, "c");
        command::add(&repo, c_path)?;
        command::commit(&repo, "Committing c.txt file")?;

        let d_path = repo.path.join("d.txt");
        util::fs::write_to_path(&d_path, "d");
        command::add(&repo, d_path)?;
        command::commit(&repo, "Committing d.txt file")?;

        // Checkout merge branch (B) to make another change
        command::checkout(&repo, merge_branch_name)?;
        let e_path = repo.path.join("e.txt");
        util::fs::write_to_path(&e_path, "e");
        command::add(&repo, e_path)?;
        command::commit(&repo, "Committing e.txt file")?;

        // Checkout the OG branch again so that we can merge into it
        command::checkout(&repo, &a_branch.name)?;

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

            let merger = Merger::new(&repo);
            merger.merge(branch_name)?;

            // Now that we've merged in, world file should exist
            assert!(world_file.exists());

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

            let merger = Merger::new(&repo);
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

            let merger = Merger::new(&repo);
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
            let merger = Merger::new(&repo);
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
            let merger = Merger::new(&repo);
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

            // Get commit history so that we can compare after the merge and make sure a merge commit was added
            let og_commit_history = command::log(&repo)?;

            // Make sure the merger can detect the three way merge
            let merger = Merger::new(&repo);
            merger.merge(merge_branch_name)?;
        
            // There should be 5 files: [a.txt, b.txt, c.txt, d.txt e.txt]
            let file_prefixes = vec!["a", "b", "c", "d", "e"];
            for prefix in file_prefixes.iter() {
                let filename = format!("{}.txt", prefix);
                let filepath = repo.path.join(filename);
                println!("test_merge_no_conflict_three_way_merge checking file exists {:?}", filepath);
                assert!(filepath.exists());
            }

            // Make sure we added the merge commit
            let post_merge_history = command::log(&repo)?;
            assert_eq!(og_commit_history.len()+1, post_merge_history.len());

            // Make sure the repo is clean
            let status = command::status(&repo)?;
            assert!(status.is_clean());

            Ok(())
        })
    }

    // TODO: What to do if there are conflicts in the three way merge?
}