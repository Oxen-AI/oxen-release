use crate::error::OxenError;
use crate::index::{RefReader, CommitReader, CommitEntryReader};
use crate::model::{Commit, CommitEntry, LocalRepository};
use crate::util;

pub struct Merger {
    repository: LocalRepository
}

impl Merger {
    pub fn new(repo: &LocalRepository) -> Merger {
        Merger {
            repository: repo.to_owned()
        }
    }

    /// # Merge a branch name into the current checked out branch
    pub fn merge<S: AsRef<str>>(&self, branch_name: S) -> Result<Commit, OxenError> {
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

        // Check which type of merge we need to do
        if self.needs_threeway_merge(&commit_reader, &head_commit, &merge_commit)? {
            let commit = self.three_way_merge(head_commit, merge_commit)?;
            Ok(commit)
        } else {
            let commit = self.fast_forward_merge(head_commit, merge_commit)?;
            Ok(commit)
        }
    }

    /// Check if HEAD is *not* in the direct parent chain of the merge commit. If it is a direct parent, we can just fast forward
    fn needs_threeway_merge(&self, commit_reader: &CommitReader, head_commit: &Commit, search_commit: &Commit) -> Result<bool, OxenError> {
        if search_commit.id == head_commit.id {
            return Ok(true)
        } else {
            for parent_id in search_commit.parent_ids.iter() {
                if let Some(parent) = commit_reader.get_commit_by_id(parent_id)? {
                    self.needs_threeway_merge(commit_reader, head_commit, &parent)?;
                }
            }
        }
        Ok(false)
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

    fn three_way_merge(&self, head_commit: Commit, merge_commit: Commit) -> Result<Commit, OxenError> {
        Ok(merge_commit)
    }

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
    use crate::util;
    use crate::test;

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

    // TODO test for merge conflicts (if main branch gets ahead, need to write detection logic for this)
}