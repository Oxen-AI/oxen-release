use crate::index::Committer;
use crate::model::{Commit, CommitEntry, LocalRepository};

use crate::error::OxenError;

pub fn list_all(repo: &LocalRepository, commit: &Commit) -> Result<Vec<CommitEntry>, OxenError> {
    let committer = Committer::new(repo)?;
    let entries = committer.list_entries_for_commit(commit)?;
    Ok(entries)
}

#[cfg(test)]
mod tests {
    
    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_api_local_entries_list_all() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (file already created in helper)
            let file_to_add = repo.path.join("labels.txt");

            // Commit the file
            command::add(&repo, &file_to_add)?;
            let commit = command::commit(&repo, "Adding labels file")?.unwrap();

            let entries = api::local::entries::list_all(&repo, &commit)?;
            assert_eq!(entries.len(), 1);

            Ok(())
        })
    }
}
