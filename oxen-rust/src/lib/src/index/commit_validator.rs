use crate::constants::HASH_FILE;
use crate::error::OxenError;
use crate::index::CommitDirReader;
use crate::model::{Commit, CommitEntry, ContentHashable, LocalRepository, NewCommit};
use crate::util;

#[derive(Debug)]
struct SimpleHash {
    hash: String,
}

impl ContentHashable for SimpleHash {
    fn content_hash(&self) -> String {
        self.hash.clone()
    }
}

pub fn compute_commit_content_hash(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<String, OxenError> {
    let commit_entry_reader = CommitDirReader::new(repository, commit)?;
    let entries = commit_entry_reader.list_entries()?;
    let n_commit = NewCommit::from_commit(commit); // need this to pass in metadata about commit
    let content_hash = compute_versions_hash(repository, &n_commit, &entries)?;
    Ok(content_hash)
}

fn compute_versions_hash(
    repository: &LocalRepository,
    commit: &NewCommit,
    entries: &[CommitEntry],
) -> Result<String, OxenError> {
    // log::debug!("Computing commit hash for {} entries", entries.len());
    let mut hashes: Vec<SimpleHash> = vec![];
    for (_, entry) in entries.iter().enumerate() {
        // Sometimes we have pre computed the HASH, so that we don't have to fully hash contents again to
        // check if data is synced (I guess this is already in the file path...should we just grab it from there instead?)
        // I think the extra hash computation on the server is nice so that you know the actual contents was saved to disk
        let version_path = util::fs::version_path(repository, entry);

        let maybe_hash_file = version_path.parent().unwrap().join(HASH_FILE);
        // log::debug!("Entry [{}]: {:?}", i, entry.path);
        if maybe_hash_file.exists() {
            let hash = util::fs::read_from_path(&maybe_hash_file)?;
            // log::debug!(
            //     "compute_versions_hash cached hash [{i}] {hash} => {:?}",
            //     entry.path
            // );
            hashes.push(SimpleHash { hash });
            continue;
        }

        let hash = util::hasher::hash_file_contents(&version_path)?;
        // log::debug!("Got hash: {:?} -> {}", entry.path, hash);

        hashes.push(SimpleHash { hash })
    }

    let content_id = util::hasher::compute_commit_hash(commit, &hashes);
    Ok(content_id)
}

pub fn has_all_data(
    repository: &LocalRepository,
    commit: &Commit,
    size: usize,
) -> Result<bool, OxenError> {
    log::debug!(
        "has_all_data: {size} entries {} -> {}",
        commit.id,
        commit.message
    );
    let commit_entry_reader = CommitDirReader::new(repository, commit)?;
    let entries = commit_entry_reader.list_entries()?;

    if size != entries.len() {
        log::debug!("has_all_data {} != {}", size, entries.len());
        return Ok(false);
    }

    // for (i, entry) in entries.iter().enumerate() {
    //     log::debug!("has_all_data entry[{i}] {:?}", entry.path);
    // }

    let n_commit = NewCommit::from_commit(commit); // need this to pass in metadata about commit
    let expected_hash = util::hasher::compute_commit_hash(&n_commit, &entries);

    let content_hash = compute_versions_hash(repository, &n_commit, &entries)?;
    log::debug!(
        "Done computing commit hash for {} expected == content -> {} == {}",
        entries.len(),
        expected_hash,
        content_hash
    );
    Ok(expected_hash == content_hash)
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::commit_validator;
    use crate::test;
    use crate::util;

    #[test]
    fn test_commit_validator_validate_commit() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let latest_commit = commits.first().unwrap();
            let total_entries = util::fs::rcount_files_in_dir(&repo.path);

            // Local repo should work just fine
            match commit_validator::has_all_data(&repo, latest_commit, total_entries) {
                Ok(result) => assert!(result),
                Err(err) => {
                    log::error!("Err: {}", err);
                    panic!("test_commit_validator_validate_commit failed");
                }
            }

            Ok(())
        })
    }
}
