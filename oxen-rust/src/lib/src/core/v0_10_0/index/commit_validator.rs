use crate::constants::HASH_FILE;
use crate::core::v0_10_0::index::CommitEntryReader;
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, ContentHashable, LocalRepository, NewCommit};
use crate::repositories;
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

pub fn validate_tree_hash(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<bool, OxenError> {
    let is_valid: Result<bool, OxenError> = if commit.parent_ids.is_empty() {
        validate_complete_merkle_tree(repository, commit)
    } else {
        let parent = repositories::commits::get_by_id(repository, &commit.parent_ids[0])?.ok_or(
            OxenError::basic_str(format!("parent commit not found for commit {}", commit.id)),
        )?;
        validate_changed_parts_of_merkle_tree(repository, commit, &parent)
    };

    match is_valid {
        Ok(is_valid) => {
            log::debug!("got is_valid {:?} for commit {:?}", is_valid, commit);
            Ok(is_valid)
        }
        Err(e) => {
            log::debug!("got is_valid error {:?} for commit {:?}", e, commit);
            Err(e)
        }
    }
}

pub fn compute_commit_content_hash(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<String, OxenError> {
    let commit_entry_reader = CommitEntryReader::new(repository, commit)?;
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
    for entry in entries.iter() {
        // Sometimes we have pre computed the HASH, so that we don't have to fully hash contents again to
        // check if data is synced (I guess this is already in the file path...should we just grab it from there instead?)
        // I think the extra hash computation on the server is nice so that you know the actual contents was saved to disk
        let version_path = util::fs::version_path(repository, entry);
        let maybe_hash_file = version_path.parent().unwrap().join(HASH_FILE);
        // log::debug!("Versions hash Entry [{}]: {:?}", i, entry.path);
        if maybe_hash_file.exists() {
            let hash = util::fs::read_from_path(&maybe_hash_file)?;
            // log::debug!(
            //     "compute_versions_hash cached hash [{i}] {hash} => {:?}",
            //     entry.path
            // );
            hashes.push(SimpleHash { hash });
            continue;
        }

        let hash = util::hasher::hash_file_contents_with_retry(&version_path)?;
        // log::debug!("Got hash: {:?} -> {}", entry.path, hash);

        hashes.push(SimpleHash { hash })
    }

    let content_id = util::hasher::compute_commit_hash(commit, &hashes);
    Ok(content_id)
}

fn validate_complete_merkle_tree(
    _repository: &LocalRepository,
    _commit: &Commit,
) -> Result<bool, OxenError> {
    panic!("v0.10.0 no longer supported");
}

fn validate_changed_parts_of_merkle_tree(
    _repository: &LocalRepository,
    _commit: &Commit,
    _parent: &Commit,
) -> Result<bool, OxenError> {
    panic!("v0.10.0 no longer supported");
}
