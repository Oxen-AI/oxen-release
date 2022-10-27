use crate::error::OxenError;
use crate::index::{CommitDirReader, CommitSchemaRowIndex, SchemaReader};
use crate::model::{Commit, CommitEntry, ContentHashable, LocalRepository, NewCommit};
use crate::util;

struct SimpleHash {
    hash: String,
}

impl ContentHashable for SimpleHash {
    fn content_hash(&self) -> String {
        self.hash.clone()
    }
}

pub struct CommitValidator {
    repository: LocalRepository,
}

impl CommitValidator {
    pub fn new(repository: &LocalRepository) -> CommitValidator {
        CommitValidator {
            repository: repository.clone(),
        }
    }

    pub fn compute_versions_hash(
        &self,
        commit: &NewCommit,
        entries: &[CommitEntry],
    ) -> Result<Option<String>, OxenError> {
        // log::debug!("Computing commit hash for {} entries", entries.len());
        let mut hashes: Vec<SimpleHash> = vec![];
        for entry in entries.iter() {
            // Sometimes we have pre computed the HASH, so that we don't have to fully hash contents again to
            // check if data is synced (I guess this is already in the file path...should we just grab it from there instead?)
            // I think the extra hash computation on the server is nice so that you know the actual contents was saved to disk
            // Cannot be tampered with in flight (I'm not technical enough to even know how you do that in theory but 🤷‍♂️)
            let version_path = util::fs::version_path(&self.repository, entry);

            let maybe_hash_file = version_path.parent().unwrap().join("HASH");
            // log::debug!("Checking for hash file: {:?}", maybe_hash_file);
            if maybe_hash_file.exists() {
                let hash = util::fs::read_from_path(&maybe_hash_file)?;
                // log::debug!("GOT HASH FILE {hash} => {:?}", entry.path);
                hashes.push(SimpleHash { hash });
                continue;
            }

            let hash = if util::fs::is_tabular(&entry.path) {
                let schema_reader = SchemaReader::new(&self.repository, &entry.commit_id)?;
                let schema = schema_reader.get_schema_for_file(&entry.path)?.unwrap();
                let reader = CommitSchemaRowIndex::new(
                    &self.repository,
                    &entry.commit_id,
                    &schema,
                    &entry.path,
                )?;
                let df = reader.sorted_entry_df_with_row_hash()?;

                util::hasher::compute_tabular_hash(&df)
            } else {
                if !version_path.exists() {
                    log::debug!(
                        "Could not find version path for {:?} -> {:?}",
                        entry.path,
                        version_path
                    );
                    return Ok(None);
                }

                util::hasher::hash_file_contents(&version_path)?
            };

            hashes.push(SimpleHash { hash })
        }

        let content_id = util::hasher::compute_commit_hash(commit, &hashes);
        Ok(Some(content_id))
    }

    pub fn has_all_data(&self, commit: &Commit, size: usize) -> Result<bool, OxenError> {
        let commit_entry_reader = CommitDirReader::new(&self.repository, commit)?;
        let entries = commit_entry_reader.list_entries()?;
        if size != entries.len() {
            log::debug!("has_all_data {} != {}", size, entries.len());
            return Ok(false);
        }

        let n_commit = NewCommit::from_commit(commit); // need this to pass in metadata about commit
        let entries_id = util::hasher::compute_commit_hash(&n_commit, &entries);

        if let Some(content_id) = self.compute_versions_hash(&n_commit, &entries)? {
            log::debug!(
                "Done computing commit hash for {} entries {} == {}",
                entries.len(),
                entries_id,
                content_id
            );
            Ok(entries_id == content_id)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::CommitValidator;
    use crate::test;
    use crate::util;

    #[test]
    fn test_commit_validator_validate_commit() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = command::log(&repo)?;
            let latest_commit = commits.first().unwrap();
            let validator = CommitValidator::new(&repo);
            let total_entries = util::fs::rcount_files_in_dir(&repo.path);

            // Local repo should work just fine
            match validator.has_all_data(latest_commit, total_entries) {
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
