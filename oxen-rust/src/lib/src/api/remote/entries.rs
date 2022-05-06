use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository, RemoteEntry};
use crate::view::{PaginatedEntries, RemoteEntryResponse};

use std::fs::File;

pub fn create(repository: &LocalRepository, entry: &CommitEntry) -> Result<RemoteEntry, OxenError> {
    let config = AuthConfig::default()?;
    let fullpath = repository.path.join(&entry.path);
    let file = File::open(&fullpath)?;
    let client = reqwest::blocking::Client::new();
    let uri = format!(
        "/repositories/{}/entries?filename={}&hash={}",
        repository.name,
        entry.path.to_str().unwrap(),
        entry.hash
    );
    let url = api::endpoint::url_from(&uri);
    match client
        .post(url)
        .body(file)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        Ok(res) => {
            let status = res.status();
            let body = res.text()?;
            let response: Result<RemoteEntryResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(result) => Ok(result.entry),
                Err(_) => Err(OxenError::basic_str(&format!(
                    "Error serializing EntryResponse: status_code[{}] \n\n{}",
                    status, body
                ))),
            }
        }
        Err(err) => {
            let err = format!("api::entries::create err: {}", err);
            Err(OxenError::basic_str(&err))
        }
    }
}

pub fn list_page(
    repository: &LocalRepository,
    commit: &Commit,
    page_num: usize,
) -> Result<PaginatedEntries, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!(
        "/repositories/{}/commits/{}/entries?page={}",
        repository.name, commit.id, page_num
    );
    let url = api::endpoint::url_from(&uri);
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", config.auth_token()),
        )
        .send()
    {
        let status = res.status();
        let body = res.text()?;
        let response: Result<PaginatedEntries, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val),
            Err(_) => Err(OxenError::basic_str(&format!(
                "status_code[{}] \n\n{}",
                status, body
            ))),
        }
    } else {
        let err = format!("api::entries::list_page request failed: {}", url);
        Err(OxenError::basic_str(&err))
    }
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::index::Committer;
    use crate::test;
    use crate::util;

    #[test]
    fn test_create_entry() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, _remote_repo| {
            // Track an image
            let image_file = local_repo.path.join("train").join("dog_1.jpg");
            command::add(local_repo, &image_file)?;
            // Commit the directory
            let commit = command::commit(local_repo, "Adding image")?.unwrap();

            let committer = Committer::new(local_repo)?;
            let entries = committer.list_unsynced_entries_for_commit(&commit)?;
            assert!(!entries.is_empty());

            let entry = entries.last().unwrap();
            let result = api::remote::entries::create(local_repo, entry);
            assert!(result.is_ok());

            Ok(())
        })
    }

    #[test]
    fn test_list_entries() -> Result<(), OxenError> {
        test::run_training_data_sync_test_no_commits(|local_repo, _remote_repo| {
            // Track train directory
            let train_dir = local_repo.path.join("train");
            let num_files = util::fs::rcount_files_in_dir(&train_dir);
            command::add(local_repo, &train_dir)?;
            // Commit the directory
            let commit = command::commit(local_repo, "Adding image")?.unwrap();
            command::push(local_repo)?;

            let entries = api::remote::entries::list_page(local_repo, &commit, 1)?;
            assert_eq!(entries.total_entries, num_files);
            assert_eq!(entries.entries.len(), num_files);

            Ok(())
        })
    }
}
