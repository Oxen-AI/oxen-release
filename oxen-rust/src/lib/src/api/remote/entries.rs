use crate::api;
use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository, RemoteEntry, RemoteRepository};
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
    repository: &RemoteRepository,
    page_num: usize,
) -> Result<PaginatedEntries, OxenError> {
    let config = AuthConfig::default()?;
    let uri = format!("/repositories/{}/entries?page={}", repository.id, page_num);
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
            println!("Posting entry {:?}", entry);
            let result = api::remote::entries::create(local_repo, entry);
            log::debug!("{:?}", result);
            assert!(result.is_ok());

            Ok(())
        })
    }

    #[test]
    fn test_list_entries() -> Result<(), OxenError> {
        // let repo_name = format!("{}", uuid::Uuid::new_v4());
        // let repo_cfg = test::create_repo_cfg(&repo_name)?;

        // let paths = vec![
        //     Path::new("data/test/images/cole_anthony.jpeg"),
        //     Path::new("data/test/images/dwight_vince.jpeg"),
        //     Path::new("data/test/images/ignas_brazdeikis.jpeg"),
        // ];

        // for path in paths.iter() {
        //     let hash = hasher::hash_file_contents(path)?;
        //     api::entries::create(&repo_cfg, path, &hash)?;
        // }

        // let page = api::entries::list_page(&repo_cfg, 1)?;
        // assert_eq!(page.page_number, 1);
        // assert_eq!(page.total_entries, paths.len());
        // assert_eq!(page.total_pages, 1);

        // // cleanup
        // api::repositories::delete(&repo_cfg, &repo_cfg.repository)?;
        assert!(false);
        Ok(())
    }
}
