use crate::config::{AuthConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::{RemoteEntry, RemoteRepository};
use crate::view::{RemoteEntryResponse, PaginatedEntries};

use std::fs::File;
use std::path::Path;

pub fn from_hash<'a>(config: &'a dyn HTTPConfig<'a>, hash: &str) -> Result<RemoteEntry, OxenError> {
    let url = format!(
        "http://{}/api/v1/entries/search?hash={}",
        config.host(),
        hash
    );
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(reqwest::header::AUTHORIZATION, config.auth_token())
        .send()
    {
        let status = res.status();
        let body = res.text()?;
        let response: Result<RemoteEntryResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val.entry),
            Err(_) => Err(OxenError::basic_str(&format!(
                "status_code[{}] \n\n{}",
                status, body
            ))),
        }
    } else {
        println!("hash_exists request failed..");
        Err(OxenError::basic_str("Request failed"))
    }
}

pub fn create(repository: &RemoteRepository, path: &Path, hash: &str) -> Result<RemoteEntry, OxenError> {
    let file = File::open(path)?;
    let client = reqwest::blocking::Client::new();
    let url = format!(
        "http://0.0.0.0:3000/repositories/{}/entries?filename={}&hash={}",
        repository.name,
        path.to_str().unwrap(),
        hash
    );
    match client.post(url).body(file).send() {
        Ok(res) => {
            let status = res.status();
            let body = res.text()?;
            let response: Result<RemoteEntryResponse, serde_json::Error> = serde_json::from_str(&body);
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
    let auth = AuthConfig::default()?;
    let url = format!(
        "http://{}/api/v1/repositories/{}/entries?page={}",
        auth.host(),
        repository.id,
        page_num
    );
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(&url)
        .header(reqwest::header::AUTHORIZATION, auth.auth_token())
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
    /*
        use crate::api;
        use crate::error::OxenError;
        use crate::test;
        use crate::util::hasher;

        use std::path::Path;

        // TODO: Make these tests for oxen-server not oxen-hub

        #[test]
        fn test_create_image_entry() -> Result<(), OxenError> {
            let img_path = test::test_jpeg_file();
            let repo_name = format!("{}", uuid::Uuid::new_v4());
            let repo_cfg = test::create_repo_cfg(&repo_name)?;
            let hash = hasher::hash_file_contents(img_path)?;
            let entry = api::entries::create(&repo_cfg, img_path, &hash)?;

            assert_eq!("image", entry.data_type);
            assert_eq!(hash, entry.hash);

            // cleanup
            api::repositories::delete(&repo_cfg, &repo_cfg.repository)?;
            Ok(())
        }

        #[test]
        fn test_list_entries() -> Result<(), OxenError> {
            let repo_name = format!("{}", uuid::Uuid::new_v4());
            let repo_cfg = test::create_repo_cfg(&repo_name)?;

            let paths = vec![
                Path::new("data/test/images/cole_anthony.jpeg"),
                Path::new("data/test/images/dwight_vince.jpeg"),
                Path::new("data/test/images/ignas_brazdeikis.jpeg"),
            ];

            for path in paths.iter() {
                let hash = hasher::hash_file_contents(path)?;
                api::entries::create(&repo_cfg, path, &hash)?;
            }

            let page = api::entries::list_page(&repo_cfg, 1)?;
            assert_eq!(page.page_number, 1);
            assert_eq!(page.total_entries, paths.len());
            assert_eq!(page.total_pages, 1);

            // cleanup
            api::repositories::delete(&repo_cfg, &repo_cfg.repository)?;
            Ok(())
        }
    */
}
