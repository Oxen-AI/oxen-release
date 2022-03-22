use crate::config::{HTTPConfig, RepoConfig};
use crate::error::OxenError;
use crate::model::{Dataset, Entry, EntryResponse, PaginatedEntries};

use std::path::Path;

pub fn from_hash<'a>(config: &'a dyn HTTPConfig<'a>, hash: &str) -> Result<Entry, OxenError> {
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
        let response: Result<EntryResponse, serde_json::Error> = serde_json::from_str(&body);
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

pub fn create(config: &RepoConfig, dataset: &Dataset, path: &Path) -> Result<Entry, OxenError> {
    if let Ok(form) = reqwest::blocking::multipart::Form::new().file("file", &path) {
        let client = reqwest::blocking::Client::new();
        let url = format!(
            "http://{}/api/v1/repositories/{}/datasets/{}/entries",
            config.host(),
            config.repository.id,
            dataset.id
        );
        if let Ok(res) = client
            .post(url)
            .header(reqwest::header::AUTHORIZATION, config.auth_token())
            .multipart(form)
            .send()
        {
            let status = res.status();
            let body = res.text()?;
            let response: Result<EntryResponse, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.entry),
                Err(_) => Err(OxenError::basic_str(&format!(
                    "status_code[{}] \n\n{}",
                    status, body
                ))),
            }
        } else {
            Err(OxenError::basic_str(
                "api::entries::create error sending data from file",
            ))
        }
    } else {
        let err = format!(
            "api::entries::create Could not create form for file {:?}",
            path
        );
        Err(OxenError::basic_str(&err))
    }
}

pub fn list_page(
    config: &RepoConfig,
    dataset: &Dataset,
    page_num: i64,
) -> Result<PaginatedEntries, OxenError> {
    let url = format!(
        "http://{}/api/v1/repositories/{}/datasets/{}/entries?page={}",
        config.host(),
        config.repository.id,
        dataset.id,
        page_num
    );
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(&url)
        .header(reqwest::header::AUTHORIZATION, config.auth_token())
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
    use crate::error::OxenError;
    use crate::test;
    use crate::util::hasher;

    use std::path::Path;

    #[test]
    fn test_create_image_entry() -> Result<(), OxenError> {
        let img_path = test::test_jpeg_file();
        let repo_name = format!("{}", uuid::Uuid::new_v4());
        let repo_cfg = test::create_repo_cfg(&repo_name)?;
        let dataset = api::datasets::create(&repo_cfg, "dataset_1")?;
        let hash = hasher::hash_file_contents(img_path)?;
        let entry = api::entries::create(&repo_cfg, &dataset, img_path)?;

        assert_eq!("image", entry.data_type);
        assert_eq!(hash, entry.hash);

        // cleanup
        api::datasets::delete(&repo_cfg, &dataset)?;
        api::repositories::delete(&repo_cfg, &repo_cfg.repository)?;
        Ok(())
    }

    #[test]
    fn test_list_entries() -> Result<(), OxenError> {
        let repo_name = format!("{}", uuid::Uuid::new_v4());
        let repo_cfg = test::create_repo_cfg(&repo_name)?;
        let dataset = api::datasets::create(&repo_cfg, "dataset_1")?;

        let paths = vec![
            Path::new("data/test/images/cole_anthony.jpeg"),
            Path::new("data/test/images/dwight_vince.jpeg"),
            Path::new("data/test/images/ignas_brazdeikis.jpeg"),
        ];

        for path in paths.iter() {
            api::entries::create(&repo_cfg, &dataset, path)?;
        }

        let page = api::entries::list_page(&repo_cfg, &dataset, 1)?;
        assert_eq!(page.page_number, 1);
        assert_eq!(page.total_entries, paths.len());
        assert_eq!(page.total_pages, 1);

        // cleanup
        api::datasets::delete(&repo_cfg, &dataset)?;
        api::repositories::delete(&repo_cfg, &repo_cfg.repository)?;
        Ok(())
    }
}
