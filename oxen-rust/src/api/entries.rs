use crate::config::{HTTPConfig, RepoConfig};
use crate::error::OxenError;
use crate::model::{
    Dataset,
    Entry,
    EntryResponse
};

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
    if let Ok(form) = reqwest::blocking::multipart::Form::new().file("file", path) {
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
            Err(OxenError::basic_str("api::entries::create error sending data from file"))
        }
    } else {
        Err(OxenError::basic_str("api::entries::create Could not create form"))
    }
}

pub fn list_page(
    _config: &dyn HTTPConfig,
    _dataset_id: &str,
    _page_num: i64,
    _page_size: i32,
) -> Result<Vec<Entry>, OxenError> {
    Ok(vec![])
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::error::OxenError;
    use crate::test;
    use crate::util::hasher;

    #[test]
    fn test_create_image_entry() -> Result<(), OxenError> {
        let img_path = test::test_jpeg_file();
        let repo_cfg = test::create_repo_cfg("test entry repo")?;
        let dataset = api::datasets::create(&repo_cfg, "dataset_1")?;
        let hash = hasher::hash_file_contents(img_path)?;
        let entry = api::entries::create(&repo_cfg, &dataset, &img_path)?;

        assert_eq!("image", entry.data_type);
        assert_eq!(hash, entry.hash);

        // cleanup
        api::datasets::delete(&repo_cfg, &dataset)?;
        api::repositories::delete(&repo_cfg, &repo_cfg.repository)?;
        Ok(())
    }
}
