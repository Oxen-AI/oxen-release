use crate::config::repo_config::RepoConfig;
use crate::error::OxenError;
use crate::model::dataset::{Dataset, DatasetResponse, ListDatasetsResponse};
use serde_json::json;
use reqwest::blocking::Client;

pub fn list(config: &RepoConfig) -> Result<Vec<Dataset>, OxenError> {
    let url = format!(
        "{}/repositories/{}/datasets",
        config.endpoint(),
        config.repository.id
    );
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(reqwest::header::AUTHORIZATION, &config.user.token)
        .send()
    {
        if let Ok(datasets_res) = res.json::<ListDatasetsResponse>() {
            Ok(datasets_res.datasets)
        } else {
            Err(OxenError::basic_str("Could not serialize entry"))
        }
    } else {
        println!("hash_exists request failed..");
        Err(OxenError::basic_str("Could not serialize entry"))
    }
}

pub fn create(config: &RepoConfig, name: &str) -> Result<Dataset, OxenError> {
    let url = format!(
        "{}/repositories/{}/datasets",
        config.endpoint(),
        config.repository.id
    );
    let params = json!({
        "name": name,
    });

    if let Ok(res) = Client::new()
        .post(url)
        .header(reqwest::header::AUTHORIZATION, &config.user.token)
        .json(&params)
        .send()
    {
        let status = res.status();
        if let Ok(user_res) = res.json::<DatasetResponse>() {
            Ok(user_res.dataset)
        } else {
            Err(OxenError::basic_str(&format!(
                "status_code[{}], could not create dataset",
                status
            )))
        }
    } else {
        Err(OxenError::basic_str("api::create_dataset() API failed"))
    }
}

#[cfg(test)]
mod tests {

    // use crate::api;
    // use crate::config::RepoConfig;
    // use crate::test;
    use crate::error::OxenError;

    #[test]
    fn test_create_repository() -> Result<(), OxenError> {
        // let path = test::repo_cfg_file();
        // let config = RepoConfig::from(path);
        // let name: &str = "my repo";
        // let repository = api::datasets::create(&config, name)?;
        // assert_eq!(repository.name, name);
        // cleanup
        // api::repositories::delete(&config, &repository.id)?;
        Ok(())
    }
}
