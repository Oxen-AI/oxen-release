use crate::config::{RepoConfig, HTTPConfig};
use crate::error::OxenError;
use crate::model::StatusMessage;
use crate::model::dataset::{Dataset, DatasetResponse, ListDatasetsResponse};
use reqwest::blocking::Client;
use serde_json::json;

pub fn list(config: &RepoConfig) -> Result<Vec<Dataset>, OxenError> {
    let url = format!(
        "http://{}/api/v1/repositories/{}/datasets",
        config.host(),
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
        "http://{}/api/v1/repositories/{}/datasets",
        config.host(),
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
        let body = res.text()?;
        let response: Result<DatasetResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => {
                Ok(val.dataset)
            },
            Err(_) => {
                Err(OxenError::basic_str(&format!(
                    "status_code[{}], could not create dataset \n\n{}",
                    status,
                    body
                )))
            }
        }
    } else {
        Err(OxenError::basic_str("api::create_dataset() API failed"))
    }
}

pub fn delete(config: &RepoConfig, dataset: &Dataset) -> Result<StatusMessage, OxenError> {
    let url = format!("http://{}/api/v1/repositories/{}/datasets/{}", config.host(), config.repository.id, dataset.id);

    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .delete(url)
        .header(reqwest::header::AUTHORIZATION, config.auth_token())
        .send()
    {
        let status = res.status();
        let body = res.text()?;
        let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => {
                Ok(val)
            },
            Err(_) => {
                Err(OxenError::basic_str(&format!(
                    "status_code[{}], could not delete repository \n\n{}",
                    status,
                    body
                )))
            }
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::delete() Request failed",
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::config::{AuthConfig, RepoConfig};
    use crate::test;
    use crate::error::OxenError;

    #[test]
    fn test_create_delete_dataset() -> Result<(), OxenError> {
        let path = test::auth_cfg_file();
        let auth_cfg = AuthConfig::from(path);
        let repo_name = uuid::Uuid::new_v4();

        let repository = api::repositories::create(&auth_cfg, &format!("{}", repo_name))?;
        let name: &str = "test dataset";

        let repo_cfg = RepoConfig::new(&auth_cfg, &repository);
        let dataset = api::datasets::create(&repo_cfg, name)?;
        assert_eq!(dataset.name, name);

        // Have to delete the dataset before the repo, because they are linked
        api::datasets::delete(&repo_cfg, &dataset)?;
        api::repositories::delete(&repo_cfg, &repository)?;
        Ok(())
    }
}
