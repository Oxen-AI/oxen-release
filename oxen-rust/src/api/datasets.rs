use crate::config::repo_config::RepoConfig;
use crate::error::OxenError;
use crate::model::dataset::{Dataset, ListDatasetsResponse};

pub fn list(config: &RepoConfig) -> Result<Vec<Dataset>, OxenError> {
    if let (Some(user), Some(repository_id)) = (&config.user, &config.repository_id) {
        let url = format!(
            "{}/repositories/{}/datasets",
            config.endpoint(),
            repository_id
        );
        let client = reqwest::blocking::Client::new();
        if let Ok(res) = client
            .get(url)
            .header(reqwest::header::AUTHORIZATION, &user.token)
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
    } else {
        Err(OxenError::basic_str("Could not serialize entry"))
    }
}
