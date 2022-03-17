

use crate::config::repo_config::RepoConfig;
use crate::model::dataset::{Dataset, ListDatasetsResponse};

pub fn list(config: &RepoConfig) -> Result<Vec<Dataset>, String> {
  if let (Some(user), Some(repository_id)) = (&config.user, &config.repository_id) {
    let url = format!("{}/repositories/{}/datasets", config.endpoint(), repository_id);
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client.get(url)
                      .header(reqwest::header::AUTHORIZATION, &user.token)
                      .send() {
      if let Ok(datasets_res) = res.json::<ListDatasetsResponse>() {
        Ok(datasets_res.datasets)
      } else {
        Err(String::from("Could not serialize entry"))
      }
    } else {
      println!("hash_exists request failed..");
      Err(String::from("Request failed"))
    }
  } else {
    Err(String::from("User is not logged in."))
  }
}
