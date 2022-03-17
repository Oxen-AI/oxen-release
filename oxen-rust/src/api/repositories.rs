


use crate::config::Config;
use crate::model::repository::{Repository};

pub fn create(config: &Config) -> Result<Repository, String> {
  if let Some(user) = &config.user {
    let url = format!("{}/repositories", config.endpoint(), config.repository_id);
    let params = json!({
      "name": config.email
    });
  
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client.post(url)
                      .json(&params)
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

#[cfg(test)]
mod tests {

  #[test]
  fn test_create() -> Result<(), String> {

    Ok(())
  }
}
