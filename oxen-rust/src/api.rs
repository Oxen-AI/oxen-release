
use crate::config::repo_config::RepoConfig;
use crate::error::OxenError;
use crate::model::user::*;
use crate::model::entry::*;
use crate::model::dataset::*;
use serde_json::json;
use reqwest::blocking::Client;

pub mod datasets;

pub fn login(config: &RepoConfig, email: &str, password: &str) -> Result<User, OxenError> {
  let url = format!("{}/login", config.endpoint());
  let params = json!({
    "user": {
      "email": email,
      "password": password,
    }
  });

  if let Ok(res) = Client::new()
    .post(&url)
    .json(&params)
    .send() {
      let status = res.status();
      if let Ok(user_res) = res.json::<UserResponse>() {
        Ok(user_res.user)
      } else {
        let err = format!("login failed status_code[{}], check email and password", status);
        Err(OxenError::Basic(err))
      }
  } else {
    Err(OxenError::Basic(format!("login failed [{}]", &url)))
  }
}

pub fn get_user(config: &RepoConfig) -> Result<User, String> {
  let url = format!("{}/login", config.endpoint());
  let params = json!({
    "user": {
      "email": "denied",
      "password": "nope",
    }
  });

  if let Ok(res) = Client::new()
    .post(url)
    .json(&params)
    .send() {
      let status = res.status();
      if let Ok(user_res) = res.json::<UserResponse>() {
        Ok(user_res.user)
      } else {
        Err(format!("status_code[{}], check email and password", status))
      }
  } else {
    Err(String::from("api::get_user() API failed"))
  }
}

pub fn entry_from_hash(config: &RepoConfig, hash: &String) -> Result<Entry, String> {
  if let Some(user) = &config.user {
    let url = format!("{}/entries/search?hash={}", config.endpoint(), hash);
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client.get(url)
                      .header(reqwest::header::AUTHORIZATION, &user.token)
                      .send() { 
      if let Ok(entry_res) = res.json::<EntryResponse>() {
        Ok(entry_res.entry)
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

pub fn create_dataset(config: &RepoConfig, name: &str) -> Result<Dataset, String> {
  if let (Some(user), Some(repository_id)) = (&config.user, &config.repository_id) {
    let url = format!("{}/repositories/{}/datasets", config.endpoint(), repository_id);
    let params = json!({
      "name": name,
    });

    if let Ok(res) = Client::new()
      .post(url)
      .header(reqwest::header::AUTHORIZATION, &user.token)
      .json(&params)
      .send() {
        let status = res.status();
        if let Ok(user_res) = res.json::<DatasetResponse>() {
          Ok(user_res.dataset)
        } else {
          Err(format!("status_code[{}], could not create dataset", status))
        }
    } else {
      Err(String::from("api::create_dataset() API failed"))
    }
  } else {
    Err(String::from("User is not logged in."))
  }
}
