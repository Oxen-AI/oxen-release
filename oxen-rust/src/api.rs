
use crate::config::Config;
use crate::model::user::*;
use crate::model::entry::*;
use crate::model::dataset::*;
use serde_json::json;
use reqwest::blocking::Client;

pub fn get_user(config: &Config) -> Result<User, String> {
  let url = format!("{}/login", config.endpoint());
  let params = json!({
    "user": {
      "email": config.email,
      "password": config.password,
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

pub fn entry_from_hash(config: &Config, hash: &String) -> Result<Entry, String> {
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

pub fn list_datasets(config: &Config) -> Result<Vec<Dataset>, String> {
  if let Some(user) = &config.user {
    let url = format!("{}/repositories/{}/datasets", config.endpoint(), config.repository_id);
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
