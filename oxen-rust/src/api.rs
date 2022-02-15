
use crate::config::Config;
use crate::model::user::*;
use crate::model::entry::*;
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
  println!("params {:?}", params);

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

pub fn entry_from_hash(config: &Config, user: &User, hash: &String) -> Result<Entry, String> {
  let url = format!("{}/entries/search?hash={}", config.endpoint(), hash);
  let client = reqwest::blocking::Client::new();
  if let Ok(res) = client.get(url)
                    .header(reqwest::header::AUTHORIZATION, &user.access_token)
                    .send() { 
    if let Ok(entry_res) = res.json::<EntryResponse>() {
      Ok(entry_res.entry)
    } else {
      // probably not the best, I wish we didn't have to serialized to check
      Err(String::from("Could not serialize entry"))
    }
  } else {
    println!("hash_exists request failed..");
    Err(String::from("Request failed"))
  }
}