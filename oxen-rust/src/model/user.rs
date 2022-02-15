
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct User {
  pub id: String,
  pub access_token: String,
  pub email: String,
  pub name: String,
}

#[derive(Deserialize, Debug)]
pub struct UserResponse {
  pub user: User
}

