
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Repository {
  pub id: String,
  pub name: String,
}

#[derive(Deserialize, Debug)]
pub struct RepositoryResponse {
  pub repository: Repository,
}

#[derive(Deserialize, Debug)]
pub struct ListRepositoriesResponse {
  pub repositories: Vec<Repository>,
}

