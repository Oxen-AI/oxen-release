use serde::Deserialize;

#[derive(Deserialize)]
pub struct ForkRequest {
    pub namespace: String,
    pub new_repo_name: Option<String>,
}
