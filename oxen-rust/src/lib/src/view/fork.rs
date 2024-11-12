use serde::Deserialize;

#[derive(Deserialize)]
pub struct ForkRequest {
    pub user_namespace: String,
    pub new_repo_name: Option<String>,
    pub organization_namespace: Option<String>,
}
