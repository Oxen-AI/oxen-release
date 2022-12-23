use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::PaginatedDirEntries;

pub async fn list_dir(
    remote_repo: &RemoteRepository,
    commit_or_branch: &str,
    path: &str,
    page_num: usize,
    page_size: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    let uri = format!(
        "/dir/{}/{}?page_num={}&page_size={}",
        commit_or_branch, path, page_num, page_size
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let status = res.status();
        let body = res.text().await?;
        // log::debug!("list_page got body: {}", body);
        let response: Result<PaginatedDirEntries, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(val) => Ok(val),
            Err(_) => Err(OxenError::basic_str(format!(
                "api::dir::list_dir {} Err status_code[{}] \n\n{}",
                url, status, body
            ))),
        }
    } else {
        let err = format!("api::dir::list_dir Err request failed: {}", url);
        Err(OxenError::basic_str(err))
    }
}
