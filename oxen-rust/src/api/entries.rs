use crate::config::repo_config::RepoConfig;
use crate::error::OxenError;
use crate::model::{Entry, EntryResponse};

pub fn from_hash(config: &RepoConfig, hash: &str) -> Result<Entry, OxenError> {
    let url = format!("{}/entries/search?hash={}", config.endpoint(), hash);
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(reqwest::header::AUTHORIZATION, &config.user.token)
        .send()
    {
        if let Ok(entry_res) = res.json::<EntryResponse>() {
            Ok(entry_res.entry)
        } else {
            Err(OxenError::basic_str("Could not serialize entry"))
        }
    } else {
        println!("hash_exists request failed..");
        Err(OxenError::basic_str("Request failed"))
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;

    #[test]
    fn test_get_from_hash() -> Result<(), OxenError> {
        // TODO: implement
        Ok(())
    }
}
