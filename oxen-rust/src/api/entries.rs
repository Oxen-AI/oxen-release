use crate::config::HTTPConfig;
use crate::error::OxenError;
use crate::model::{Entry, EntryResponse};

pub fn from_hash<'a>(config: &'a dyn HTTPConfig<'a>, hash: &str) -> Result<Entry, OxenError> {
    let url = format!(
        "http://{}/api/v1/entries/search?hash={}",
        config.host(),
        hash
    );
    let client = reqwest::blocking::Client::new();
    if let Ok(res) = client
        .get(url)
        .header(reqwest::header::AUTHORIZATION, config.auth_token())
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

pub fn list_page(
    _config: &dyn HTTPConfig,
    _dataset_id: &str,
    _page_num: i64,
    _page_size: i32,
) -> Result<Vec<Entry>, OxenError> {
    Ok(vec![])
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;

    #[test]
    fn test_list_page() -> Result<(), OxenError> {
        Ok(())
    }
}
