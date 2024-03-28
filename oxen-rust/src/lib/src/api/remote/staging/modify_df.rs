use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::entry::mod_entry::ModType;
use crate::model::ContentType;
use crate::model::{ModEntry, RemoteRepository};
use crate::view::json_data_frame_view::JsonDataFrameRowResponse;
use crate::view::{JsonDataFrameViewResponse, StagedFileModResponse};

use std::path::Path;

pub async fn modify_df(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    path: &Path,
    data: String,
    content_type: ContentType,
    mod_type: ModType,
) -> Result<JsonDataFrameRowResponse, OxenError> {
    if mod_type != ModType::Append {
        return Err(OxenError::basic_str(
            "api::staging::modify_df only supports ModType::Append",
        ));
    }

    let file_path_str = path.to_str().unwrap();
    let uri = format!("/staging/{identifier}/df/rows/{branch_name}/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("modify_df {url}\n{data}");

    let client = client::new_for_url(&url)?;
    match client
        .post(&url)
        .header("Content-Type", content_type.to_http_content_type())
        .body(data)
        .send()
        .await
    {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<JsonDataFrameRowResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => {
                    let err = format!("api::staging::modify_df error parsing response from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::modify_df Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::config::UserConfig;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::entry::mod_entry::ModType;
    use crate::model::ContentType;
    use crate::test;
    use crate::{api, command};

    use std::path::Path;

    #[tokio::test]
    async fn test_stage_row_on_dataframe_json() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let identifier = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let path = Path::new("annotations").join("train").join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            api::remote::staging::index_dataset(&remote_repo, branch_name, &identifier, &path).await?;
            let result =
                api::remote::staging::modify_df(
                    &remote_repo,
                    branch_name,
                    &identifier,
                    &path,
                    data.to_string(),
                    ContentType::Json,
                    ModType::Append
                ).await;

            assert!(result.is_ok());
            println!("{:?}", result.unwrap());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_should_not_stage_invalid_schema_for_dataframe() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);
            let identifier = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let data = "{\"id\": 1, \"name\": \"greg\"}";
            let result = api::remote::staging::modify_df(
                &remote_repo,
                branch_name,
                &identifier,
                &path,
                data.to_string(),
                ContentType::Json,
                ModType::Append,
            )
            .await;

            assert!(result.is_err());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_stage_row_on_dataframe_csv() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);
            let identifier = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let data = "image1.jpg, dog, 13, 14, 100, 100";
            api::remote::staging::index_dataset(&remote_repo, branch_name, &identifier, &path)
                .await?;
            let result = api::remote::staging::modify_df(
                &remote_repo,
                branch_name,
                &identifier,
                &path,
                data.to_string(),
                ContentType::Csv,
                ModType::Append,
            )
            .await;

            assert!(result.is_ok());
            println!("{:?}", result.unwrap());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_status_modified_dataframe() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let identifier = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            api::remote::staging::index_dataset(
                &remote_repo,
                branch_name,
                &identifier,
                &path,
            ).await?;
            api::remote::staging::modify_df(
                &remote_repo,
                branch_name,
                &identifier,
                &path,
                data.to_string(),
                ContentType::Json,
                ModType::Append
            ).await?;

            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let entries = api::remote::staging::status(
                &remote_repo,
                branch_name,
                &identifier,
                &directory,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.modified_files.entries.len(), 1);
            assert_eq!(entries.modified_files.total_entries, 1);

            Ok(remote_repo)
        })
        .await
    }
}
