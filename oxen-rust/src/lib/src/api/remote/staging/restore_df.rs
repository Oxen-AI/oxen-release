use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;

use std::path::Path;

pub async fn restore_df(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let file_name = path.as_ref().to_string_lossy();
    let uri = format!("/staging/{identifier}/modifications/{branch_name}/{file_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("restore_df {}", url);
    let client = client::new_for_url(&url)?;
    match client.delete(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("restore_df got body: {}", body);
            Ok(())
        }
        Err(err) => {
            let err = format!("restore_df Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};
    use crate::error::OxenError;
    use crate::model::diff::DiffResult;
    use crate::model::entry::mod_entry::ModType;
    use crate::model::ContentType;

    use crate::api;
    use crate::test;

    use std::path::Path;

    #[tokio::test]
    async fn test_restore_modified_dataframe() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let identifier = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::remote::staging::dataset::index_dataset(&remote_repo, branch_name,&identifier, &path).await?;

            let result_1 = api::remote::staging::modify_df(
                    &remote_repo,
                    branch_name,
                    &identifier,
                    &path,
                    data.to_string(),
                    ContentType::Json,
                    ModType::Append
                ).await;
            assert!(result_1.is_ok());

            let data = "{\"file\":\"image2.jpg\", \"label\": \"cat\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            let result_2 = api::remote::staging::modify_df(
                    &remote_repo,
                    branch_name,
                    &identifier,
                    &path,
                    data.to_string(),
                    ContentType::Json,
                    ModType::Append
                ).await;
            assert!(result_2.is_ok());

            // Make sure both got staged
            let diff = api::remote::staging::diff(
                &remote_repo,
                branch_name,
                &identifier,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                }
                _ => assert!(false, "Expected tabular diff result"),
            }
            // Delete result_2
            let result_delete = api::remote::staging::restore_df(
                &remote_repo,
                branch_name,
                &identifier,
                &path,
            ).await;
            assert!(result_delete.is_ok());

            // Should be cleared
            let diff = api::remote::staging::diff(
                &remote_repo,
                branch_name,
                &identifier,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 0);
                }
                _ => assert!(false, "Expected tabular diff result."),
            }

            Ok(remote_repo)
        })
        .await
    }
}
