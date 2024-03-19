use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::Schema;
use crate::model::{DataFrameDiff, RemoteRepository};
use crate::view::ListStagedFileModResponseDF;

use std::path::Path;

pub async fn diff(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    path: impl AsRef<Path>,
    page: usize,
    page_size: usize,
) -> Result<DataFrameDiff, OxenError> {
    let path_str = path.as_ref().to_str().unwrap();
    let uri = format!(
        "/staging/{identifier}/diff/{branch_name}/{path_str}?page={page}&page_size={page_size}"
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("diff got body: {}", body);
            let response: Result<ListStagedFileModResponseDF, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    let mods = val.modifications;
                    let added_rows = mods.added_rows.map(|added| added.to_df());
                    let schema = Schema::from_polars(&added_rows.as_ref().unwrap().schema());

                    Ok(DataFrameDiff {
                        head_schema: Some(schema.clone()),
                        base_schema: Some(schema),
                        added_rows,
                        removed_rows: None,
                        added_cols: None,
                        removed_cols: None,
                    })
                }
                Err(err) => Err(OxenError::basic_str(format!(
                    "api::staging::diff error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("api::staging::diff Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};
    use crate::error::OxenError;
    use crate::model::entry::mod_entry::ModType;
    use crate::model::ContentType;
    use crate::test;

    use std::path::Path;

    #[tokio::test]
    async fn test_diff_modified_dataframe() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let identifier = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            api::remote::staging::modify_df(
                &remote_repo,
                branch_name,
                &identifier,
                &path,
                data.to_string(),
                ContentType::Json,
                ModType::Append
            ).await?;

            let diff = api::remote::staging::diff(
                &remote_repo,
                branch_name,
                &identifier,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            let added_rows = diff.added_rows.unwrap();
            assert_eq!(added_rows.height(), 1);
            assert_eq!(added_rows.width(), 7); // 6+1 for _id

            Ok(remote_repo)
        })
        .await
    }

    // #[tokio::test]
    // async fn test_diff_delete_row_from_modified_dataframe() -> Result<(), OxenError> {
    //     test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
    //         let branch_name = "add-images";
    //         let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
    //         assert_eq!(branch.name, branch_name);
    //         let identifier = UserConfig::identifier()?;

    //         // train/dog_1.jpg,dog,101.5,32.0,385,330
    //         let directory = Path::new("annotations").join("train");
    //         let path = directory.join("bounding_box.csv");
    //         let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
    //         let result_1 = api::remote::staging::modify_df(
    //                 &remote_repo,
    //                 branch_name,
    //                 &identifier,
    //                 &path,
    //                 data.to_string(),
    //                 ContentType::Json,
    //                 ModType::Append
    //             ).await;
    //         assert!(result_1.is_ok());

    //         let data = "{\"file\":\"image2.jpg\", \"label\": \"cat\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
    //         let result_2 = api::remote::staging::modify_df(
    //                 &remote_repo,
    //                 branch_name,
    //                 &identifier,
    //                 &path,
    //                 data.to_string(),
    //                 ContentType::Json,
    //                 ModType::Append
    //             ).await?;

    //         // Make sure both got staged
    //         let diff = api::remote::staging::diff(
    //             &remote_repo,
    //             branch_name,
    //             &identifier,
    //             &path,
    //             DEFAULT_PAGE_NUM,
    //             DEFAULT_PAGE_SIZE
    //         ).await?;
    //         let added_rows = diff.added_rows.unwrap();
    //         assert_eq!(added_rows.height(), 2);

    //         // Delete result_2
    //         let result_delete = api::remote::staging::rm_df_mod(
    //             &remote_repo,
    //             branch_name,
    //             &identifier,
    //             &path,
    //             &result_2.uuid
    //         ).await;
    //         assert!(result_delete.is_ok());

    //         // Make there is only one left
    //         let diff = api::remote::staging::diff(
    //             &remote_repo,
    //             branch_name,
    //             &identifier,
    //             &path,
    //             DEFAULT_PAGE_NUM,
    //             DEFAULT_PAGE_SIZE
    //         ).await?;
    //         let added_rows = diff.added_rows.unwrap();
    //         assert_eq!(added_rows.height(), 1);

    //         Ok(remote_repo)
    //     })
    //     .await
    // }
}
