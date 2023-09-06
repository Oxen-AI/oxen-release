use std::path::Path;

use crate::api;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::opts::DFOpts;
use crate::view::JsonDataFrameSliceResponse;

use super::client;

pub async fn show(
    remote_repo: &RemoteRepository,
    commit_or_branch: &str,
    path: impl AsRef<Path>,
    opts: DFOpts,
) -> Result<JsonDataFrameSliceResponse, OxenError> {
    let path_str = path.as_ref().to_str().unwrap();
    let query_str = opts.to_http_query_params();
    let uri = format!("/df/{commit_or_branch}/{path_str}?{query_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {}", body);
            let response: Result<JsonDataFrameSliceResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got JsonDataFrameSliceResponse: {:?}", val);
                    Ok(val)
                }
                Err(err) => Err(OxenError::basic_str(format!(
                    "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::command;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::error::OxenError;

    use crate::opts::DFOpts;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_fetch_schema_metadata() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("large_files");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_200k_csv();
            util::fs::copy(from_file, &csv_file)?;

            command::add(&local_repo, &csv_file)?;
            command::commit(&local_repo, "add test.csv")?;

            // Add some metadata to the schema
            /*
shape: (200_000, 11)
┌────────────┬───────────┬───────────┬────────────┬───┬─────────────┬─────────────┬──────────────┬──────────────┐
│ image_id   ┆ lefteye_x ┆ lefteye_y ┆ righteye_x ┆ … ┆ leftmouth_x ┆ leftmouth_y ┆ rightmouth_x ┆ rightmouth_y │
│ ---        ┆ ---       ┆ ---       ┆ ---        ┆   ┆ ---         ┆ ---         ┆ ---          ┆ ---          │
│ str        ┆ i64       ┆ i64       ┆ i64        ┆   ┆ i64         ┆ i64         ┆ i64          ┆ i64          │
╞════════════╪═══════════╪═══════════╪════════════╪═══╪═════════════╪═════════════╪══════════════╪══════════════╡
│ 000001.jpg ┆ 69        ┆ 109       ┆ 106        ┆ … ┆ 73          ┆ 152         ┆ 108          ┆ 154          │
│ 000002.jpg ┆ 69        ┆ 110       ┆ 107        ┆ … ┆ 70          ┆ 151         ┆ 108          ┆ 153          │
│ 000003.jpg ┆ 76        ┆ 112       ┆ 104        ┆ … ┆ 74          ┆ 156         ┆ 98           ┆ 158          │
│ 000004.jpg ┆ 72        ┆ 113       ┆ 108        ┆ … ┆ 71          ┆ 155         ┆ 101          ┆ 151          │
│ …          ┆ …         ┆ …         ┆ …          ┆ … ┆ …           ┆ …           ┆ …            ┆ …            │
│ 199997.jpg ┆ 70        ┆ 110       ┆ 106        ┆ … ┆ 69          ┆ 151         ┆ 110          ┆ 154          │
│ 199998.jpg ┆ 68        ┆ 112       ┆ 109        ┆ … ┆ 67          ┆ 151         ┆ 110          ┆ 151          │
│ 199999.jpg ┆ 70        ┆ 111       ┆ 108        ┆ … ┆ 71          ┆ 152         ┆ 105          ┆ 153          │
│ 200000.jpg ┆ 69        ┆ 112       ┆ 108        ┆ … ┆ 74          ┆ 151         ┆ 103          ┆ 152          │
└────────────┴───────────┴───────────┴────────────┴───┴─────────────┴─────────────┴──────────────┴──────────────┘
            */

            // Add some metadata to the schema
            let schema_ref = "large_files/test.csv";
            let schema_metadata = "{\"task\": \"gen_faces\", \"description\": \"generate some faces\"}".to_string();
            let column_name = "image_id".to_string();
            let column_metadata = "{\"root\": \"images\"}".to_string();
            command::schemas::add_column_metadata(&local_repo, &schema_ref, &column_name, &column_metadata)?;
            command::schemas::add_column_overrides(&local_repo, &csv_file, "image_id:path")?;
            command::schemas::add_schema_metadata(&local_repo, &schema_ref, &schema_metadata)?;
            command::commit(&local_repo, "add test.csv schema metadata")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push the repo
            command::push(&local_repo).await?;

            // Get the df
            let mut opts = DFOpts::empty();
            opts.page_size = Some(10);
            let df = api::remote::df::show(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "large_files/test.csv",
                opts,
            )
            .await?;
            assert_eq!(df.full_size.height, 200_000);
            assert_eq!(df.full_size.width, 11);

            assert_eq!(df.page_number, 1);
            assert_eq!(df.page_size, 10);
            assert_eq!(df.total_entries, 200_000);
            assert_eq!(df.total_pages, 20_000);

            assert_eq!(df.df.data.as_array().unwrap().len(), 10);

            // check schema
            assert_eq!(df.df.schema.metadata, Some(schema_metadata));
            assert_eq!(df.df.schema.fields[0].dtype_override, Some("path".to_string()));
            assert_eq!(df.df.schema.fields[0].metadata, Some(column_metadata));


            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_paginate_df_page_one() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("large_files");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_200k_csv();
            util::fs::copy(from_file, &csv_file)?;

            command::add(&local_repo, &csv_file)?;
            command::commit(&local_repo, "add test.csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push the repo
            command::push(&local_repo).await?;

            // Get the df
            let mut opts = DFOpts::empty();
            opts.page_size = Some(10);
            let df = api::remote::df::show(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "large_files/test.csv",
                opts,
            )
            .await?;
            assert_eq!(df.full_size.height, 200_000);
            assert_eq!(df.full_size.width, 11);

            assert_eq!(df.page_number, 1);
            assert_eq!(df.page_size, 10);
            assert_eq!(df.total_entries, 200_000);
            assert_eq!(df.total_pages, 20_000);

            assert_eq!(df.df.data.as_array().unwrap().len(), 10);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_paginate_df_page_1_page_size_20() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("large_files");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_200k_csv();
            util::fs::copy(from_file, &csv_file)?;

            command::add(&local_repo, &csv_file)?;
            command::commit(&local_repo, "add test.csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push the repo
            command::push(&local_repo).await?;

            // Get the df
            let mut opts = DFOpts::empty();
            opts.page = Some(1);
            opts.page_size = Some(20);
            let df = api::remote::df::show(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "large_files/test.csv",
                opts,
            )
            .await?;

            assert_eq!(df.full_size.height, 200_000);
            assert_eq!(df.full_size.width, 11);

            assert_eq!(df.page_number, 1);
            assert_eq!(df.page_size, 20);
            assert_eq!(df.total_entries, 200_000);
            assert_eq!(df.total_pages, 10_000);

            assert_eq!(df.df.data.as_array().unwrap().len(), 20);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_paginate_df_after_filter() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("large_files");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_200k_csv();
            util::fs::copy(from_file, &csv_file)?;

            command::add(&local_repo, &csv_file)?;
            command::commit(&local_repo, "add test.csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push the repo
            command::push(&local_repo).await?;

            // Get the df
            let mut opts = DFOpts::empty();
            opts.page_size = Some(100);
            opts.filter = Some("lefteye_x > 70".to_string());
            let df = api::remote::df::show(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "large_files/test.csv",
                opts,
            )
            .await?;
            assert_eq!(df.full_size.height, 200_000);
            assert_eq!(df.full_size.width, 11);

            assert_eq!(df.slice_size.height, 37_291);
            assert_eq!(df.slice_size.width, 11);

            assert_eq!(df.page_number, 1);
            assert_eq!(df.page_size, 100);
            assert_eq!(df.total_entries, 37_291);
            assert_eq!(df.total_pages, 373);

            assert_eq!(df.df.data.as_array().unwrap().len(), 100);

            Ok(())
        })
        .await
    }
}
