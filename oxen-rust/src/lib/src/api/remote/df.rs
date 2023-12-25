use std::path::Path;

use crate::api;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::opts::DFOpts;
use crate::view::JsonDataFrameViewResponse;

use super::client;

pub async fn get(
    remote_repo: &RemoteRepository,
    commit_or_branch: &str,
    path: impl AsRef<Path>,
    opts: DFOpts,
) -> Result<JsonDataFrameViewResponse, OxenError> {
    let path_str = path.as_ref().to_str().unwrap();
    let query_str = opts.to_http_query_params();
    let uri = format!("/data_frame/{commit_or_branch}/{path_str}?{query_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {}", body);
            let response: Result<JsonDataFrameViewResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got JsonDataFrameViewResponse: {:?}", val);
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

    use serde_json::json;

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
            let schema_metadata = json!({
                "description": "A dataset of faces",
                "task": "gen_faces"
            });
            let column_name = "image_id".to_string();
            let column_metadata = json!({
                "root": "images"
            });
            command::schemas::add_column_metadata(
                &local_repo,
                schema_ref,
                &column_name,
                &column_metadata,
            )?;
            command::schemas::add_schema_metadata(&local_repo, schema_ref, &schema_metadata)?;
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
            let df = api::remote::df::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "large_files/test.csv",
                opts,
            )
            .await?;
            assert_eq!(df.data_frame.source.size.height, 200_000);
            assert_eq!(df.data_frame.source.size.width, 11);

            assert_eq!(df.data_frame.view.pagination.page_number, 1);
            assert_eq!(df.data_frame.view.pagination.page_size, 10);
            assert_eq!(df.data_frame.view.pagination.total_entries, 200_000);
            assert_eq!(df.data_frame.view.pagination.total_pages, 20_000);

            assert_eq!(df.data_frame.view.data.as_array().unwrap().len(), 10);

            // check source schema
            assert_eq!(
                df.data_frame.source.schema.metadata,
                Some(schema_metadata.to_owned())
            );
            assert_eq!(
                df.data_frame.source.schema.fields[0].metadata,
                Some(column_metadata.to_owned())
            );

            // check view schema
            assert_eq!(df.data_frame.view.schema.metadata, Some(schema_metadata));
            assert_eq!(
                df.data_frame.view.schema.fields[0].metadata,
                Some(column_metadata)
            );

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
            let df = api::remote::df::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "large_files/test.csv",
                opts,
            )
            .await?;
            assert_eq!(df.data_frame.source.size.height, 200_000);
            assert_eq!(df.data_frame.source.size.width, 11);

            assert_eq!(df.data_frame.view.pagination.page_number, 1);
            assert_eq!(df.data_frame.view.pagination.page_size, 10);
            assert_eq!(df.data_frame.view.pagination.total_entries, 200_000);
            assert_eq!(df.data_frame.view.pagination.total_pages, 20_000);

            assert_eq!(df.data_frame.view.data.as_array().unwrap().len(), 10);

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
            let df = api::remote::df::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "large_files/test.csv",
                opts,
            )
            .await?;

            assert_eq!(df.data_frame.source.size.height, 200_000);
            assert_eq!(df.data_frame.source.size.width, 11);

            assert_eq!(df.data_frame.view.pagination.page_number, 1);
            assert_eq!(df.data_frame.view.pagination.page_size, 20);
            assert_eq!(df.data_frame.view.pagination.total_entries, 200_000);
            assert_eq!(df.data_frame.view.pagination.total_pages, 10_000);

            assert_eq!(df.data_frame.view.data.as_array().unwrap().len(), 20);

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
            let df = api::remote::df::get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "large_files/test.csv",
                opts,
            )
            .await?;
            assert_eq!(df.data_frame.source.size.height, 200_000);
            assert_eq!(df.data_frame.source.size.width, 11);

            assert_eq!(df.data_frame.view.size.height, 37_291);
            assert_eq!(df.data_frame.view.size.width, 11);

            assert_eq!(df.data_frame.view.pagination.page_number, 1);
            assert_eq!(df.data_frame.view.pagination.page_size, 100);
            assert_eq!(df.data_frame.view.pagination.total_entries, 37_291);
            assert_eq!(df.data_frame.view.pagination.total_pages, 373);

            assert_eq!(df.data_frame.view.data.as_array().unwrap().len(), 100);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_get_schema_df_on_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("csvs");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_csv_file_with_name("mixed_data_types.csv");
            util::fs::copy(from_file, &csv_file)?;

            // Add the file
            command::add(&local_repo, &csv_file)?;
            command::commit(&local_repo, "add test.csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Cannot get schema that does not exist
            let opts = DFOpts::empty();
            let result =
                api::remote::df::get(&remote_repo, DEFAULT_BRANCH_NAME, "csvs/test.csv", opts)
                    .await;
            assert!(result.is_err());

            // Push the repo
            command::push(&local_repo).await?;

            // Create a new branch
            let branch_name = "new_branch";
            command::create_checkout(&local_repo, branch_name)?;

            // Add some metadata to the schema
            /*
            prompt,response,is_correct,response_time,difficulty
            who is it?,issa me,true,0.5,1
            */
            let schema_ref = "csvs/test.csv";
            let schema_metadata = json!({
                "task": "chat_bot",
                "description": "some generic description",
            });

            let column_name = "difficulty".to_string();
            let column_metadata = json!(
                {
                    "values": [0, 1, 2]
                }
            );
            command::schemas::add_schema_metadata(&local_repo, schema_ref, &schema_metadata)?;
            command::schemas::add_column_metadata(
                &local_repo,
                schema_ref,
                &column_name,
                &column_metadata,
            )?;

            command::commit(&local_repo, "add test.csv schema metadata")?;

            // Cannot get schema that does not exist
            let opts = DFOpts::empty();
            let result =
                api::remote::df::get(&remote_repo, branch_name, "csvs/test.csv", opts).await;
            assert!(result.is_err());

            // Push the repo
            command::push(&local_repo).await?;

            // List the one schema
            let opts = DFOpts::empty();
            let results =
                api::remote::df::get(&remote_repo, branch_name, "csvs/test.csv", opts).await;
            assert!(results.is_ok());

            let result = results.unwrap();
            let schema = result.data_frame.source.schema;

            // prompt,response,is_correct,response_time,difficulty
            assert_eq!(schema.fields.len(), 5);
            assert_eq!(schema.fields[0].name, "prompt");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "response");
            assert_eq!(schema.fields[1].dtype, "str");
            assert_eq!(schema.fields[2].name, "is_correct");
            assert_eq!(schema.fields[2].dtype, "bool");
            assert_eq!(schema.fields[3].name, "response_time");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[4].name, "difficulty");
            assert_eq!(schema.fields[4].dtype, "i64");

            // Check the metadata
            assert_eq!(schema.metadata, Some(schema_metadata));
            assert_eq!(schema.fields[4].metadata, Some(column_metadata));

            Ok(())
        })
        .await
    }

    // Tests page=4 page_size=6 for data/test/parquet/wiki_1k.parquet file
    #[tokio::test]
    async fn test_paginate_remote_parquet() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("data");
            std::fs::create_dir_all(&large_dir)?;
            let test_file = large_dir.join("test.parquet");
            let from_file = test::test_10k_parquet();
            util::fs::copy(from_file, &test_file)?;

            command::add(&local_repo, &test_file)?;
            command::commit(&local_repo, "add test.parquet")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push the repo
            command::push(&local_repo).await?;

            // Get the df
            let mut opts = DFOpts::empty();
            opts.page = Some(4);
            opts.page_size = Some(5);
            opts.columns = Some("id,title,url".to_string());
            let df =
                api::remote::df::get(&remote_repo, DEFAULT_BRANCH_NAME, "data/test.parquet", opts)
                    .await?;

            let p_df = df.data_frame.view.to_df();
            println!("{:?}", p_df);

            // Original DF
            assert_eq!(df.data_frame.source.size.height, 10_000);
            assert_eq!(df.data_frame.source.size.width, 4);

            // View DF
            assert_eq!(df.data_frame.view.size.height, 5);
            assert_eq!(df.data_frame.view.size.width, 3);

            assert_eq!(df.data_frame.view.pagination.page_number, 4);
            assert_eq!(df.data_frame.view.pagination.page_size, 5);
            assert_eq!(df.data_frame.view.pagination.total_entries, 10_000);
            assert_eq!(df.data_frame.view.pagination.total_pages, 2000);

            assert_eq!(df.data_frame.view.data.as_array().unwrap().len(), 5);

            println!("{}", df.data_frame.view.data[0]["title"]);
            assert_eq!(df.data_frame.view.data[0]["title"], "Ayn Rand");

            Ok(())
        })
        .await
    }

    // Test slice=330..333 for data/test/parquet/wiki_1k.parquet file
    #[tokio::test]
    async fn test_slice_remote_parquet() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("data");
            std::fs::create_dir_all(&large_dir)?;
            let test_file = large_dir.join("test.parquet");
            let from_file = test::test_10k_parquet();
            util::fs::copy(from_file, &test_file)?;

            command::add(&local_repo, &test_file)?;
            command::commit(&local_repo, "add test.parquet")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push the repo
            command::push(&local_repo).await?;

            // Get the df
            let mut opts = DFOpts::empty();
            opts.slice = Some("330..333".to_string());
            opts.columns = Some("id,title".to_string());
            let df =
                api::remote::df::get(&remote_repo, DEFAULT_BRANCH_NAME, "data/test.parquet", opts)
                    .await?;

            let p_df = df.data_frame.view.to_df();
            println!("{:?}", p_df);

            // Original DF
            assert_eq!(df.data_frame.source.size.height, 10_000);
            assert_eq!(df.data_frame.source.size.width, 4);

            // View DF
            assert_eq!(df.data_frame.view.size.height, 3);
            assert_eq!(df.data_frame.view.size.width, 2);

            assert_eq!(df.data_frame.view.data.as_array().unwrap().len(), 3);

            println!("{}", df.data_frame.view.data[0]["title"]);
            assert_eq!(df.data_frame.view.data[0]["title"], "April 26");

            Ok(())
        })
        .await
    }
}
