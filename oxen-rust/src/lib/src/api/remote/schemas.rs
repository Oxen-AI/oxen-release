//! # Remote Schemas
//!
//! Interact with remote schemas.
//!

use std::path::Path;

use crate::api;
use crate::error::OxenError;
use crate::model::{RemoteRepository, Schema};
use crate::view::{ListSchemaResponse, SchemaResponse};

use super::client;

pub async fn list(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
) -> Result<Vec<Schema>, OxenError> {
    let revision = revision.as_ref();

    let uri = format!("/schemas/{revision}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {}", body);
            let response: Result<ListSchemaResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got ListSchemaResponse: {:?}", val);
                    Ok(val.schemas)
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

pub async fn get(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<Schema, OxenError> {
    let revision = revision.as_ref();
    let path = path.as_ref();

    let uri = format!("/schemas/{revision}/{}", path.to_string_lossy());
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {}", body);
            let response: Result<SchemaResponse, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got SchemaResponse: {:?}", val);
                    Ok(val.schema)
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

    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_remote_list_schemas() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("csvs");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_csv_file_with_name("mixed_data_types.csv");
            util::fs::copy(from_file, &csv_file)?;

            command::add(&local_repo, &csv_file)?;
            command::commit(&local_repo, "add test.csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // List no schemas
            let schemas = api::remote::schemas::list(&remote_repo, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(schemas.len(), 0);

            // Push the repo
            command::push(&local_repo).await?;

            // List the one schema
            let schemas = api::remote::schemas::list(&remote_repo, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(schemas.len(), 1);

            // prompt,response,is_correct,response_time,difficulty
            let schema = &schemas[0];
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

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_get_schema() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("csvs");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_csv_file_with_name("mixed_data_types.csv");
            util::fs::copy(from_file, &csv_file)?;

            command::add(&local_repo, &csv_file)?;
            command::commit(&local_repo, "add test.csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Cannot get schema that does not exist
            let result =
                api::remote::schemas::get(&remote_repo, DEFAULT_BRANCH_NAME, "csvs/test.csv").await;
            assert!(result.is_err());

            // Push the repo
            command::push(&local_repo).await?;

            // List the one schema
            let schema =
                api::remote::schemas::get(&remote_repo, DEFAULT_BRANCH_NAME, "csvs/test.csv")
                    .await?;

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

            Ok(())
        })
        .await
    }
}
