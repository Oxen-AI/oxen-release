//! # Remote Schemas
//!
//! Interact with remote schemas.
//!

use std::path::Path;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::ListSchemaResponse;
use crate::view::schema::SchemaWithPath;

pub async fn list(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
) -> Result<Vec<SchemaWithPath>, OxenError> {
    let revision = revision.as_ref();

    let uri = format!("/schemas/{revision}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {body}");
            let response: Result<ListSchemaResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got ListSchemaResponse: {val:?}");
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
) -> Result<Option<SchemaWithPath>, OxenError> {
    let revision = revision.as_ref();
    let path = path.as_ref();

    let uri = format!("/schemas/{revision}/{}", path.to_string_lossy());
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {body}");
            let response: Result<ListSchemaResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got SchemaResponse: {val:?}");
                    if !val.schemas.is_empty() {
                        Ok(val.schemas.into_iter().next())
                    } else {
                        Ok(None)
                    }
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
    use super::{get, list};
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::Schema;
    use crate::repositories;
    use crate::repositories::data_frames::schemas;
    use crate::test;
    use crate::util;

    use std::path::PathBuf;

    use serde_json::json;

    /// Asserts the columns of `mixed_data_types.csv`: prompt,response,is_correct,response_time,difficulty
    fn assert_mixed_data_types(schema: &Schema) {
        let fields: Vec<(&str, &str)> = schema
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field.dtype.as_str()))
            .collect();
        assert_eq!(
            fields,
            [
                ("prompt", "str"),
                ("response", "str"),
                ("is_correct", "bool"),
                ("response_time", "f64"),
                ("difficulty", "i64"),
            ]
        );
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_schemas() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|mut local_repo| async move {
            let remote_repo = test::connect_remote_repo(&mut local_repo).await?;
            let schema_ref = &PathBuf::from("csvs")
                .join("test.csv")
                .to_string_lossy()
                .to_string();
            let result = get(&remote_repo, DEFAULT_BRANCH_NAME, schema_ref).await;
            assert!(
                result.is_err(),
                "a branch the remote does not have yet is not found"
            );

            repositories::push(&local_repo).await?;
            assert_eq!(
                list(&remote_repo, DEFAULT_BRANCH_NAME).await?.len(),
                0,
                "a repo without tabular files has no schemas"
            );

            let csvs_dir = local_repo.path.join("csvs");
            util::fs::create_dir_all(&csvs_dir)?;
            let csv_file = csvs_dir.join("test.csv");
            util::fs::copy(
                test::test_csv_file_with_name("mixed_data_types.csv"),
                &csv_file,
            )?;
            repositories::add(&local_repo, &csv_file).await?;
            repositories::commit(&local_repo, "add test.csv")?;

            let result = get(&remote_repo, DEFAULT_BRANCH_NAME, schema_ref).await;
            assert!(
                result.is_err(),
                "a schema that is not pushed yet is not found"
            );

            repositories::push(&local_repo).await?;
            let schemas = list(&remote_repo, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(schemas.len(), 1);
            assert_mixed_data_types(&schemas[0].schema);

            // Metadata committed on a branch reads back on that branch.
            let branch_name = "new_branch";
            repositories::branches::create_checkout(&local_repo, branch_name)?;
            let schema_metadata = json!({
                "task": "chat_bot",
                "description": "some generic description",
            });
            let column_metadata = json!({ "values": [0, 1, 2] });
            schemas::add_schema_metadata(&local_repo, schema_ref, &schema_metadata)?;
            schemas::add_column_metadata(&local_repo, schema_ref, "difficulty", &column_metadata)?;
            repositories::commit(&local_repo, "add test.csv schema metadata")?;
            repositories::push(&local_repo).await?;

            let schema = get(&remote_repo, branch_name, schema_ref)
                .await?
                .unwrap()
                .schema;
            assert_mixed_data_types(&schema);
            assert_eq!(schema.metadata, Some(schema_metadata));
            assert_eq!(schema.fields[4].metadata, Some(column_metadata));

            let on_main = get(&remote_repo, DEFAULT_BRANCH_NAME, schema_ref)
                .await?
                .unwrap()
                .schema;
            assert_eq!(
                on_main.metadata, None,
                "the branch's metadata stays off main"
            );

            Ok(())
        })
        .await
    }
}
