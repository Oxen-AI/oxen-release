use std::path::Path;

use polars::frame::DataFrame;

use crate::api;
<<<<<<< HEAD
use crate::api::remote::client;
=======
use crate::api::client;
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
use crate::error::OxenError;
use crate::view::json_data_frame_view::JsonDataFrameColumnResponse;

use crate::model::RemoteRepository;

pub async fn create(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    data: String,
) -> Result<DataFrame, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };

    let uri = format!("/workspaces/{workspace_id}/data_frames/columns/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("modify_df {url}\n{data}");

    let client = client::new_for_url(&url)?;
    match client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await
    {
        Ok(res) => {
<<<<<<< HEAD
            let body = client::parse_json_body(&url, res).await?;
=======
            let body: String = client::parse_json_body(&url, res).await?;
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
            let response: Result<JsonDataFrameColumnResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.data_frame.view.to_df()),
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

pub async fn delete(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    column_name: &str,
) -> Result<DataFrame, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };

    let uri = format!(
        "/workspaces/{workspace_id}/data_frames/columns/{column_name}/resource/{file_path_str}"
    );

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.delete(&url).send().await {
        Ok(res) => {
<<<<<<< HEAD
            let body = client::parse_json_body(&url, res).await?;
=======
            let body: String = client::parse_json_body(&url, res).await?;
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
            log::debug!("rm_df_mod got body: {}", body);
            let response: Result<JsonDataFrameColumnResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val.data_frame.view.to_df()),
                Err(err) => {
                    let err = format!("api::staging::rm_df_mod error parsing response from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("rm_df_mod Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn update(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    column_name: &str,
    data: String,
) -> Result<JsonDataFrameColumnResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };

    let uri = format!(
        "/workspaces/{workspace_id}/data_frames/columns/{column_name}/resource/{file_path_str}"
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("update_column {url}\n{data}");

    let client = client::new_for_url(&url)?;
    match client
        .put(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await
    {
        Ok(res) => {
<<<<<<< HEAD
            let body = client::parse_json_body(&url, res).await?;
=======
            let body: String = client::parse_json_body(&url, res).await?;
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
            let response: Result<JsonDataFrameColumnResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => {
                    let err = format!("api::staging::update_row error parsing response from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::update_row Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::data_frame::schema::field::Changes;
    use crate::opts::DFOpts;
    use crate::test;

    use std::path::Path;

    #[tokio::test]
    async fn test_create_column_in_dataframe() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
<<<<<<< HEAD
            let branch = api::remote::branches::create_from_or_get(
=======
            let branch = api::client::branches::create_from_branch(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace =
<<<<<<< HEAD
                api::remote::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
=======
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
            assert_eq!(workspace.id, workspace_id);

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let column_name = "test";
            let data = format!(r#"{{"name":"{}", "data_type": "str"}}"#, column_name);

<<<<<<< HEAD
            api::remote::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let result = api::remote::workspaces::data_frames::columns::create(
=======
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let result = api::client::workspaces::data_frames::columns::create(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string(),
            )
            .await;

<<<<<<< HEAD
            let df = api::remote::workspaces::data_frames::get(
=======
            let df = api::client::workspaces::data_frames::get(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                &workspace_id,
                &path,
                DFOpts::empty(),
            )
            .await?;

            if !df
                .data_frame
                .unwrap()
                .view
                .schema
                .fields
                .iter()
                .any(|field| field.name == column_name)
            {
                panic!("Column {} does not exist in the data frame", column_name);
            }

            assert!(result.is_ok());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_column() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
<<<<<<< HEAD
            let branch = api::remote::branches::create_from_or_get(
=======
            let branch = api::client::branches::create_from_branch(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;

            // Path to the CSV file
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

<<<<<<< HEAD
            api::remote::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            api::remote::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let df = api::remote::workspaces::data_frames::get(
=======
            api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let df = api::client::workspaces::data_frames::get(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                &workspace_id,
                &path,
                DFOpts::empty(),
            )
            .await?;

            let binding = df.data_frame.unwrap();
            let column = binding.view.schema.fields.get(4).unwrap();

<<<<<<< HEAD
            api::remote::workspaces::data_frames::columns::delete(
=======
            api::client::workspaces::data_frames::columns::delete(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                &workspace_id,
                &path,
                &column.name,
            )
            .await?;

<<<<<<< HEAD
            let df = api::remote::workspaces::data_frames::get(
=======
            let df = api::client::workspaces::data_frames::get(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                &workspace_id,
                &path,
                DFOpts::empty(),
            )
            .await?;

            if let Some((_index, field)) = df
                .data_frame
                .unwrap()
                .view
                .schema
                .fields
                .iter()
                .enumerate()
                .find(|(_index, field)| field.name == column.name)
            {
                if <std::option::Option<Changes> as Clone>::clone(&field.changes)
                    .unwrap()
                    .status
                    != "deleted"
                {
                    panic!("Column {} still exists in the data frame", column.name);
                }
            }

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_update_column() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
<<<<<<< HEAD
            let branch = api::remote::branches::create_from_or_get(
=======
            let branch = api::client::branches::create_from_branch(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let workspace =
<<<<<<< HEAD
                api::remote::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
=======
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
            assert_eq!(workspace.id, workspace_id);

            // Path to the CSV file
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

<<<<<<< HEAD
            api::remote::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let df = api::remote::workspaces::data_frames::get(
=======
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let df = api::client::workspaces::data_frames::get(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                &workspace_id,
                &path,
                DFOpts::empty(),
            )
            .await?;

            // Extract the first _oxen_row_id from the data frame
            let binding = df.data_frame.unwrap();
            let column = binding.view.schema.fields.get(4).unwrap();

            let data: &str = "{\"new_name\":\"files\"}";

<<<<<<< HEAD
            api::remote::workspaces::data_frames::columns::update(
=======
            api::client::workspaces::data_frames::columns::update(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                &workspace_id,
                &path,
                &column.name,
                data.to_string(),
            )
            .await?;

<<<<<<< HEAD
            let df = api::remote::workspaces::data_frames::get(
=======
            let df = api::client::workspaces::data_frames::get(
>>>>>>> 4c21262a2e9d28d1e55e4aeabc878c751c583ae1
                &remote_repo,
                &workspace_id,
                &path,
                DFOpts::empty(),
            )
            .await?;

            if !df
                .data_frame
                .unwrap()
                .view
                .schema
                .fields
                .iter()
                .any(|field| field.name == "files")
            {
                panic!("Column {} doesn't exist in the data frame", "files");
            }

            Ok(remote_repo)
        })
        .await
    }
}
