use crate::api;
use crate::api::remote::client;
use crate::error::OxenError;
use crate::model::diff::tabular_diff::{
    TabularDiffMods, TabularDiffParameters, TabularDiffSchemas, TabularDiffSummary,
    TabularSchemaDiff,
};
// use crate::model::diff::tabular_diff_summary::{TabularDiffSummaryImpl};
use crate::model::diff::{AddRemoveModifyCounts, DiffResult, TabularDiff};
use crate::model::RemoteRepository;
use crate::model::Schema;
use crate::view::compare::{CompareTabularMods, CompareTabularResponseWithDF};

use std::path::Path;

pub async fn diff(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
    page: usize,
    page_size: usize,
) -> Result<DiffResult, OxenError> {
    let path_str = path.as_ref().to_str().unwrap();
    log::debug!("sending this workspace_id to diff: {}", workspace_id);
    let uri =
        format!("/workspaces/{workspace_id}/diff/{path_str}?page={page}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("diff got body: {}", body);
            let response: Result<CompareTabularResponseWithDF, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(ct) => {
                    // Get df from the json view
                    let df = ct.data.view.to_df();
                    let schema = Schema::from_polars(&df.schema().clone());
                    let schema_diff = match ct.dfs.schema_diff {
                        Some(diff) => diff.to_tabular_schema_diff(),
                        None => TabularSchemaDiff::default(),
                    };

                    let mods = match ct.dfs.summary {
                        Some(summary) => summary.modifications,
                        None => CompareTabularMods::default(),
                    };

                    let schemas = TabularDiffSchemas {
                        left: ct.dfs.source_schemas.left,
                        right: ct.dfs.source_schemas.right,
                        diff: schema,
                    };

                    let summary = TabularDiffSummary {
                        modifications: TabularDiffMods {
                            row_counts: AddRemoveModifyCounts {
                                added: mods.added_rows,
                                removed: mods.removed_rows,
                                modified: mods.modified_rows,
                            },
                            col_changes: schema_diff,
                        },
                        schemas,
                        dupes: ct.dfs.dupes.to_tabular_diff_dupes(),
                    };

                    let tdiff = TabularDiff {
                        summary,
                        contents: df,
                        parameters: TabularDiffParameters {
                            keys: ct
                                .dfs
                                .keys
                                .unwrap_or_default()
                                .iter()
                                .map(|k| k.as_string())
                                .collect(),
                            targets: ct
                                .dfs
                                .targets
                                .unwrap_or_default()
                                .iter()
                                .map(|t| t.to_string())
                                .collect::<Result<Vec<String>, OxenError>>()?,
                            display: ct
                                .dfs
                                .display
                                .unwrap_or_default()
                                .iter()
                                .map(|d| d.to_string())
                                .collect::<Result<Vec<String>, OxenError>>()?,
                        },
                    };
                    Ok(DiffResult::Tabular(tdiff))
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
    use crate::model::diff::DiffResult;
    use crate::test;

    use std::path::Path;

    #[tokio::test]
    async fn test_diff_modified_dataframe() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::remote::workspaces::data_frames::index(
                &remote_repo,
                &workspace_id,
                &path
            ).await?;

            api::remote::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string()
            ).await?;

            let diff = api::remote::workspaces::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                },
                _ => panic!("Diff result is not of tabular type."),
            }


            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_delete_row_from_modified_dataframe() -> Result<(), OxenError> {
        // Skip if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::remote::branches::create_from_or_get(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::remote::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let (_df_1, _row_id_1) = api::remote::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string()
                ).await?;

            let data = "{\"file\":\"image2.jpg\", \"label\": \"cat\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            let (_df_2, row_id_2) = api::remote::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string(),
                ).await?;

            // Make sure both got staged
            let diff = api::remote::workspaces::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                },
                _ => panic!("Diff result is not of tabular type."),
            }


            let uuid_2 = row_id_2.unwrap();
            // Delete result_2
            let result_delete = api::remote::workspaces::data_frames::rows::delete(
                &remote_repo,
                &workspace_id,
                &path,
                &uuid_2
            ).await;
            assert!(result_delete.is_ok());

            // Make there is only one left
            let diff = api::remote::workspaces::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                },
                _ => panic!("Diff result is not of tabular type."),
            }

            Ok(remote_repo)
        })
        .await
    }
}
