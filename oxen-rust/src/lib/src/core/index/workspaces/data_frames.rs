use duckdb::Connection;
use polars::frame::DataFrame;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use sql_query_builder::{Delete, Select};

use crate::api;
use crate::constants::{DIFF_HASH_COL, DIFF_STATUS_COL, OXEN_COLS, TABLE_NAME};
use crate::constants::{MODS_DIR, OXEN_HIDDEN_DIR, WORKSPACES_DIR};
use crate::core::db::staged_df_db::select_cols_from_schema;
use crate::core::db::{self, df_db, staged_df_db, str_json_db};
use crate::core::df::{sql, tabular};
use crate::core::index::workspaces;
use crate::core::index::CommitEntryReader;
use crate::model::diff::tabular_diff::{
    TabularDiffDupes, TabularDiffMods, TabularDiffParameters, TabularDiffSchemas,
    TabularDiffSummary, TabularSchemaDiff,
};
use crate::model::diff::{AddRemoveModifyCounts, DiffResult, TabularDiff};

use crate::model::staged_row_status::StagedRowStatus;
use crate::model::{Commit, CommitEntry, LocalRepository};
use crate::opts::DFOpts;
use crate::{error::OxenError, util};
use std::path::{Path, PathBuf};

pub mod rows;

pub fn is_behind(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    let commit_path = previous_commit_ref_path(repo, commit, workspace_id, path);
    let commit_id = util::fs::read_from_path(commit_path)?;
    Ok(commit_id != commit.id)
}

pub fn previous_commit_ref_path(
    repo: &LocalRepository,
    commit: &Commit,
    identifier: &str,
    path: impl AsRef<Path>,
) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspaces::workspace_dir(repo, commit, identifier)
        .join(OXEN_HIDDEN_DIR)
        .join(WORKSPACES_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("COMMIT_ID")
}

pub fn mods_db_path(
    repo: &LocalRepository,
    commit: &Commit,
    identifier: &str,
    path: impl AsRef<Path>,
) -> PathBuf {
    let path_hash = util::hasher::hash_str(path.as_ref().to_string_lossy());
    workspaces::workspace_dir(repo, commit, identifier)
        .join(OXEN_HIDDEN_DIR)
        .join(WORKSPACES_DIR)
        .join(MODS_DIR)
        .join("duckdb")
        .join(path_hash)
        .join("db")
}

pub fn count(
    repo: &LocalRepository,
    commit: &Commit,
    path: PathBuf,
    identifier: &str,
) -> Result<usize, OxenError> {
    let db_path = mods_db_path(repo, commit, identifier, path);
    let conn = df_db::get_connection(db_path)?;

    let count = df_db::count(&conn, TABLE_NAME)?;
    Ok(count)
}

pub fn index(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    path: &Path,
) -> Result<(), OxenError> {
    if !util::fs::is_tabular(path) {
        return Err(OxenError::basic_str(
            "File format not supported, must be tabular.must be tabular.",
        ));
    }

    // need to init or get the remote staging env - for if this was called from API? todo
    let _workspace = workspaces::init_or_get(repo, commit, workspace_id)?;

    let reader = CommitEntryReader::new(repo, commit)?;
    let entry = reader.get_entry(path)?;
    let entry = match entry {
        Some(entry) => entry,
        None => return Err(OxenError::resource_not_found(path.to_string_lossy())),
    };

    let db_path = mods_db_path(repo, commit, workspace_id, &entry.path);

    if !db_path
        .parent()
        .expect("Failed to get parent directory")
        .exists()
    {
        std::fs::create_dir_all(db_path.parent().expect("Failed to get parent directory"))?;
    }

    copy_duckdb_if_already_indexed(repo, &entry, &db_path)?;

    let conn = df_db::get_connection(db_path)?;
    if df_db::table_exists(&conn, TABLE_NAME)? {
        df_db::drop_table(&conn, TABLE_NAME)?;
    }
    let version_path = util::fs::version_path(repo, &entry);

    log::debug!(
        "index_dataset({:?}) got version path: {:?}",
        entry.path,
        version_path
    );

    df_db::index_file_with_id(&version_path, &conn)?;
    log::debug!("index_dataset({:?}) finished!", entry.path);

    add_row_status_cols(&conn)?;

    // Save the current commit id so we know if the branch has advanced
    let commit_path = previous_commit_ref_path(repo, commit, workspace_id, path);
    util::fs::write_to_path(commit_path, &commit.id)?;

    Ok(())
}

pub fn query(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let db_path = mods_db_path(repo, commit, workspace_id, path);
    log::debug!("query_staged_df() got db_path: {:?}", db_path);

    let conn = df_db::get_connection(db_path)?;

    // Get the schema of this commit entry
    let schema = api::local::schemas::get_by_path_from_ref(repo, &commit.id, path)?
        .ok_or_else(|| OxenError::resource_not_found(path.to_string_lossy()))?;

    // Enrich w/ oxen cols
    let full_schema = staged_df_db::enhance_schema_with_oxen_cols(&schema)?;

    let col_names = select_cols_from_schema(&schema)?;

    let select = Select::new().select(&col_names).from(TABLE_NAME);

    let df = df_db::select(&conn, &select, true, Some(&full_schema), Some(opts))?;

    Ok(df)
}

fn add_row_status_cols(conn: &Connection) -> Result<(), OxenError> {
    let query_status = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" VARCHAR DEFAULT '{}'",
        TABLE_NAME,
        DIFF_STATUS_COL,
        StagedRowStatus::Unchanged
    );
    conn.execute(&query_status, [])?;

    let query_hash = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" VARCHAR DEFAULT NULL",
        TABLE_NAME, DIFF_HASH_COL
    );
    conn.execute(&query_hash, [])?;
    Ok(())
}

fn copy_duckdb_if_already_indexed(
    repo: &LocalRepository,
    entry: &CommitEntry,
    new_db_path: &Path,
) -> Result<(), OxenError> {
    let maybe_existing_db_path = sql::db_cache_path(repo, entry);
    let conn = df_db::get_connection(&maybe_existing_db_path)?;
    if df_db::table_exists(&conn, TABLE_NAME)? {
        log::debug!(
            "copying existing db from {:?} to {:?}",
            maybe_existing_db_path,
            new_db_path
        );
        std::fs::copy(&maybe_existing_db_path, new_db_path)?;
        return Ok(());
    }
    Ok(())
}

pub fn unindex(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    let db_path = mods_db_path(repo, commit, workspace_id, path);
    let conn = df_db::get_connection(db_path)?;
    df_db::drop_table(&conn, TABLE_NAME)?;

    Ok(())
}

pub fn is_indexed(
    repo: &LocalRepository,
    commit: &Commit,
    identifier: &str,
    path: &Path,
) -> Result<bool, OxenError> {
    log::debug!("checking dataset is indexed for {:?}", path);
    let db_path = mods_db_path(repo, commit, identifier, path);
    log::debug!("getting conn at path {:?}", db_path);
    let conn = df_db::get_connection(db_path)?;

    let table_exists = df_db::table_exists(&conn, TABLE_NAME)?;
    log::debug!("dataset_is_indexed() got table_exists: {:?}", table_exists);
    Ok(table_exists)
}

pub fn diff(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<DiffResult, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    // Get commit for the branch head
    log::debug!("diff_workspace_df got repo at path {:?}", repo.path);

    let entry = api::local::entries::get_commit_entry(repo, commit, path)?
        .ok_or(OxenError::entry_does_not_exist(path))?;

    let _branch_repo = workspaces::init_or_get(repo, commit, workspace_id)?;

    if !workspaces::data_frames::is_indexed(repo, commit, workspace_id, path)? {
        return Err(OxenError::basic_str("Dataset is not indexed"));
    };

    let db_path = workspaces::data_frames::mods_db_path(repo, commit, workspace_id, entry.path);

    let conn = df_db::get_connection(db_path)?;

    let diff_df = staged_df_db::df_diff(&conn)?;

    if diff_df.is_empty() {
        return Ok(DiffResult::Tabular(TabularDiff::empty()));
    }

    let row_mods = AddRemoveModifyCounts::from_diff_df(&diff_df)?;

    let schema = staged_df_db::schema_without_oxen_cols(&conn, TABLE_NAME)?;

    let schemas = TabularDiffSchemas {
        left: schema.clone(),
        right: schema.clone(),
        diff: schema.clone(),
    };

    let diff_summary = TabularDiffSummary {
        modifications: TabularDiffMods {
            row_counts: row_mods,
            col_changes: TabularSchemaDiff::empty(),
        },
        schemas,
        dupes: TabularDiffDupes::empty(),
    };

    let diff_result = TabularDiff {
        contents: diff_df,
        parameters: TabularDiffParameters::empty(),
        summary: diff_summary,
    };

    Ok(DiffResult::Tabular(diff_result))
}

pub fn extract_dataset_to_versions_dir(
    repo: &LocalRepository,
    commit: &Commit,
    entry: &CommitEntry,
    workspace_id: &str,
) -> Result<(), OxenError> {
    let version_path = util::fs::version_path(repo, entry);
    let db_path = mods_db_path(repo, commit, workspace_id, entry.path.clone());
    let conn = df_db::get_connection(db_path)?;

    log::debug!("extracting to versions path: {:?}", version_path);

    // Filter out any with removed status before extracting
    let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
        "\"{}\" = '{}'",
        DIFF_STATUS_COL,
        StagedRowStatus::Removed
    ));
    conn.execute(&delete.to_string(), [])?;

    let df_before = tabular::read_df(&version_path, DFOpts::empty())?;
    log::debug!(
        "extract_dataset_to_versions_dir() got df_before: {:?}",
        df_before
    );

    match entry.path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") => export_csv(&version_path, &conn)?,
            Some("tsv") => export_tsv(&version_path, &conn)?,
            Some("json") | Some("jsonl") | Some("ndjson") => export_rest(&version_path, &conn)?,
            Some("parquet") => export_parquet(&version_path, &conn)?,
            _ => {
                return Err(OxenError::basic_str(
                    "File format not supported, must be tabular.",
                ))
            }
        },
        None => {
            return Err(OxenError::basic_str(
                "File format not supported, must be tabular.",
            ))
        }
    }

    let df_after = tabular::read_df(&version_path, DFOpts::empty())?;
    log::debug!(
        "extract_dataset_to_versions_dir() got df_after: {:?}",
        df_after
    );

    Ok(())
}

// TODONOW combine with versions dir export fn and genericize on path
pub fn extract_dataset_to_working_dir(
    repo: &LocalRepository,
    workspace: &LocalRepository,
    commit: &Commit,
    entry: &CommitEntry,
    workspace_id: &str,
) -> Result<PathBuf, OxenError> {
    let working_path = workspace.path.join(entry.path.clone());
    let db_path = mods_db_path(repo, commit, workspace_id, entry.path.clone());
    let conn = df_db::get_connection(db_path)?;
    // Match on the extension

    if !working_path.exists() {
        util::fs::create_dir_all(
            working_path
                .parent()
                .expect("Failed to get parent directory"),
        )?;
    }

    log::debug!("created working path: {:?}", working_path);

    let delete = Delete::new().delete_from(TABLE_NAME).where_clause(&format!(
        "\"{}\" = '{}'",
        DIFF_STATUS_COL,
        StagedRowStatus::Removed
    ));
    let res = conn.execute(&delete.to_string(), [])?;
    log::debug!("delete query result is: {:?}", res);

    match entry.path.extension() {
        Some(ext) => match ext.to_str() {
            Some("csv") => export_csv(&working_path, &conn)?,
            Some("tsv") => export_tsv(&working_path, &conn)?,
            Some("json") | Some("jsonl") | Some("ndjson") => export_rest(&working_path, &conn)?,
            Some("parquet") => export_parquet(&working_path, &conn)?,
            _ => {
                return Err(OxenError::basic_str(
                    "File format not supported, must be tabular.",
                ))
            }
        },
        None => {
            return Err(OxenError::basic_str(
                "File format not supported, must be tabular.",
            ))
        }
    }

    let df_after = tabular::read_df(&working_path, DFOpts::empty())?;
    log::debug!(
        "extract_dataset_to_working_dir() got df_after: {:?}",
        df_after
    );

    Ok(working_path)
}

pub fn unstage(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    unindex(repo, commit, workspace_id, &path)?;

    let opts = db::opts::default();
    let files_db_path = workspaces::stager::files_db_path(repo, commit, workspace_id);
    let files_db: DBWithThreadMode<MultiThreaded> =
        rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
    let key = path.as_ref().to_string_lossy();
    str_json_db::delete(&files_db, key)?;

    Ok(())
}

pub fn restore(
    repo: &LocalRepository,
    commit: &Commit,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    // Unstage and then restage the df
    unindex(repo, commit, workspace_id, &path)?;

    // TODO: we could do this more granularly without a full reset
    index(repo, commit, workspace_id, path.as_ref())?;

    Ok(())
}

fn export_rest(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_rest()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}';",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    // let temp_select_query = Select::new().select("*").from(TABLE_NAME);
    // let temp_res = df_db::select(conn, &temp_select_query)?;
    // log::debug!("export_rest() got df: {:?}", temp_res);

    conn.execute(&query, [])?;
    Ok(())
}

fn export_csv(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_csv()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (HEADER, DELIMITER ',');",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    // let temp_select_query = Select::new().select("*").from(TABLE_NAME);

    // let temp_res = df_db::select(conn, &temp_select_query)?;
    // log::debug!("export_csv() got df: {:?}", temp_res);

    conn.execute(&query, [])?;

    Ok(())
}

fn export_tsv(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_tsv()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (HEADER, DELIMITER '\t');",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );

    conn.execute(&query, [])?;
    Ok(())
}

fn export_parquet(path: &Path, conn: &Connection) -> Result<(), OxenError> {
    log::debug!("export_parquet()");
    let excluded_cols = OXEN_COLS
        .iter()
        .map(|col| format!("\"{}\"", col))
        .collect::<Vec<String>>()
        .join(", ");

    let query = format!(
        "COPY (SELECT * EXCLUDE ({}) FROM '{}') to '{}' (FORMAT PARQUET);",
        excluded_cols,
        TABLE_NAME,
        path.to_string_lossy()
    );
    conn.execute(&query, [])?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::command;
    use crate::config::UserConfig;
    use crate::constants::OXEN_ID_COL;
    use crate::core::index::workspaces;
    use crate::error::OxenError;
    use crate::model::diff::DiffResult;
    use crate::model::entry::mod_entry::ModType;
    use crate::model::entry::mod_entry::NewMod;
    use crate::model::ContentType;
    use crate::model::NewCommitBody;
    use crate::opts::DFOpts;
    use crate::test;

    #[test]
    fn test_stage_json_append_tabular() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let workspace_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "{\"file\":\"dawg1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data: data.to_string(),
                mod_type: ModType::Append,
                content_type: ContentType::Json,
            };
            workspaces::data_frames::index(&repo, &commit, &workspace_id, &file_path)?;
            workspaces::data_frames::rows::add(&repo, &commit, &workspace_id, &new_mod)?;

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 1);

            let diff = workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path)?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_stage_delete_appended_mod() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let workspace_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "{\"file\":\"dawg1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}".to_string();
            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data,
                mod_type: ModType::Append,
                content_type: ContentType::Json,
            };

            workspaces::data_frames::index(&repo, &commit, &workspace_id, &file_path)?;

            let append_entry_1 =
                workspaces::data_frames::rows::add(&repo, &commit, &workspace_id, &new_mod)?;

            let append_1_id = append_entry_1.column(OXEN_ID_COL)?.get(0)?.to_string();
            let append_1_id = append_1_id.replace('"', "");

            let data = "{\"file\":\"dawg2.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}".to_string();
            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data,
                mod_type: ModType::Append,
                content_type: ContentType::Json,
            };
            let _append_entry_2 = workspaces::data_frames::rows::add(&repo, &commit, &workspace_id, &new_mod)?;

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 1);


            // List the staged mods
            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Delete the first append
            workspaces::data_frames::rows::delete(&repo, &commit, &workspace_id, &commit_entry.path, &append_1_id)?;

            // Should only be one mod now
            let diff = workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_clear_staged_mods() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let workspace_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            // Append the data to staging area
            let data = "{\"file\":\"dawg1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}".to_string();

            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data,
                mod_type: ModType::Append,
                content_type: ContentType::Json,
            };

            log::debug!("indexing the dataset at filepath {:?}", file_path);
            workspaces::data_frames::index(&repo, &commit, &workspace_id, &file_path)?;
            log::debug!("indexed the dataset");

            let append_entry_1 =
                workspaces::data_frames::rows::add(&repo, &commit, &workspace_id, &new_mod)?;
            let append_1_id = append_entry_1.column(OXEN_ID_COL)?.get(0)?;
            let append_1_id = append_1_id.get_str().unwrap();
            log::debug!("added the row");

            let data = "{\"file\":\"dawg2.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}".to_string();
            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data,
                mod_type: ModType::Append,
                content_type: ContentType::Json,
            };
            let append_entry_2 =
                workspaces::data_frames::rows::add(&repo, &commit, &workspace_id, &new_mod)?;
            let append_2_id = append_entry_2.column(OXEN_ID_COL)?.get(0)?;
            let append_2_id = append_2_id.get_str().unwrap();

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 1);

            // List the staged mods
            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 2);
                }
                _ => panic!("Expected tabular diff result"),
            }
            // Delete the first append
            workspaces::data_frames::rows::delete(
                &repo,
                &commit,
                &workspace_id,
                &commit_entry.path,
                append_1_id,
            )?;

            // Delete the second append
            workspaces::data_frames::rows::delete(
                &repo,
                &commit,
                &workspace_id,
                &commit_entry.path,
                append_2_id,
            )?;

            // Should be zero staged files
            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 0);

            log::debug!("about to diff staged");
            // Should be zero mods left
            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            log::debug!("got diff staged");

            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }
            Ok(())
        })
    }

    #[test]
    fn test_delete_committed_row() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let workspace_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            let workspace = workspaces::init_or_get(&repo, &commit, &workspace_id)?;

            // Index the dataset
            workspaces::data_frames::index(&repo, &commit, &workspace_id, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(
                &repo,
                &commit,
                &workspace_id,
                &file_path,
                &page_opts,
            )?;

            let id_to_delete = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Stage a deletion
            workspaces::data_frames::rows::delete(
                &repo,
                &commit,
                &workspace_id,
                commit_entry.path,
                &id_to_delete,
            )?;

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 1);

            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            let status = command::status(&repo)?;
            log::debug!("got this status {:?}", status);

            // Commit the new file

            let new_commit = NewCommitBody {
                author: "author".to_string(),
                email: "email".to_string(),
                message: "Deleting a row allegedly".to_string(),
            };
            let commit_2 = workspaces::commit(
                &repo,
                &workspace,
                &commit,
                &workspace_id,
                &new_commit,
                branch_name,
            )?;

            let file_1 = api::local::revisions::get_version_file_from_commit_id(
                &repo, &commit.id, &file_path,
            )?;

            let file_2 = api::local::revisions::get_version_file_from_commit_id(
                &repo,
                commit_2.id,
                &file_path,
            )?;

            let diff_result = api::local::diff::diff_files(file_1, file_2, vec![], vec![], vec![])?;

            match diff_result {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_stage_modify_added_row() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let workspace_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            let _workspace = workspaces::init_or_get(&repo, &commit, &workspace_id)?;

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&repo, &commit, &workspace_id, &file_path)?;

            // Add a row
            let add_mod = NewMod {
                entry: commit_entry.clone(),
                data: "{\"min_x\":13,\"min_y\":14,\"width\":100,\"height\":100}".to_string(),
                mod_type: ModType::Append,
                content_type: ContentType::Json,
            };

            let new_row =
                workspaces::data_frames::rows::add(&repo, &commit, &workspace_id, &add_mod)?;

            // 1 row added
            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            let id_to_modify = new_row.column(OXEN_ID_COL)?.get(0)?;
            let id_to_modify = id_to_modify.get_str().unwrap();

            let modify_mod = NewMod {
                entry: commit_entry.clone(),
                data: "{\"height\": 101}".to_string(),
                mod_type: ModType::Modify,
                content_type: ContentType::Json,
            };

            workspaces::data_frames::rows::update(
                &repo,
                &commit,
                &workspace_id,
                id_to_modify,
                &modify_mod,
            )?;
            // List the files that are changed - this file should be back into unchanged state
            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            log::debug!("found mod entries: {:?}", commit_entries);
            assert_eq!(commit_entries.len(), 1);

            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(modified_rows, 0);
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_stage_json_delete_added_row() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let workspace_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            let _workspace = workspaces::init_or_get(&repo, &commit, &workspace_id)?;

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&repo, &commit, &workspace_id, &file_path)?;

            // Add a row
            let add_mod = NewMod {
                entry: commit_entry.clone(),
                data: "{\"min_x\":13,\"min_y\":14,\"width\":100,\"height\":100}".to_string(),
                mod_type: ModType::Append,
                content_type: ContentType::Json,
            };

            let new_row =
                workspaces::data_frames::rows::add(&repo, &commit, &workspace_id, &add_mod)?;

            // 1 row added
            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    assert_eq!(added_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            let id_to_delete = new_row.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Stage a deletion
            workspaces::data_frames::rows::delete(
                &repo,
                &commit,
                &workspace_id,
                &commit_entry.path,
                &id_to_delete,
            )?;
            log::debug!("done deleting row");
            // List the files that are changed - this file should be back into unchanged state
            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            log::debug!("found mod entries: {:?}", commit_entries);
            assert_eq!(commit_entries.len(), 0);

            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_stage_modify_row_back_to_original_state() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let workspace_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            let _workspace = workspaces::init_or_get(&repo, &commit, &workspace_id)?;

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&repo, &commit, &workspace_id, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(
                &repo,
                &commit,
                &workspace_id,
                &file_path,
                &page_opts,
            )?;

            let id_to_modify = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_modify = id_to_modify.replace('"', "");

            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data: "{\"label\": \"doggo\"}".to_string(),
                mod_type: ModType::Modify,
                content_type: ContentType::Json,
            };

            // Stage a modification
            workspaces::data_frames::rows::update(
                &repo,
                &commit,
                &workspace_id,
                &id_to_modify,
                &new_mod,
            )?;

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 1);

            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    assert_eq!(modified_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Now modify the row back to its original state
            let modify_mod = NewMod {
                entry: commit_entry.clone(),
                data: "{\"label\": \"dog\"}".to_string(),
                mod_type: ModType::Modify,
                content_type: ContentType::Json,
            };

            let res = workspaces::data_frames::rows::update(
                &repo,
                &commit,
                &workspace_id,
                &id_to_modify,
                &modify_mod,
            )?;

            log::debug!("res is... {:?}", res);

            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 0);

            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    assert_eq!(modified_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }
    #[test]
    fn test_restore_df_row() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let workspace_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            let _workspace = workspaces::init_or_get(&repo, &commit, &workspace_id)?;

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&repo, &commit, &workspace_id, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(
                &repo,
                &commit,
                &workspace_id,
                &file_path,
                &page_opts,
            )?;

            let id_to_modify = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_modify = id_to_modify.replace('"', "");

            let new_mod = NewMod {
                entry: commit_entry.clone(),
                data: "{\"label\": \"doggo\"}".to_string(),
                mod_type: ModType::Modify,
                content_type: ContentType::Json,
            };

            // Stage a modification
            workspaces::data_frames::rows::update(
                &repo,
                &commit,
                &workspace_id,
                &id_to_modify,
                &new_mod,
            )?;

            // List the files that are changed
            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 1);

            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    assert_eq!(modified_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Now restore the row
            let res = workspaces::data_frames::rows::restore(
                &repo,
                &commit,
                &workspace_id,
                &commit_entry,
                &id_to_modify,
            )?;

            log::debug!("res is... {:?}", res);

            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 0);

            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(modified_rows, 0);
                    assert_eq!(added_rows, 0);
                    assert_eq!(removed_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }

    #[test]
    fn test_restore_df_row_delete() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "test-append";
            let branch = api::local::branches::create_checkout(&repo, branch_name)?;
            let workspace_id = UserConfig::identifier()?;
            let file_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let commit = api::local::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let commit_entry =
                api::local::entries::get_commit_entry(&repo, &commit, &file_path)?.unwrap();

            let _workspace = workspaces::init_or_get(&repo, &commit, &workspace_id)?;

            // Could use cache path here but they're being sketchy at time of writing
            // Index the dataset
            workspaces::data_frames::index(&repo, &commit, &workspace_id, &file_path)?;

            // Preview the dataset to grab some ids
            let mut page_opts = DFOpts::empty();
            page_opts.page = Some(0);
            page_opts.page_size = Some(10);

            let staged_df = workspaces::data_frames::query(
                &repo,
                &commit,
                &workspace_id,
                &file_path,
                &page_opts,
            )?;

            let id_to_delete = staged_df.column(OXEN_ID_COL)?.get(0)?.to_string();
            let id_to_delete = id_to_delete.replace('"', "");

            // Stage a deletion
            workspaces::data_frames::rows::delete(
                &repo,
                &commit,
                &workspace_id,
                &commit_entry.path,
                &id_to_delete,
            )?;
            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 1);

            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(removed_rows, 1);
                }
                _ => panic!("Expected tabular diff result"),
            }

            // Now restore the row
            let res = workspaces::data_frames::rows::restore(
                &repo,
                &commit,
                &workspace_id,
                &commit_entry,
                &id_to_delete,
            )?;

            log::debug!("res is... {:?}", res);

            let commit_entries = workspaces::stager::list_files(&repo, &commit, &workspace_id)?;
            assert_eq!(commit_entries.len(), 0);

            let diff =
                workspaces::data_frames::diff(&repo, &commit, &workspace_id, file_path.clone())?;
            match diff {
                DiffResult::Tabular(tabular_diff) => {
                    let modified_rows = tabular_diff.summary.modifications.row_counts.modified;
                    let added_rows = tabular_diff.summary.modifications.row_counts.added;
                    let removed_rows = tabular_diff.summary.modifications.row_counts.removed;
                    assert_eq!(modified_rows, 0);
                    assert_eq!(added_rows, 0);
                    assert_eq!(removed_rows, 0);
                }
                _ => panic!("Expected tabular diff result"),
            }

            Ok(())
        })
    }
}
