use duckdb::Connection;
use sql_query_builder::Select;

use crate::core::db::df_db;
use crate::core::index::mod_stager;
use crate::model::entry::commit_entry::Entry;
use crate::{error::OxenError, util};
use crate::model::{LocalRepository, Branch};
use std::path::Path;

use super::{CommitEntryReader, CommitReader};

const TABLE_NAME: &str = "STAGED_DATA";

pub fn index_dataset(
    repo: &LocalRepository,
    branch_repo: &LocalRepository, 
    branch: &Branch, 
    path: &Path, 
    identifier: &str, 
) -> Result<(), OxenError> { // TODONOW: this should return a RemoteDataset struct
    
    if !util::fs::is_tabular(&path) {
        return Err(OxenError::basic_str("File format not supported, must be tabular.must be tabular."));
    }


    // Get the version path 
    let commit_reader = CommitReader::new(repo)?;
    
    let commit = commit_reader.get_commit_by_id(&branch.commit_id)?;

    let commit = match commit {
        Some(commit) => commit,
        None => return Err(OxenError::resource_not_found(&branch.commit_id)),
    };

    let reader = CommitEntryReader::new(repo, &commit)?;

    let entry = reader.get_entry(path)?;
    let entry = match entry {
        Some(entry) => entry,
        None => return Err(OxenError::resource_not_found(&path.to_string_lossy())),
    };

    // Get version path for entry 

    let db_path = mod_stager::mods_duckdb_path(repo, branch, identifier, &entry.path);
    // Clean house 
    

    // TODONOW: behavior here? 
    // drop it if it exists 

    // TODONOW this should actually be drop table

    // if db_path.exists() {
    //     util::fs::remove_dir_all(&db_path)?;
    // }

    // // Create the directory 
    // util::fs::create_dir_all(&db_path)?;



    let conn = df_db::get_connection(&db_path)?;
    
    df_db::drop_table(&conn, TABLE_NAME)?;
    let version_path = util::fs::version_path(&repo, &entry);

    log::debug!("index_dataset() got version path: {:?}", version_path);


    index_csv(&version_path, &conn)?;

    Ok(())
}



fn index_csv(
    path: &Path,
    conn: &Connection,
) -> Result<(), OxenError> {
    let query = format!("CREATE TABLE {} AS SELECT * FROM '{}';", TABLE_NAME, path.to_string_lossy());
    conn.execute(&query, [])?;

    // TODONOW delete 
    let select_all = Select::new().select("*").from(TABLE_NAME);
    let data = df_db::select(&conn, &select_all)?;
    log::debug!("index_csv() got data: {:?}", data);

    Ok(())
}


